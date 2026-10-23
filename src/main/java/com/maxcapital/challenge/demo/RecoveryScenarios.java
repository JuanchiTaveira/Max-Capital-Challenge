package com.maxcapital.challenge.demo;

import static com.maxcapital.challenge.demo.ScenarioSteps.*;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;

/** Ensayos sobre el mismo camino SQL/Kafka que consume las órdenes ordinarias. */
@Service
@Profile("evaluation-console")
public class RecoveryScenarios {
  private final ConsoleKafka kafka;
  private final ConsoleQueries queries;
  private final ConsoleSystem system;
  private final InstanceAccess instances;
  private final DockerAccess docker;
  private final Map<String, List<Handle>> active = new ConcurrentHashMap<>();

  record Handle(String service, String token, String generation) {}

  public RecoveryScenarios(
      ConsoleKafka kafka,
      ConsoleQueries queries,
      ConsoleSystem system,
      InstanceAccess instances,
      DockerAccess docker) {
    this.kafka = kafka;
    this.queries = queries;
    this.system = system;
    this.instances = instances;
    this.docker = docker;
  }

  void execute(DemoRun run, String scenario, ScenarioSteps steps, Set<String> restoration) {
    active.computeIfAbsent(run.id, ignored -> new ArrayList<>());
    restoration.addAll(List.of("app-1", "app-2"));
    if (scenario.equals("settlement-once")) settlement(run, steps);
    else {
      for (String check :
          List.of("sql-open", "sql-committed", "send-ack", "rebalance", "both-down")) {
        String boundary =
            switch (check) {
              case "sql-open", "rebalance" -> "BEFORE_SQL_COMMIT";
              case "send-ack" -> "AFTER_SEND_ACK_BEFORE_KAFKA_COMMIT";
              default -> "AFTER_SQL_COMMIT";
            };
        run.step(
            "Ensayar recuperación: " + label(check),
            "Frontera observada, recuperación sin efectos repetidos y continuación en secuencia",
            () -> recover(run, steps, check, boundary));
      }
    }
    run.step(
        "Comprobar ambas instancias después de las fallas",
        "Dos consumidores activos, una partición cada uno",
        () -> steps.ready(run, "restored"));
  }

  private void settlement(DemoRun run, ScenarioSteps steps) {
    long filled = steps.fresh(run, 0), cancelled = steps.fresh(run, 1);
    var records = new ArrayList<ConsoleKafka.Record>();
    var starts = cursors();
    run.step(
        "Completar una orden y cancelar otra",
        "Una salida FILLED; ninguna CANCELLED",
        () -> {
          records.add(steps.emit(run, filled, 1, "NEW", 0, 10));
          records.add(steps.emit(run, cancelled, 1, "NEW", 0, 10));
          records.add(steps.emit(run, filled, 2, "PARTIALLY_FILLED", 4, 6));
          records.add(steps.emit(run, cancelled, 2, "PARTIALLY_FILLED", 4, 6));
          records.add(steps.emit(run, filled, 3, "FILLED", 10, 0));
          records.add(steps.emit(run, cancelled, 3, "CANCELLED", 4, 0));
          steps.committed(run, records);
          return Map.of(
              "filled",
              steps.expectOrder(run, "filled", filled, "FILLED", 10, 1, 2, 3),
              "cancelled",
              steps.expectOrder(run, "cancelled", cancelled, "CANCELLED", 4, 1, 2, 3));
        });
    run.step(
        "Republicar terminales y creación",
        "Tres DUPLICATE, sin cambios de contador/ledger ni salida adicional",
        () -> {
          var duplicates =
              List.of(
                  steps.republish(run, records.get(4)),
                  steps.republish(run, records.get(5)),
                  steps.republish(run, records.getFirst()));
          steps.committed(run, duplicates);
          steps.assertResults(run, duplicates, "DUPLICATE", "duplicates");
          steps.expectOrder(run, "filled", filled, "FILLED", 10, 1, 2, 3);
          steps.expectOrder(run, "cancelled", cancelled, "CANCELLED", 4, 1, 2, 3);
          var output =
              await(
                  run,
                  "settlement único",
                  () ->
                      verifyOutput(
                          run, starts, filled, cancelled, steps.result(run, records.get(4))));
          run.observe(
              "settlement",
              "Un settlement confirmado por la orden completada y cero por la cancelada; las republicaciones no agregaron salidas.",
              true,
              output);
          return output;
        });
    run.step(
        "Recuperar una finalización tras caída después de SQL",
        "Mismo APPLIED FILLED y un settlement confirmado desde la otra instancia",
        () -> recover(run, steps, "recovery", "AFTER_SQL_COMMIT"));
  }

  private Map<String, Object> recover(
      DemoRun run, ScenarioSteps steps, String check, String boundary) {
    steps.ready(run, "ready");
    int partition = 0;
    String service = owner(partition), survivor = service.equals("app-1") ? "app-2" : "app-1";
    long order = steps.fresh(run, partition);
    var starts = cursors();
    var first = steps.emit(run, order, 1, "NEW", 0, 10);
    steps.committed(run, List.of(first));
    boolean open = boundary.equals("BEFORE_SQL_COMMIT");
    var armed =
        instances.recovery(
            service,
            "arm",
            Map.of(
                "runId",
                run.id,
                "numericOrderId",
                Long.toString(order),
                "fixId",
                "2",
                "boundary",
                boundary,
                "ttlMillis",
                45000));
    var handle =
        new Handle(service, armed.path("token").asString(), armed.path("generation").asString());
    active.get(run.id).add(handle);
    var target =
        steps.emit(
            run, order, 2, open ? "PARTIALLY_FILLED" : "FILLED", open ? 4 : 10, open ? 6 : 0);
    var reached =
        await(
            run,
            "barrera " + boundary,
            () -> {
              var value = instances.recovery(service, "", null);
              require(
                  value.path("token").asString().equals(handle.token())
                      && value.path("generation").asString().equals(handle.generation()),
                  "Cambió el intento de barrera");
              require(
                  value.path("state").asString().equals("REACHED"),
                  "Barrera todavía no alcanzada: " + value.path("state").asString());
              require(
                  value.path("position").path("offset").asString().equals(target.offset())
                      && value
                          .path("position")
                          .path("partition")
                          .asString()
                          .equals(target.partition()),
                  "Barrera en otra posición");
              return value;
            });
    var beforeObservation = queries.result(target.topic(), partition, target.offset());
    var before = beforeObservation.get("result");
    run.sqlObservation(
        target,
        beforeObservation.get("result") == null ? "NO_RESULT" : "RESULT_FOUND",
        beforeObservation);
    var evidence = new LinkedHashMap<String, Object>();
    evidence.put("barrier", reached);
    evidence.put("target", target);
    evidence.put("sqlBefore", before == null ? "Sin escrituras confirmadas de este ER" : before);
    run.observe(
        check,
        "Frontera " + boundary + " observada; la recuperación todavía no está verificada.",
        false,
        evidence);
    run.checkCancellation();
    require(
        open ? before == null : before != null, "La visibilidad SQL no coincide con la frontera");
    require(
        outputs(starts).stream().noneMatch(record -> record.key().equals(Long.toString(order))),
        "Salida visible antes de confirmar Kafka");
    require(
        committedOffset(partition) <= Long.parseLong(target.offset()),
        "Progreso adelantado antes de resolver barrera");
    var last = open ? steps.emit(run, order, 3, "FILLED", 10, 0) : target;
    long successorOrder = steps.fresh(run, partition);
    var successor = steps.emit(run, successorOrder, 1, "NEW", 0, 10);
    run.checkCancellation();
    if (check.equals("rebalance")) {
      evidence.put(
          "takeover",
          await(
              run,
              "reasignación real antes de liberar SQL",
              () -> {
                require(
                    assigned(survivor, partition),
                    "La otra instancia todavía no tiene la partición");
                var group = system.group();
                require(
                    ((List<?>) group.get("members")).size() == 1,
                    "El propietario anterior sigue perteneciendo al grupo");
                return group;
              }));
      instances.recovery(
          service, "control", Map.of("runId", run.id, "token", handle.token(), "rollback", false));
      var failure =
          await(
              run,
              "rechazo del intento Kafka obsoleto",
              () -> {
                var value = instances.recovery(service, "", null);
                require(
                    !value.path("failure").asString("").isBlank(),
                    "Sin rechazo observado del intento anterior");
                var consumer =
                    (JsonNode) instances.read(service, "/internal/demo/consumer").get("data");
                require(
                    !consumer.path("running").asBoolean(),
                    "El intento anterior todavía no terminó");
                return value;
              });
      evidence.put("obsoleteAttempt", failure);
    } else {
      evidence.put("kill", docker.control(service, "kill"));
      run.observe(
          check,
          "Frontera observada y proceso terminado; esperando recuperación desde progreso estable.",
          false,
          evidence);
      if (check.equals("both-down")) {
        evidence.put("killOther", docker.control(survivor, "kill"));
        docker.control(survivor, "start");
        docker.control(service, "start");
      } else {
        evidence.put(
            "takeover",
            await(
                run,
                "toma de partición por sobreviviente",
                () -> {
                  require(assigned(survivor, partition), "Partición todavía no reasignada");
                  return system.group();
                }));
      }
    }
    steps.committed(run, List.of(last, successor));
    var recovered = steps.result(run, target);
    require("APPLIED".equals(recovered.get("outcome")), "La misma posición perdió APPLIED");
    if (before != null)
      require(before.equals(recovered), "Cambió el resultado original después de recuperar");
    var completion = steps.result(run, last);
    evidence.put("result", recovered);
    evidence.put("successor", successor);
    run.observe(
        check,
        "Resultado recuperado y sucesor confirmado; esperando lectura completa de liquidaciones.",
        false,
        evidence);
    var output =
        await(
            run,
            "una salida final tras resolver intentos",
            () -> verifyOutput(run, starts, order, -1, completion));
    if (!check.equals("both-down"))
      docker.control(service, check.equals("rebalance") ? "restart" : "start");
    var ready = steps.ready(run, "ready");
    var successorState = queries.order(Long.toString(successorOrder));
    run.orderSnapshot(
        Long.toString(successorOrder), run.orderLabel(successorOrder), "successor", successorState);
    require("1".equals(successorState.get("appliedExecutionCount")), "Sucesor sin aplicar");
    if (check.equals("both-down")) {
      for (int p = 0; p < 2; p++) {
        long cancelled = steps.fresh(run, p);
        var newRecord = steps.emit(run, cancelled, 1, "NEW", 0, 10);
        var cancelledRecord = steps.emit(run, cancelled, 2, "CANCELLED", 0, 0);
        steps.committed(run, List.of(newRecord, cancelledRecord));
        var rows = outputs(starts);
        require(
            rows.stream().noneMatch(r -> r.key().equals(Long.toString(cancelled))),
            "CANCELLED produjo settlement");
        var cancelledState = queries.order(Long.toString(cancelled));
        run.orderSnapshot(
            Long.toString(cancelled), run.orderLabel(cancelled), "cancelled", cancelledState);
        require(
            "2".equals(cancelledState.get("appliedExecutionCount")),
            "No continuó en ambas particiones");
      }
    }
    var actual =
        steps.expectOrder(
            run, check, order, "FILLED", 10, open ? new int[] {1, 2, 3} : new int[] {1, 2});
    evidence.put("output", output);
    evidence.put("result", recovered);
    evidence.put("order", snapshot(actual));
    evidence.put("recoveredGroup", ready);
    evidence.put("successor", successor);
    run.observe(
        check,
        label(check)
            + ": frontera observada, secuencia recuperada sin duplicar efectos y un settlement confirmado; dos consumidores activos.",
        true,
        evidence);
    return evidence;
  }

  private String owner(int partition) {
    for (String service : List.of("app-1", "app-2"))
      if (assigned(service, partition)) return service;
    throw new IllegalStateException("No se observó propietario de la partición");
  }

  private boolean assigned(String service, int partition) {
    var value = (JsonNode) instances.read(service, "/internal/demo/consumer").get("data");
    for (var assignment : value.path("assignments"))
      if (assignment.path("partition").asString().equals(Integer.toString(partition))) return true;
    return false;
  }

  @SuppressWarnings("unchecked")
  private long committedOffset(int partition) {
    var positions = (List<Map<String, Object>>) system.group().get("partitions");
    var position =
        positions.stream()
            .filter(p -> p.get("partition").equals(Integer.toString(partition)))
            .findFirst()
            .orElseThrow();
    return position.get("committedOffset") == null
        ? 0
        : Long.parseLong(position.get("committedOffset").toString());
  }

  private Map<Integer, String> cursors() {
    var result = new LinkedHashMap<Integer, String>();
    for (int partition = 0; partition < 2; partition++)
      result.put(
          partition, kafka.read("settlements", partition, "0", 1).get("endOffset").toString());
    return result;
  }

  @SuppressWarnings("unchecked")
  List<ConsoleKafka.Record> outputs(Map<Integer, String> starts) {
    var records = new ArrayList<ConsoleKafka.Record>();
    for (int partition = 0; partition < 2; partition++) {
      String cursor = starts.get(partition);
      long limit =
          Long.parseLong(
              kafka.read("settlements", partition, cursor, 1).get("endOffset").toString());
      while (Long.parseLong(cursor) < limit) {
        var page = kafka.read("settlements", partition, cursor, 200);
        records.addAll((List<ConsoleKafka.Record>) page.get("records"));
        String next = page.get("nextCursor").toString();
        require(
            Long.parseLong(next) > Long.parseLong(cursor),
            "Lectura parcial de settlements; sin avance");
        cursor = next;
      }
    }
    return records;
  }

  private Map<String, Object> verifyOutput(
      DemoRun run,
      Map<Integer, String> starts,
      long filled,
      long cancelled,
      Map<String, Object> result) {
    var rows = outputs(starts);
    var matches = rows.stream().filter(r -> r.key().equals(Long.toString(filled))).toList();
    require(matches.size() == 1, "Esperado un settlement; observados " + matches.size());
    require(
        rows.stream().noneMatch(r -> r.key().equals(Long.toString(cancelled))),
        "Settlement de CANCELLED");
    var value = ConsoleValues.JSON.readTree(matches.getFirst().payload());
    var sourcePosition = value.path("source");
    var endOffsets = new LinkedHashMap<Integer, String>();
    for (int partition = 0; partition < 2; partition++)
      endOffsets.put(
          partition,
          kafka
              .read("settlements", partition, starts.get(partition), 1)
              .get("endOffset")
              .toString());
    for (var match : matches)
      run.output(
          match,
          "SETTLEMENT",
          sourcePosition.path("topic").asString()
              + "/"
              + sourcePosition.path("partition").asString()
              + "/"
              + sourcePosition.path("offset").asString(),
          starts,
          endOffsets);
    require(
        value.path("schemaVersion").asInt() == 1
            && value.path("settlementId").asString().equals("settlement:" + filled),
        "Identidad/versión de settlement incorrecta");
    for (String field : List.of("numericOrderId", "fixId", "ledgerId"))
      require(
          value.path(field).asString().equals(result.get(field)),
          "Referencia incorrecta: " + field);
    var source = value.path("source");
    require(
        source.path("topic").asString().equals(result.get("topic"))
            && source.path("partition").asString().equals(result.get("partitionId"))
            && source.path("offset").asString().equals(result.get("recordOffset")),
        "Posición fuente incorrecta");
    for (String field :
        List.of("nominalAmounts", "accumulativeNominalAmount", "leavesNominalAmount"))
      require(
          new BigDecimal(value.path(field).asString())
                  .compareTo(new BigDecimal(result.get(field).toString()))
              == 0,
          "Cantidad incorrecta: " + field);
    return Map.of(
        "messages",
        matches,
        "result",
        result,
        "startCursors",
        starts,
        "visibility",
        "read_committed; ambas particiones recorridas después de resolver intentos");
  }

  void restore(DemoRun run) {
    var handles = active.get(run.id);
    if (handles == null) return;
    var diagnostics = new ArrayList<String>();
    var restartRequired = new java.util.HashSet<String>();
    for (var handle : handles) {
      try {
        var observed = instances.recovery(handle.service(), "", null);
        if (observed.path("generation").asString().equals(handle.generation())
            && observed.path("token").asString().equals(handle.token()))
          instances.recovery(
              handle.service(),
              "control",
              Map.of("runId", run.id, "token", handle.token(), "rollback", true));
      } catch (RuntimeException ex) {
        diagnostics.add(DemoRun.message(ex));
        restartRequired.add(handle.service());
      }
    }
    try {
      for (String service : List.of("app-1", "app-2")) {
        docker.control(service, restartRequired.contains(service) ? "restart" : "start");
        long until = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        boolean restarted = false;
        while (true) {
          try {
            var view = (JsonNode) instances.read(service, "/internal/demo/consumer").get("data");
            if (view.path("running").asBoolean()) break;
            if (!restarted) {
              docker.control(service, "restart");
              restarted = true;
            }
          } catch (RuntimeException ex) {
            diagnostics.add(DemoRun.message(ex));
          }
          if (System.nanoTime() >= until)
            throw new IllegalStateException(
                "Reiniciar " + service + ": no se restauró el consumidor");
          sleep();
        }
      }
      long until = System.nanoTime() + Duration.ofSeconds(45).toNanos();
      while (true) {
        try {
          var members = (List<?>) system.group().get("members");
          require(
              members.size() == 2
                  && members.stream()
                      .allMatch(
                          member -> ((List<?>) ((Map<?, ?>) member).get("assignments")).size() == 1)
                  && !owner(0).equals(owner(1)),
              "Grupo todavía sin restaurar");
          break;
        } catch (RuntimeException ex) {
          if (System.nanoTime() >= until) throw ex;
          sleep();
        }
      }
      run.restoration(
          Map.of("status", "RESTORED", "group", system.group(), "diagnostics", diagnostics));
    } catch (RuntimeException ex) {
      run.restoration(
          Map.of(
              "status",
              "FAILED",
              "diagnostics",
              diagnostics,
              "action",
              "Revisar Docker y reiniciar app-1/app-2: " + DemoRun.message(ex)));
      throw ex;
    } finally {
      active.remove(run.id);
    }
  }

  private static void sleep() {
    try {
      Thread.sleep(200);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Restauración interrumpida", ex);
    }
  }

  private static String label(String check) {
    return switch (check) {
      case "sql-open" -> "Caída con escrituras SQL sin confirmar";
      case "sql-committed", "recovery" -> "Caída después del commit SQL";
      case "send-ack" -> "Caída después del envío aceptado por Kafka";
      case "rebalance" -> "Reasignación con transacción SQL abierta";
      case "both-down" -> "Caída de ambas instancias y continuación";
      default -> check;
    };
  }
}
