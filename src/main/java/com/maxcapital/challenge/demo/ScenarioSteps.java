package com.maxcapital.challenge.demo;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import org.apache.kafka.common.utils.Utils;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;

@Service
@Profile("evaluation-console")
public class ScenarioSteps {
  private final ConsoleKafka kafka;
  private final ConsoleQueries queries;
  private final ConsoleSystem system;
  private final InstanceAccess instances;
  private final DockerAccess docker;

  public ScenarioSteps(
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

  void execute(DemoRun run, String scenario, Set<String> restoration) {
    run.step(
        "Comprobar que el entorno está preparado",
        "Dos consumidores con una partición cada uno; SQL y Kafka disponibles",
        () -> ready(run));
    switch (scenario) {
      case "interleaved-orders" -> interleaved(run, false, restoration);
      case "quantity-snapshots" -> quantities(run);
      case "terminal-duplicates" -> duplicates(run);
      case "restart-persistence" -> interleaved(run, true, restoration);
      case "committed-input-only" -> transactions(run);
      default -> throw new IllegalArgumentException("Escenario no ejecutable");
    }
  }

  void restore(String service) {
    docker.control(service, "start");
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> ready(DemoRun run) {
    return ready(run, "ready");
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> ready(DemoRun run, String checkId) {
    return await(
        run,
        "consumidores y dependencias listos",
        () -> {
          try {
            queries.list(null, 1);
            var group = system.group();
            var members = (List<Map<String, Object>>) group.get("members");
            require(
                members.size() == 2,
                "Esperados dos miembros del grupo; observados " + members.size());
            for (var member : members)
              require(
                  ((List<?>) member.get("assignments")).size() == 1,
                  "Esperada una partición por miembro");
            var views = new ArrayList<Map<String, Object>>();
            for (String service : List.of("app-1", "app-2")) {
              var view = instances.read(service, "/internal/demo/consumer");
              require((int) view.get("statusCode") == 200, "Observación no disponible: " + service);
              var data = (JsonNode) view.get("data");
              require(
                  data.path("running").asBoolean() && data.path("assignments").size() == 1,
                  "Consumidor no preparado: " + service);
              require(
                  members.stream()
                      .anyMatch(m -> m.get("clientId").equals(data.path("clientId").asString())),
                  "ClientId de la instancia no observado en el grupo: " + service);
              views.add(view);
            }
            var evidence = Map.of("group", group, "instances", views);
            run.observe(
                checkId,
                "PostgreSQL y Kafka disponibles; "
                    + members.size()
                    + " consumidores activos, cada uno con una partición.",
                true,
                evidence);
            return evidence;
          } catch (RuntimeException ex) {
            run.diagnostic(checkId, DemoRun.message(ex));
            throw ex;
          }
        });
  }

  private static final class Flow {
    long first, second;
    final List<ConsoleKafka.Record> records = new ArrayList<>();
    ConsoleKafka.Record firstTerminal, secondTerminal;
    Map<String, Object> firstBefore, secondBefore;
  }

  private void interleaved(DemoRun run, boolean restart, Set<String> restoration) {
    var flow = new Flow();
    run.step(
        "Publicar órdenes intercaladas",
        "FILLED con cuatro ER y CANCELLED con tres en particiones diferentes",
        () -> {
          flow.first = fresh(run, 0);
          flow.second = fresh(run, 1);
          flow.records.add(emit(run, flow.first, 1, "NEW", 0, 10));
          flow.records.add(emit(run, flow.second, 1, "NEW", 0, 10));
          flow.records.add(emit(run, flow.first, 2, "PARTIALLY_FILLED", 3, 7));
          flow.records.add(emit(run, flow.second, 2, "PARTIALLY_FILLED", 7, 3));
          flow.records.add(emit(run, flow.first, 3, "PARTIALLY_FILLED", 7, 3));
          flow.secondTerminal = emit(run, flow.second, 3, "CANCELLED", 7, 0);
          flow.records.add(flow.secondTerminal);
          flow.firstTerminal = emit(run, flow.first, 4, "FILLED", 10, 0);
          flow.records.add(flow.firstTerminal);
          boolean different =
              !flow.firstTerminal.partition().equals(flow.secondTerminal.partition());
          run.observe(
              "partitions",
              "Orden A en partición "
                  + flow.firstTerminal.partition()
                  + "; orden B en partición "
                  + flow.secondTerminal.partition()
                  + ".",
              different,
              Map.of("messages", flow.records));
          require(different, "Se esperaban particiones diferentes");
          committed(run, flow.records);
          return Map.of("positions", flow.records);
        });
    run.step(
        "Verificar secuencia y efectos SQL",
        "Contador y ledger 4/3; siete APPLIED y mismas consultas en ambas instancias",
        () -> {
          assertResults(run, flow.records, "APPLIED", "applied");
          flow.firstBefore =
              snapshot(expectOrder(run, "order-a", flow.first, "FILLED", 10, 1, 2, 3, 4));
          flow.secondBefore =
              snapshot(expectOrder(run, "order-b", flow.second, "CANCELLED", 7, 1, 2, 3));
          return Map.of("filled", flow.firstBefore, "cancelled", flow.secondBefore);
        });
    if (!restart) return;
    run.step(
        "Reiniciar después del lote",
        "Ambas instancias vuelven a consumir; el lote ya estaba confirmado",
        () -> {
          restoration.add("app-1");
          var first = docker.control("app-1", "restart");
          restoration.add("app-2");
          var second = docker.control("app-2", "restart");
          return Map.of("controls", List.of(first, second), "recovered", ready(run, "recovered"));
        });
    run.step(
        "Republicar terminales y comparar",
        "Dos DUPLICATE nuevos; estado y ledger completos iguales",
        () -> {
          var first = republish(run, flow.firstTerminal);
          var second = republish(run, flow.secondTerminal);
          committed(run, List.of(first, second));
          assertResults(run, List.of(first, second), "DUPLICATE", "duplicates");
          var currentFirst =
              snapshot(expectOrder(run, "order-a", flow.first, "FILLED", 10, 1, 2, 3, 4));
          var currentSecond =
              snapshot(expectOrder(run, "order-b", flow.second, "CANCELLED", 7, 1, 2, 3));
          boolean same =
              flow.firstBefore.equals(currentFirst) && flow.secondBefore.equals(currentSecond);
          run.observe(
              "preserved",
              same
                  ? "Estado, cantidades, contador y ledger de ambas órdenes permanecieron iguales después del reinicio."
                  : "Se encontraron diferencias en los datos de las órdenes después del reinicio.",
              same,
              Map.of(
                  "before",
                  List.of(flow.firstBefore, flow.secondBefore),
                  "after",
                  List.of(currentFirst, currentSecond)));
          require(same, "El estado o ledger cambió tras reiniciar");
          return Map.of(
              "filled",
              currentFirst,
              "cancelled",
              currentSecond,
              "scope",
              "Reinicio después del lote; no prueba frontera de commit F-05");
        });
  }

  private void quantities(DemoRun run) {
    long order = fresh(run, 0);
    var times = new ArrayList<String>();
    var amounts = new ArrayList<String>();
    for (int index = 0; index < 4; index++) {
      int fix = index + 1, amount = new int[] {0, 3, 7, 10}[index];
      String status = index == 0 ? "NEW" : index == 3 ? "FILLED" : "PARTIALLY_FILLED";
      run.step(
          "Aplicar mensaje " + fix + ": acumulado " + amount,
          "Acumulado " + amount + ", restante " + (10 - amount) + ", contador " + fix,
          () -> {
            var record = emit(run, order, fix, status, amount, 10 - amount);
            committed(run, List.of(record));
            assertResults(run, List.of(record), "APPLIED");
            int[] fixes = java.util.stream.IntStream.rangeClosed(1, fix).toArray();
            var actual = expectOrder(run, "snapshot-" + fix, order, status, amount, fixes);
            times.add(
                ConsoleValues.JSON.readTree(record.payload()).path("transactionTime").asString());
            amounts.add(
                new BigDecimal(actual.get("accumulativeNominalAmount").toString())
                    .stripTrailingZeros()
                    .toPlainString());
            if (fix == 4) {
              boolean descending =
                  java.util.stream.IntStream.range(1, times.size())
                      .allMatch(i -> times.get(i).compareTo(times.get(i - 1)) <= 0);
              run.observe(
                  "clock",
                  "Acumulados observados "
                      + String.join(" → ", amounts)
                      + (descending
                          ? ". Se aplicaron en ese orden aunque las horas de origen retroceden."
                          : ". Las horas de origen no siguieron la secuencia decreciente prevista."),
                  descending,
                  Map.of(
                      "orderId",
                      Long.toString(order),
                      "transactionTimes",
                      List.copyOf(times),
                      "amounts",
                      List.copyOf(amounts)));
              require(descending, "El reloj del origen no fue decreciente");
            }
            return Map.of(
                "order",
                actual,
                "transactionTime",
                ConsoleValues.JSON.readTree(record.payload()).path("transactionTime").asString());
          });
    }
  }

  private void duplicates(DemoRun run) {
    long order = fresh(run, 1);
    var records = new ArrayList<ConsoleKafka.Record>();
    run.step(
        "Crear la orden",
        "Un mensaje aplicado crea la orden, con contador 1 y una entrada de historial",
        () -> {
          var record = emit(run, order, 1, "NEW", 0, 10);
          records.add(record);
          committed(run, List.of(record));
          return Map.of("order", expectOrder(run, "new", order, "NEW", 0, 1));
        });
    run.step(
        "Completar la orden y repetir la finalización",
        "FILLED aplicado una vez y su republicación DUPLICATE",
        () -> {
          var terminal = emit(run, order, 2, "FILLED", 10, 0);
          records.add(terminal);
          committed(run, List.of(terminal));
          var duplicate = republish(run, terminal);
          records.add(duplicate);
          committed(run, List.of(duplicate));
          assertResults(run, List.of(duplicate), "DUPLICATE");
          return Map.of("order", expectOrder(run, "terminal", order, "FILLED", 10, 1, 2));
        });
    run.step(
        "Repetir la creación después de completar la orden",
        "DUPLICATE con snapshot FILLED y referencia al ledger de NEW",
        () -> {
          var late = republish(run, records.getFirst());
          records.add(late);
          committed(run, List.of(late));
          assertResults(run, List.of(late), "DUPLICATE");
          var original = result(run, records.getFirst());
          var duplicate = result(run, late);
          boolean historical =
              "NEW".equals(original.get("status"))
                  && "1".equals(original.get("appliedExecutionCount"))
                  && "FILLED".equals(duplicate.get("status"))
                  && original.get("ledgerId").equals(duplicate.get("ledgerId"));
          run.observe(
              "history",
              "La creación original conserva "
                  + original.get("status")
                  + "; la repetición tardía conserva "
                  + duplicate.get("status")
                  + ". Referencias de ledger: "
                  + original.get("ledgerId")
                  + " y "
                  + duplicate.get("ledgerId")
                  + ".",
              historical,
              Map.of("original", original, "lateDuplicate", duplicate));
          require(historical, "Snapshot o referencia histórica incorrectos");
          var outcomes = records.stream().map(record -> result(run, record)).toList();
          long applied =
              outcomes.stream().filter(row -> "APPLIED".equals(row.get("outcome"))).count();
          long duplicated =
              outcomes.stream().filter(row -> "DUPLICATE".equals(row.get("outcome"))).count();
          run.observe(
              "duplicates",
              records.size()
                  + " envíos confirmados: "
                  + applied
                  + " aplicados y "
                  + duplicated
                  + " duplicados sin nuevo efecto.",
              applied == 2 && duplicated == 2,
              Map.of("results", outcomes, "messages", List.copyOf(records)));
          require(applied == 2 && duplicated == 2, "Distribución de resultados incorrecta");
          return Map.of(
              "original",
              original,
              "lateDuplicate",
              duplicate,
              "order",
              expectOrder(run, "final", order, "FILLED", 10, 1, 2));
        });
  }

  private void transactions(DemoRun run) {
    long order = fresh(run, 0);
    var records = new ArrayList<ConsoleKafka.Record>();
    run.step(
        "Confirmar la creación de la orden",
        "NEW visible y aplicado",
        () -> {
          var record =
              kafka.transaction(Long.toString(order), report(run, order, 1, "NEW", 0, 10), false);
          run.message(record, "COMMITTED");
          records.add(record);
          committed(run, List.of(record));
          return Map.of(
              "transaction", "COMMITTED", "order", expectOrder(run, "new", order, "NEW", 0, 1));
        });
    run.step(
        "Abortar la actualización parcial",
        "El envío tiene posición pero no es entrada read_committed",
        () -> {
          var record =
              kafka.transaction(
                  Long.toString(order), report(run, order, 2, "PARTIALLY_FILLED", 7, 3), true);
          run.message(record, "ABORTED");
          records.add(record);
          run.observe(
              "aborted",
              "El productor abortó la transacción del parcial. Su posición no demuestra aplicación; se verifica en el paso siguiente.",
              true,
              Map.of("position", record));
          return Map.of("transaction", "ABORTED", "position", record);
        });
    run.step(
        "Confirmar la finalización y comprobar qué mensajes se ven",
        "Sólo NEW y FILLED aplicados; sin efectos del parcial abortado",
        () -> {
          var last =
              kafka.transaction(
                  Long.toString(order), report(run, order, 3, "FILLED", 10, 0), false);
          run.message(last, "COMMITTED");
          records.add(last);
          committed(run, List.of(last));
          var visible = new ArrayList<String>();
          String cursor = records.getFirst().offset();
          while (Long.parseLong(cursor) <= Long.parseLong(last.offset())) {
            run.checkCancellation();
            require(!run.expired(), "No se observó el sucesor confirmado dentro del plazo");
            var page =
                kafka.read("execution-reports", Integer.parseInt(last.partition()), cursor, 200);
            @SuppressWarnings("unchecked")
            var batch = (List<ConsoleKafka.Record>) page.get("records");
            batch.forEach(record -> visible.add(record.offset()));
            cursor = page.get("nextCursor").toString();
          }
          boolean visibility =
              visible.contains(records.getFirst().offset())
                  && visible.contains(last.offset())
                  && !visible.contains(records.get(1).offset());
          run.observe(
              "visibility",
              "Creación "
                  + (visible.contains(records.getFirst().offset()) ? "visible" : "no visible")
                  + "; finalización "
                  + (visible.contains(last.offset()) ? "visible" : "no visible")
                  + "; parcial abortado "
                  + (visible.contains(records.get(1).offset())
                      ? "visible"
                      : "excluido de la lectura confirmada")
                  + ".",
              visibility,
              Map.of("visibleOffsets", List.copyOf(visible), "messages", List.copyOf(records)));
          require(visibility, "Visibilidad transaccional incorrecta");
          var abortedResult =
              queries
                  .result(
                      "execution-reports",
                      Integer.parseInt(last.partition()),
                      records.get(1).offset())
                  .get("result");
          run.observe(
              "no-result",
              abortedResult == null
                  ? "No se registró resultado SQL para el parcial abortado después de observar el sucesor confirmado."
                  : "Se registró un resultado SQL para el parcial abortado.",
              abortedResult == null,
              Map.of("position", records.get(1), "successor", last));
          require(abortedResult == null, "El parcial abortado produjo resultado SQL");
          assertResults(run, List.of(records.getFirst(), last), "APPLIED", "applied");
          return Map.of(
              "visibleOffsets",
              visible,
              "order",
              expectOrder(run, "final", order, "FILLED", 10, 1, 3),
              "scope",
              "Visibilidad de entrada; no prueba settlement");
        });
  }

  private long fresh(DemoRun run, int partition) {
    long order;
    do {
      run.checkCancellation();
      order = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
    } while ((Utils.murmur2(Long.toString(order).getBytes(StandardCharsets.UTF_8)) & 0x7fffffff) % 2
            != partition
        || queries.exists(order));
    run.order(Long.toString(order));
    return order;
  }

  private ConsoleKafka.Record emit(
      DemoRun run, long order, int fix, String status, int accumulated, int leaves) {
    run.checkCancellation();
    var record =
        kafka.publish(Long.toString(order), report(run, order, fix, status, accumulated, leaves));
    run.message(record);
    return record;
  }

  private ConsoleKafka.Record republish(DemoRun run, ConsoleKafka.Record original) {
    run.checkCancellation();
    var record = kafka.publish(original.key(), original.payload());
    run.message(record);
    require(
        !original.offset().equals(record.offset())
            && original.partition().equals(record.partition()),
        "Republicación sin una nueva posición en la misma partición");
    return record;
  }

  private String report(
      DemoRun run, long order, int fix, String status, int accumulated, int leaves) {
    return """
        {"numericOrderId":%d,"fixId":%d,"status":"%s","nominalAmounts":10,
        "accumulativeNominalAmount":%d,"leavesNominalAmount":%d,
        "transactionTime":"2026-07-20T18:08:%02d.129","ticker":"VSCPC","scenarioRun":"%s"}
        """
        .formatted(order, fix, status, accumulated, leaves, 56 - fix, run.id);
  }

  @SuppressWarnings("unchecked")
  private void committed(DemoRun run, List<ConsoleKafka.Record> records) {
    await(
        run,
        "offsets de las posiciones emitidas confirmados",
        () -> {
          var group = system.group();
          var positions = (List<Map<String, Object>>) group.get("partitions");
          for (var record : records) {
            var position =
                positions.stream()
                    .filter(p -> p.get("partition").equals(record.partition()))
                    .findFirst()
                    .orElseThrow();
            require(
                position.get("committedOffset") != null
                    && Long.parseLong(position.get("committedOffset").toString())
                        > Long.parseLong(record.offset()),
                "Offset sin confirmar: " + record.partition() + "/" + record.offset());
          }
          return group;
        });
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> result(DemoRun run, ConsoleKafka.Record record) {
    return await(
        run,
        "resultado SQL de " + record.partition() + "/" + record.offset(),
        () -> {
          var value =
              queries
                  .result(record.topic(), Integer.parseInt(record.partition()), record.offset())
                  .get("result");
          require(value != null, "Sin resultado registrado: " + record.offset());
          return (Map<String, Object>) value;
        });
  }

  private void assertResults(DemoRun run, List<ConsoleKafka.Record> records, String outcome) {
    assertResults(run, records, outcome, null);
  }

  private void assertResults(
      DemoRun run, List<ConsoleKafka.Record> records, String outcome, String checkId) {
    var rows = new ArrayList<Map<String, Object>>();
    try {
      for (var record : records) {
        var row = result(run, record);
        rows.add(row);
        require(
            outcome.equals(row.get("outcome")),
            "Outcome esperado " + outcome + "; observado " + row.get("outcome"));
        require(record.key().equals(row.get("numericOrderId")), "Resultado asociado a otra orden");
      }
      if (checkId != null)
        run.observe(
            checkId,
            rows.size()
                + (outcome.equals("APPLIED")
                    ? " mensajes aplicados (APPLIED)"
                    : " mensajes reconocidos como duplicados (DUPLICATE), sin volver a aplicar")
                + ", vinculados en SQL a las posiciones y órdenes publicadas.",
            true,
            Map.of("results", rows, "messages", records));
    } catch (RuntimeException ex) {
      if (checkId != null) {
        if (rows.isEmpty()) run.diagnostic(checkId, DemoRun.message(ex));
        else
          run.observe(
              checkId,
              "Resultados observados: "
                  + rows.stream().map(row -> row.get("outcome").toString()).toList()
                  + ". "
                  + DemoRun.message(ex),
              false,
              Map.of("results", List.copyOf(rows), "messages", records));
      }
      throw ex;
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> expectOrder(
      DemoRun run, String checkId, long order, String status, int accumulated, int... fixes) {
    String id = Long.toString(order);
    var actual = queries.order(id);
    try {
      require(
          status.equals(actual.get("status")),
          "Estado esperado " + status + "; observado " + actual.get("status"));
      require(
          Integer.toString(fixes.length).equals(actual.get("appliedExecutionCount")),
          "Contador incorrecto: " + actual.get("appliedExecutionCount"));
      require(
          new BigDecimal(actual.get("accumulativeNominalAmount").toString())
                  .compareTo(BigDecimal.valueOf(accumulated))
              == 0,
          "Acumulado incorrecto");
      int leaves = status.equals("FILLED") || status.equals("CANCELLED") ? 0 : 10 - accumulated;
      require(
          new BigDecimal(actual.get("leavesNominalAmount").toString())
                  .compareTo(BigDecimal.valueOf(leaves))
              == 0,
          "Restante incorrecto");
      var ledger = (List<Map<String, String>>) actual.get("ledger");
      require(ledger.size() == fixes.length, "Tamaño del ledger incorrecto");
      long previous = 0;
      for (int i = 0; i < fixes.length; i++) {
        var entry = ledger.get(i);
        long ledgerId = Long.parseLong(entry.get("ledgerId"));
        require(ledgerId > previous, "Ledger fuera de orden");
        previous = ledgerId;
        var payload = ConsoleValues.JSON.readTree(entry.get("payload"));
        require(
            payload.path("fixId").asInt() == fixes[i]
                && payload.path("numericOrderId").asLong() == order,
            "Secuencia o identidad de ledger incorrecta");
        require("VSCPC".equals(payload.path("ticker").asString()), "Campo adicional perdido");
      }
      var first = instances.order("app-1", id);
      var second = instances.order("app-2", id);
      require(
          (int) first.get("statusCode") == 200 && (int) second.get("statusCode") == 200,
          "Consulta HTTP no disponible");
      require(
          first.get("data").equals(second.get("data")),
          "Las instancias no devuelven el mismo estado y ledger");
      run.observe(
          checkId,
          describeOrder(run, order, actual)
              + " Historial en secuencia y consultas de ambas instancias coincidentes.",
          true,
          Map.of("orderId", id, "snapshot", actual, "instances", List.of(first, second)));
      return actual;
    } catch (RuntimeException ex) {
      run.observe(
          checkId,
          describeOrder(run, order, actual) + " " + DemoRun.message(ex),
          false,
          Map.of("orderId", id, "snapshot", actual));
      throw ex;
    }
  }

  private static String describeOrder(DemoRun run, long order, Map<String, Object> actual) {
    String status =
        switch (actual.get("status").toString()) {
          case "NEW" -> "creada";
          case "PARTIALLY_FILLED" -> "parcialmente ejecutada";
          case "FILLED" -> "completada";
          case "CANCELLED" -> "cancelada";
          default -> actual.get("status").toString();
        };
    return run.orderLabel(order)
        + " "
        + status
        + ": acumulado "
        + new BigDecimal(actual.get("accumulativeNominalAmount").toString())
            .stripTrailingZeros()
            .toPlainString()
        + ", restante "
        + new BigDecimal(actual.get("leavesNominalAmount").toString())
            .stripTrailingZeros()
            .toPlainString()
        + ", "
        + actual.get("appliedExecutionCount")
        + ("1".equals(actual.get("appliedExecutionCount"))
            ? " mensaje aplicado y "
            : " mensajes aplicados y ")
        + ((List<?>) actual.get("ledger")).size()
        + (((List<?>) actual.get("ledger")).size() == 1
            ? " entrada de historial."
            : " entradas de historial.");
  }

  private static Map<String, Object> snapshot(Map<String, Object> observed) {
    var snapshot = new LinkedHashMap<>(observed);
    snapshot.remove("source");
    snapshot.remove("observedAt");
    return snapshot;
  }

  private static <T> T await(DemoRun run, String expected, Supplier<T> check) {
    String last = "Sin observación";
    do {
      run.checkCancellation();
      try {
        return check.get();
      } catch (RuntimeException ex) {
        last = DemoRun.message(ex);
      }
      if (run.expired())
        throw new IllegalStateException(
            "No se observó " + expected + " dentro del plazo. Último diagnóstico: " + last);
      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new java.util.concurrent.CancellationException();
      }
    } while (true);
  }

  private static void require(boolean condition, String message) {
    if (!condition) throw new IllegalStateException(message);
  }
}
