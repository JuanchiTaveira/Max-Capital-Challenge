package com.maxcapital.challenge.demo;

import static com.maxcapital.challenge.demo.ScenarioSteps.*;

import com.maxcapital.challenge.adapter.in.report.ExecutionReportParser;
import com.maxcapital.challenge.application.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Service
@Profile("evaluation-console")
public class QuarantineScenarios {
  private final ConsoleKafka kafka;
  private final ConsoleQueries queries;
  private final QuarantineStore store;
  private final ExecutionReportParser parser;
  private final DockerAccess docker;
  private final RecoveryScenarios recovery;
  private final Map<String, OwnCase> active = new ConcurrentHashMap<>();

  private static final class OwnCase {
    final long order;
    final String effective;
    boolean publishing;
    ConsoleKafka.Record original;

    OwnCase(long order, String effective) {
      this.order = order;
      this.effective = effective;
    }
  }

  public QuarantineScenarios(
      ConsoleKafka kafka,
      ConsoleQueries queries,
      QuarantineStore store,
      ExecutionReportParser parser,
      DockerAccess docker,
      RecoveryScenarios recovery) {
    this.kafka = kafka;
    this.queries = queries;
    this.store = store;
    this.parser = parser;
    this.docker = docker;
    this.recovery = recovery;
  }

  void execute(DemoRun run, ScenarioSteps steps) {
    long order = steps.fresh(run, 0), healthy = steps.fresh(run, 1);
    var own = new OwnCase(order, steps.report(run, order, 2, "PARTIALLY_FILLED", 4, 6));
    active.put(run.id, own);
    var starts = new LinkedHashMap<Integer, String>();
    for (int p = 0; p < 2; p++)
      starts.put(p, kafka.read("settlements", p, "0", 1).get("endOffset").toString());
    var records = new ArrayList<ConsoleKafka.Record>();
    run.step(
        "Crear la orden antes del error",
        "NEW aplicado, contador y ledger en uno",
        () -> {
          var first = steps.emit(run, order, 1, "NEW", 0, 10);
          records.add(first);
          steps.committed(run, List.of(first));
          return steps.expectOrder(run, "created", order, "NEW", 0, 1);
        });
    run.step(
        "Publicar un parcial ilegible y su sucesor",
        "Partición pausada; otra partición continúa",
        () -> {
          own.publishing = true;
          own.original = kafka.publish(Long.toString(order), "{invalid-json:" + run.id);
          run.message(own.original);
          records.add(own.original);
          var successor = steps.emit(run, order, 3, "FILLED", 10, 0);
          records.add(successor);
          var other =
              List.of(
                  steps.emit(run, healthy, 1, "NEW", 0, 10),
                  steps.emit(run, healthy, 2, "FILLED", 10, 0));
          records.addAll(other);
          steps.committed(run, other);
          var entry =
              await(
                  run,
                  "cuarentena y aviso DLQ",
                  () -> {
                    var e = store.find(source(own.original), false).orElseThrow();
                    require(
                        e.state().equals("OPEN") && e.notified(),
                        "Captura o aviso todavía pendientes");
                    return e;
                  });
          require(
              queries.result(successor.topic(), 0, successor.offset()).get("result") == null,
              "FILLED adelantado");
          steps.expectOrder(run, "healthy", healthy, "FILLED", 10, 1, 2);
          var blocked = steps.expectOrder(run, "paused", order, "NEW", 0, 1);
          run.observe(
              "paused",
              "Parcial en cuarentena, FILLED sin aplicar y otra partición completada.",
              true,
              Map.of(
                  "original",
                  own.original,
                  "quarantineState",
                  entry.state(),
                  "dlqNotified",
                  entry.notified(),
                  "order",
                  blocked));
          return Map.of("original", own.original, "successor", successor, "healthy", other);
        });
    run.step(
        "Registrar la corrección y reiniciar los consumidores",
        "Misma posición original y secuencia NEW, parcial, FILLED",
        () -> {
          restore(run);
          steps.ready(run, "ready");
          steps.committed(run, records);
          var current = steps.expectOrder(run, "recovered", order, "FILLED", 10, 1, 2, 3);
          var audit = store.corrections(source(own.original));
          require(
              audit.size() == 1 && audit.getFirst().outcome().equals("APPLIED"),
              "Auditoría sin aplicación única");
          run.observe(
              "recovered",
              "Corrección auditada aplicada en el offset original; secuencia 1, 2, 3 sin duplicar ledger.",
              true,
              Map.of("order", current, "correction", audit.getFirst(), "original", own.original));
          return Map.of("order", current, "audit", audit);
        });
    run.step(
        "Verificar salidas y restauración",
        "Dos settlements, uno por orden completada; originales conservados",
        () -> {
          var outputs =
              await(
                  run,
                  "settlements confirmados",
                  () -> {
                    var rows =
                        recovery.outputs(starts).stream()
                            .filter(
                                r ->
                                    r.key().equals(Long.toString(order))
                                        || r.key().equals(Long.toString(healthy)))
                            .toList();
                    require(
                        rows.size() == 2
                            && rows.stream().map(ConsoleKafka.Record::key).distinct().count() == 2,
                        "Settlement ausente o repetido");
                    return rows;
                  });
          require(
              store.find(source(own.original), false).orElseThrow().state().equals("RESOLVED"),
              "Cuarentena sin resolver");
          run.observe(
              "outputs",
              "Un settlement por cada FILLED; el original y su corrección siguen consultables.",
              true,
              Map.of("settlements", outputs, "source", own.original));
          var ready = steps.ready(run, "restored");
          return Map.of("outputs", outputs, "group", ready);
        });
  }

  void restore(DemoRun run) {
    var own = active.get(run.id);
    if (own == null) return;
    try {
      if (own.original == null) {
        require(
            !own.publishing,
            "Publicación incierta de orden "
                + own.order
                + "; localizar su cuarentena en DLQ antes de corregir");
      } else {
        var p = source(own.original);
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (store.find(p, false).isEmpty()) {
          require(System.nanoTime() < deadline, "Sin captura del original propio " + p);
          sleep();
        }
        var entry = store.find(p, false).orElseThrow();
        if (!entry.state().equals("RESOLVED")) {
          var parsed =
              parser.parse(
                  Long.toString(own.order), own.effective.getBytes(StandardCharsets.UTF_8));
          var accepted =
              store.accept(
                  new QuarantineStore.Correction(
                      "demo:" + run.id,
                      p,
                      Long.toString(own.order),
                      parsed.payload(),
                      "Corrección predefinida del ensayo " + run.id,
                      "ensayo local",
                      "ACCEPTED",
                      null,
                      null),
                  p.partition());
          require(
              !accepted.outcome().equals("REJECTED"),
              "La corrección predefinida fue rechazada: " + p);
          docker.control("app-1", "restart");
          docker.control("app-2", "restart");
          while (!store.find(p, false).orElseThrow().state().equals("RESOLVED")) {
            require(System.nanoTime() < deadline, "Corrección propia pendiente: " + p);
            sleep();
          }
        }
      }
      active.remove(run.id);
      run.restoration(
          Map.of("status", "RESTORED", "scope", "Original propio resuelto; evidencia conservada"));
    } catch (RuntimeException failure) {
      run.restoration(
          Map.of(
              "status",
              "FAILED",
              "action",
              "Consultar /quarantine, corregir la posición propia y reiniciar app-1/app-2",
              "diagnostic",
              DemoRun.message(failure)));
      throw failure;
    }
  }

  private static InputPosition source(ConsoleKafka.Record r) {
    return new InputPosition(
        r.topic(), Integer.parseInt(r.partition()), Long.parseLong(r.offset()));
  }

  private static void sleep() {
    try {
      Thread.sleep(100);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Restauración interrumpida", interrupted);
    }
  }
}
