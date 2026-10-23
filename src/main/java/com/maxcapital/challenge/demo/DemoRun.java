package com.maxcapital.challenge.demo;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

final class DemoRun {
  @FunctionalInterface
  interface Action {
    Map<String, Object> execute();
  }

  final String id;
  final String mode;
  final List<String> scenarios;
  final long timeoutMillis;
  final String createdAt = Instant.now().toString();
  private final List<Map<String, Object>> steps = new ArrayList<>();
  private final List<String> orders = new ArrayList<>();
  private final List<Map<String, Object>> messages = new ArrayList<>();
  private final Map<String, ScenarioResult> scenarioResults = new LinkedHashMap<>();
  private ScenarioResult current;
  private Map<String, Object> currentStep;
  private String status = "RUNNING";
  private String error;
  private boolean cancelled;
  private boolean permit;
  private Map<String, Object> restoration = Map.of();
  private int passed;
  private long deadline;

  DemoRun(String id, String mode, List<String> scenarios, long timeoutMillis) {
    this.id = id;
    this.mode = mode;
    this.scenarios = List.copyOf(scenarios);
    this.timeoutMillis = timeoutMillis;
    scenarios.forEach(scenario -> scenarioResults.put(scenario, new ScenarioResult(scenario)));
  }

  private static final class ScenarioResult {
    final String id;
    final Map<String, Map<String, Object>> checks;
    final List<Map<String, String>> orders = new ArrayList<>();
    final List<Map<String, Object>> evidenceMessages = new ArrayList<>();
    final List<Map<String, Object>> orderSnapshots = new ArrayList<>();
    final List<Map<String, Object>> outputs = new ArrayList<>();
    String status = "PENDING";
    String conclusion = "Todavía no se ejecutó esta prueba.";

    ScenarioResult(String id) {
      this.id = id;
      checks = ScenarioGuidance.checks(id);
    }

    Map<String, Object> snapshot() {
      var evidence = new LinkedHashMap<String, Object>();
      evidence.put("messages", evidenceMessages.stream().map(DemoRun::copyEvidence).toList());
      evidence.put("orderSnapshots", List.copyOf(orderSnapshots));
      evidence.put("outputs", List.copyOf(outputs));
      return Map.of(
          "scenarioId",
          id,
          "status",
          status,
          "conclusion",
          conclusion,
          "orders",
          List.copyOf(orders),
          "evidence",
          evidence,
          "checks",
          checks.values().stream().map(LinkedHashMap::new).toList());
    }
  }

  synchronized void beginScenario(String id) {
    current = scenarioResults.get(id);
    current.status = "RUNNING";
    current.conclusion = "Prueba en curso. Las filas indican lo observado hasta ahora.";
  }

  synchronized String orderLabel(long id) {
    return current.orders.stream()
        .filter(o -> o.get("numericOrderId").equals(Long.toString(id)))
        .map(o -> o.get("label"))
        .findFirst()
        .orElse("Orden");
  }

  synchronized void observe(
      String checkId, String observed, boolean passed, Map<String, ?> references) {
    var check = current.checks.get(checkId);
    if (check == null) throw new IllegalArgumentException("Verificación desconocida: " + checkId);
    check.put("observed", observed);
    check.put("status", passed ? "PASSED" : "WAITING");
    check.put("observedAt", Instant.now().toString());
    check.put("stepId", currentStep.get("id"));
    check.put("references", Map.copyOf(references));
  }

  synchronized void diagnostic(String checkId, String error) {
    var check = current.checks.get(checkId);
    check.put("status", "WAITING");
    check.put("diagnostic", error);
    check.put("stepId", currentStep.get("id"));
  }

  void step(String title, String expected, Action action) {
    var step = new LinkedHashMap<String, Object>();
    synchronized (this) {
      checkCancellation();
      step.put(
          "id",
          current.id
              + "-step-"
              + (steps.stream().filter(s -> current.id.equals(s.get("scenarioId"))).count() + 1));
      step.put("scenarioId", current.id);
      step.put("title", title);
      step.put("expected", expected);
      step.put("status", "PENDING");
      steps.add(step);
      currentStep = step;
      if (mode.equals("step")) {
        status = "WAITING";
        current.status = "WAITING";
        long until = System.nanoTime() + java.time.Duration.ofMinutes(30).toNanos();
        while (!permit && !cancelled) {
          if (System.nanoTime() > until)
            throw new IllegalStateException("La pausa de 30 minutos venció");
          try {
            wait(1000);
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new CancellationException();
          }
        }
        permit = false;
      }
      checkCancellation();
      status = "RUNNING";
      current.status = "RUNNING";
      step.put("status", "RUNNING");
      deadline = System.nanoTime() + java.time.Duration.ofMillis(timeoutMillis).toNanos();
    }
    try {
      var evidence = action.execute();
      synchronized (this) {
        step.put("evidence", evidence);
        step.put("observedAt", Instant.now().toString());
        step.put("status", "PASSED");
      }
    } catch (RuntimeException ex) {
      synchronized (this) {
        step.put("status", ex instanceof CancellationException ? "CANCELLED" : "FAILED");
        step.put("error", message(ex));
      }
      throw ex;
    }
  }

  synchronized void next() {
    if (!mode.equals("step") || !status.equals("WAITING"))
      throw new ResponseStatusException(
          HttpStatus.CONFLICT, "No hay un paso esperando continuación");
    status = "RUNNING";
    permit = true;
    notifyAll();
  }

  synchronized void cancel() {
    if (status.equals("RUNNING") || status.equals("WAITING")) {
      cancelled = true;
      notifyAll();
    }
  }

  synchronized void checkCancellation() {
    if (cancelled)
      throw new CancellationException("Ejecución cancelada; se conservan los efectos confirmados");
  }

  synchronized boolean expired() {
    return System.nanoTime() > deadline;
  }

  synchronized void order(String id) {
    orders.add(id);
    current.orders.add(
        Map.of("numericOrderId", id, "label", "Orden " + (char) ('A' + current.orders.size())));
  }

  synchronized void message(ConsoleKafka.Record record) {
    message(record, "NONE");
  }

  synchronized void message(ConsoleKafka.Record record, String transaction) {
    var now = Instant.now().toString();
    var message =
        new LinkedHashMap<String, Object>(
            Map.of(
                "topic",
                record.topic(),
                "partition",
                record.partition(),
                "offset",
                record.offset(),
                "key",
                record.key(),
                "payload",
                record.payload(),
                "transaction",
                transaction));
    message.put("id", record.topic() + "/" + record.partition() + "/" + record.offset());
    message.put("scenarioId", current.id);
    message.put("stepId", currentStep.get("id"));
    message.put("observedAt", now);
    messages.add(message);

    var position = new LinkedHashMap<String, Object>();
    position.put("topic", record.topic());
    position.put("partition", record.partition());
    position.put("offset", record.offset());
    var report = new LinkedHashMap<String, Object>();
    try {
      var payload = ConsoleValues.JSON.readTree(record.payload());
      for (String field :
          List.of(
              "fixId",
              "numericOrderId",
              "status",
              "nominalAmounts",
              "leavesNominalAmount",
              "accumulativeNominalAmount",
              "transactionTime")) {
        var value = payload.get(field);
        if (value != null && !value.isNull())
          report.put(field, value.isValueNode() ? value.asString() : value.toString());
      }
    } catch (RuntimeException ignored) {
      // Keep the original payload and leave the summary empty for invalid JSON.
    }
    var evidence = new LinkedHashMap<String, Object>();
    evidence.put("id", message.get("id"));
    evidence.put("source", position);
    evidence.put("scenarioId", current.id);
    evidence.put("stepId", currentStep.get("id"));
    evidence.put("observedAt", now);
    evidence.put("key", record.key());
    evidence.put("payload", record.payload());
    evidence.put("transaction", transaction);
    evidence.put("report", report);
    evidence.put("sqlObservationStatus", "NOT_QUERIED");
    evidence.put("sqlObservations", new ArrayList<Map<String, Object>>());
    current.evidenceMessages.add(evidence);
  }

  synchronized void sqlObservation(
      ConsoleKafka.Record record, String status, Map<String, Object> result) {
    var id = record.topic() + "/" + record.partition() + "/" + record.offset();
    for (var message : current.evidenceMessages) {
      if (id.equals(message.get("id"))) {
        message.put("sqlObservationStatus", status);
        var observation = new LinkedHashMap<String, Object>();
        observation.put("status", status);
        observation.put("observedAt", Instant.now().toString());
        observation.put("source", "PostgreSQL");
        observation.put("result", result.get("result"));
        @SuppressWarnings("unchecked")
        var observations = (List<Map<String, Object>>) message.get("sqlObservations");
        observations.add(observation);
        return;
      }
    }
  }

  synchronized boolean hasSqlObservation(ConsoleKafka.Record record) {
    String id = record.topic() + "/" + record.partition() + "/" + record.offset();
    return current.evidenceMessages.stream()
        .filter(message -> id.equals(message.get("id")))
        .map(message -> (List<?>) message.get("sqlObservations"))
        .anyMatch(observations -> !observations.isEmpty());
  }

  synchronized void orderSnapshot(
      String id, String label, String checkId, Map<String, Object> snapshot) {
    var value = new LinkedHashMap<String, Object>();
    value.put("numericOrderId", id);
    value.put("label", label);
    value.put("scenarioId", current.id);
    value.put("stepId", currentStep.get("id"));
    value.put("checkId", checkId);
    value.put("observedAt", Instant.now().toString());
    value.put("source", "PostgreSQL");
    value.put("snapshot", snapshot);
    current.orderSnapshots.add(value);
  }

  synchronized void output(
      ConsoleKafka.Record record,
      String type,
      String inputPosition,
      Map<Integer, String> startCursors,
      Map<Integer, String> endOffsets) {
    String id = record.topic() + "/" + record.partition() + "/" + record.offset();
    if (current.outputs.stream().anyMatch(existing -> id.equals(existing.get("id")))) return;
    var value = new LinkedHashMap<String, Object>();
    value.put("id", id);
    value.put(
        "source",
        Map.of(
            "topic", record.topic(), "partition", record.partition(), "offset", record.offset()));
    value.put("key", record.key());
    value.put("payload", record.payload());
    value.put("type", type);
    value.put("inputPosition", inputPosition);
    value.put("scenarioId", current.id);
    value.put("stepId", currentStep.get("id"));
    value.put("observedAt", Instant.now().toString());
    value.put("readSource", "Kafka read_committed");
    value.put("startCursors", startCursors);
    value.put("endOffsets", endOffsets);
    current.outputs.add(value);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> copyEvidence(Map<String, Object> original) {
    var copy = new LinkedHashMap<>(original);
    Object observations = original.get("sqlObservations");
    if (observations instanceof List<?> list)
      copy.put(
          "sqlObservations",
          list.stream().map(value -> new LinkedHashMap<>((Map<String, Object>) value)).toList());
    return copy;
  }

  synchronized void passed() {
    if (current.checks.values().stream().anyMatch(check -> !"PASSED".equals(check.get("status"))))
      throw new IllegalStateException("Quedaron verificaciones sin completar en " + current.id);
    current.status = "PASSED";
    List<String> summary =
        switch (current.id) {
          case "interleaved-orders" -> List.of("order-a", "order-b");
          case "restart-persistence" -> List.of("preserved", "duplicates");
          case "quantity-snapshots" -> List.of("clock", "snapshot-4");
          case "terminal-duplicates" -> List.of("duplicates", "final");
          case "committed-input-only" -> List.of("visibility", "no-result", "final");
          case "poison-recovery" -> List.of("paused", "recovered", "outputs", "restored");
          case "settlement-once" -> List.of("settlement", "recovery", "restored");
          case "commit-boundary-recovery" ->
              List.of(
                  "sql-open", "sql-committed", "send-ack", "rebalance", "both-down", "restored");
          default -> throw new IllegalStateException("Escenario desconocido");
        };
    current.conclusion =
        summary.stream()
            .map(id -> current.checks.get(id).get("observed").toString())
            .collect(java.util.stream.Collectors.joining(" "));
    passed++;
  }

  synchronized void finish(String status, String error) {
    this.status = status;
    this.error = error;
    if (status.equals("FAILED") && current != null && current.status.equals("PASSED")) {
      current.status = "FAILED";
      passed--;
    }
    for (var scenario : scenarioResults.values()) {
      if (scenario.status.equals("PENDING")) {
        scenario.status = "NOT_RUN";
        scenario.conclusion = "No se ejecutó: el recorrido terminó antes de llegar a esta prueba.";
      } else if (!scenario.status.equals("PASSED")) {
        scenario.status = status;
        scenario.conclusion =
            status.equals("CANCELLED")
                ? "Prueba cancelada. Se conservan los efectos y verificaciones obtenidos; las comprobaciones pendientes no se completaron."
                : "No se completó la prueba. Revisá el diagnóstico y las verificaciones pendientes; los datos confirmados se conservan.";
        scenario.checks.values().stream()
            .filter(check -> "WAITING".equals(check.get("status")))
            .forEach(
                check -> check.put("status", status.equals("CANCELLED") ? "INCOMPLETE" : "FAILED"));
      }
    }
    if (!steps.isEmpty() && List.of("PENDING", "RUNNING").contains(steps.getLast().get("status"))) {
      steps.getLast().put("status", status);
      steps.getLast().put("error", error);
    }
  }

  synchronized Map<String, Object> snapshot() {
    var result = new LinkedHashMap<String, Object>();
    result.put("id", id);
    result.put("mode", mode);
    result.put("status", status);
    result.put("createdAt", createdAt);
    result.put("scenarioIds", scenarios);
    result.put("passedScenarios", passed);
    result.put("excludedPending", 0);
    result.put("restoration", restoration);
    result.put("orders", List.copyOf(orders));
    result.put("messages", List.copyOf(messages));
    result.put("steps", steps.stream().map(LinkedHashMap::new).toList());
    result.put(
        "scenarioResults",
        scenarioResults.values().stream().map(ScenarioResult::snapshot).toList());
    result.put("error", error);
    return result;
  }

  synchronized void restoration(Map<String, Object> evidence) {
    restoration = Map.copyOf(evidence);
  }

  static String message(Throwable ex) {
    return ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage();
  }
}
