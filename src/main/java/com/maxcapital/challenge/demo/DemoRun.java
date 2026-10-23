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
    String status = "PENDING";
    String conclusion = "Todavía no se ejecutó esta prueba.";

    ScenarioResult(String id) {
      this.id = id;
      checks = ScenarioGuidance.checks(id);
    }

    Map<String, Object> snapshot() {
      return Map.of(
          "scenarioId",
          id,
          "status",
          status,
          "conclusion",
          conclusion,
          "orders",
          List.copyOf(orders),
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
    messages.add(message);
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
