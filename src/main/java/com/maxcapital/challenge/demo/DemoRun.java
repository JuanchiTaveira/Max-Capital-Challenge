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
  private String status = "RUNNING";
  private String error;
  private boolean cancelled;
  private boolean permit;
  private int passed;
  private long deadline;

  DemoRun(String id, String mode, List<String> scenarios, long timeoutMillis) {
    this.id = id;
    this.mode = mode;
    this.scenarios = List.copyOf(scenarios);
    this.timeoutMillis = timeoutMillis;
  }

  void step(String title, String expected, Action action) {
    var step = new LinkedHashMap<String, Object>();
    synchronized (this) {
      checkCancellation();
      step.put("title", title);
      step.put("expected", expected);
      step.put("status", "PENDING");
      steps.add(step);
      if (mode.equals("step")) {
        status = "WAITING";
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
  }

  synchronized void message(ConsoleKafka.Record record) {
    message(record, "NONE");
  }

  synchronized void message(ConsoleKafka.Record record, String transaction) {
    messages.add(
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
  }

  synchronized void passed() {
    passed++;
  }

  synchronized void finish(String status, String error) {
    this.status = status;
    this.error = error;
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
    result.put("excludedPending", 3);
    result.put("orders", List.copyOf(orders));
    result.put("messages", List.copyOf(messages));
    result.put("steps", steps.stream().map(LinkedHashMap::new).toList());
    result.put("error", error);
    return result;
  }

  static String message(Throwable ex) {
    return ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage();
  }
}
