package com.maxcapital.challenge.demo.recovery;

import com.maxcapital.challenge.application.InputPosition;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

/** Control temporal de ensayo; no participa en la decisión de negocio. */
public final class RecoveryBarrier {
  public enum Boundary {
    BEFORE_SQL_COMMIT,
    AFTER_SQL_COMMIT,
    AFTER_SEND_ACK_BEFORE_KAFKA_COMMIT
  }

  public record Arm(
      String runId, long numericOrderId, long fixId, Boundary boundary, long ttlMillis) {}

  public record Control(String runId, String token, boolean rollback) {}

  private final String instance;
  private final String generation = UUID.randomUUID().toString();
  private Arm arm;
  private String token;
  private String state = "IDLE";
  private String reachedAt;
  private String failure;
  private InputPosition position;
  private long deadline;
  private boolean rollback;
  private boolean waiting;

  public RecoveryBarrier(String instance) {
    this.instance = instance;
  }

  public synchronized Map<String, Object> arm(Arm request) {
    expire();
    if (request.runId() == null
        || request.runId().isBlank()
        || request.runId().length() > 100
        || request.numericOrderId() <= 0
        || request.fixId() <= 0
        || request.boundary() == null
        || request.ttlMillis() < 100
        || request.ttlMillis() > 45000) throw new IllegalArgumentException("Barrera inválida");
    if (waiting || state.equals("ARMED") || state.equals("REACHED")) conflict();
    arm = request;
    token = UUID.randomUUID().toString();
    deadline = System.nanoTime() + java.time.Duration.ofMillis(request.ttlMillis()).toNanos();
    state = "ARMED";
    position = null;
    reachedAt = null;
    failure = null;
    rollback = false;
    return observe();
  }

  public synchronized Map<String, Object> control(Control control) {
    expire();
    if (arm == null || !arm.runId().equals(control.runId()) || !token.equals(control.token()))
      conflict();
    if (state.equals("ARMED") || state.equals("REACHED")) {
      rollback = control.rollback();
      state = rollback ? "CANCELLED" : "RELEASED";
      notifyAll();
    }
    return observe();
  }

  public synchronized void reach(Boundary boundary, InputPosition input, long order, long fix) {
    expire();
    if (arm == null
        || !state.equals("ARMED")
        || arm.boundary() != boundary
        || arm.numericOrderId() != order
        || arm.fixId() != fix) return;
    position = input;
    reachedAt = Instant.now().toString();
    state = "REACHED";
    waiting = true;
    notifyAll();
    while (state.equals("REACHED")) {
      expire();
      if (!state.equals("REACHED")) break;
      try {
        wait(Math.max(1, Math.min(100, (deadline - System.nanoTime()) / 1_000_000)));
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        state = "CANCELLED";
        waiting = false;
        throw new IllegalStateException("Barrera interrumpida; ER pendiente", ex);
      }
    }
    waiting = false;
    if (rollback || state.equals("EXPIRED"))
      throw new IllegalStateException("Barrera " + state + "; ER pendiente: " + input);
  }

  public synchronized void failed(InputPosition input, Exception exception) {
    if (input.equals(position)) {
      var cause = exception;
      while (cause.getCause() instanceof Exception nested && nested != cause) cause = nested;
      failure = exception + ": " + cause;
    }
  }

  public synchronized Map<String, Object> observe() {
    expire();
    var result = new LinkedHashMap<String, Object>();
    result.put("instanceId", instance);
    result.put("generation", generation);
    result.put("pid", Long.toString(ProcessHandle.current().pid()));
    result.put("state", state);
    result.put("observedAt", Instant.now().toString());
    result.put("runId", arm == null ? null : arm.runId());
    result.put("token", token);
    result.put("numericOrderId", arm == null ? null : Long.toString(arm.numericOrderId()));
    result.put("fixId", arm == null ? null : Long.toString(arm.fixId()));
    result.put("boundary", arm == null ? null : arm.boundary().name());
    result.put(
        "position",
        position == null
            ? null
            : Map.of(
                "topic",
                position.topic(),
                "partition",
                Integer.toString(position.partition()),
                "offset",
                Long.toString(position.offset())));
    result.put("reachedAt", reachedAt);
    result.put("failure", failure);
    return result;
  }

  private void expire() {
    if ((state.equals("ARMED") || state.equals("REACHED")) && System.nanoTime() >= deadline) {
      state = "EXPIRED";
      notifyAll();
    }
  }

  private static void conflict() {
    throw new ResponseStatusException(HttpStatus.CONFLICT, "Barrera ocupada o intento ajeno");
  }
}
