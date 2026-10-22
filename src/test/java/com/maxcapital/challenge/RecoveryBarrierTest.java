package com.maxcapital.challenge;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import com.maxcapital.challenge.application.InputPosition;
import com.maxcapital.challenge.demo.recovery.RecoveryBarrier;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class RecoveryBarrierTest {
  @Test
  void scopesOneShotControlsAndRejectsStaleAttemptsAcrossGenerations() {
    var barrier = new RecoveryBarrier("a");
    var request =
        new RecoveryBarrier.Arm("run", 1, 2, RecoveryBarrier.Boundary.BEFORE_SQL_COMMIT, 2000);
    var armed = barrier.arm(request);
    var control = new RecoveryBarrier.Control("run", armed.get("token").toString(), false);
    barrier.identify(new InputPosition("in", 0, 4), "foreign", 1, 2);
    barrier.reach(request.boundary(), new InputPosition("in", 0, 4), 1, 2);
    assertThat(barrier.observe().get("state")).isEqualTo("ARMED");
    barrier.identify(new InputPosition("in", 0, 4), "run", 1, 2);
    assertThatThrownBy(() -> barrier.arm(request)).hasMessageContaining("409");
    barrier.reach(request.boundary(), new InputPosition("in", 0, 4), 2, 2);
    assertThat(barrier.observe().get("state")).isEqualTo("ARMED");
    var pending =
        CompletableFuture.runAsync(
            () -> barrier.reach(request.boundary(), new InputPosition("in", 0, 4), 1, 2));
    await()
        .atMost(Duration.ofSeconds(2))
        .until(() -> barrier.observe().get("state").equals("REACHED"));
    assertThatThrownBy(
            () -> barrier.control(new RecoveryBarrier.Control("other", control.token(), false)))
        .hasMessageContaining("409");
    barrier.control(control);
    pending.join();
    barrier.reach(request.boundary(), new InputPosition("in", 0, 4), 1, 2);
    barrier.arm(request);
    assertThatThrownBy(() -> barrier.control(control)).hasMessageContaining("409");
    var restarted = new RecoveryBarrier("a");
    assertThat(restarted.observe().get("generation")).isNotEqualTo(armed.get("generation"));
    assertThatThrownBy(() -> restarted.control(control)).hasMessageContaining("409");
  }

  @Test
  void expiryAndCancellationFailTheAttemptInsteadOfContinuing() {
    var barrier = new RecoveryBarrier("a");
    barrier.arm(
        new RecoveryBarrier.Arm("run", 1, 1, RecoveryBarrier.Boundary.AFTER_SQL_COMMIT, 100));
    barrier.identify(new InputPosition("in", 0, 1), "run", 1, 1);
    assertThatThrownBy(
            () ->
                barrier.reach(
                    RecoveryBarrier.Boundary.AFTER_SQL_COMMIT, new InputPosition("in", 0, 1), 1, 1))
        .hasMessageContaining("EXPIRED");
    var next =
        barrier.arm(
            new RecoveryBarrier.Arm(
                "next", 2, 1, RecoveryBarrier.Boundary.BEFORE_SQL_COMMIT, 2000));
    barrier.identify(new InputPosition("in", 1, 1), "next", 2, 1);
    var pending =
        CompletableFuture.runAsync(
            () ->
                barrier.reach(
                    RecoveryBarrier.Boundary.BEFORE_SQL_COMMIT,
                    new InputPosition("in", 1, 1),
                    2,
                    1));
    await()
        .atMost(Duration.ofSeconds(2))
        .until(() -> barrier.observe().get("state").equals("REACHED"));
    barrier.control(new RecoveryBarrier.Control("next", next.get("token").toString(), true));
    assertThatThrownBy(pending::join)
        .hasRootCauseMessage(
            "Barrera CANCELLED; ER pendiente: InputPosition[topic=in, partition=1, offset=1]");
  }
}
