package com.maxcapital.challenge.demo;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class RecoveryScenariosTest {
  @Test
  void neverKillsAnInstanceWhenTheBarrierWasNotObserved() {
    var instances = mock(InstanceAccess.class);
    var docker = mock(DockerAccess.class);
    var kafka = mock(ConsoleKafka.class);
    var recovery =
        new RecoveryScenarios(
            kafka, mock(ConsoleQueries.class), mock(ConsoleSystem.class), instances, docker);
    var steps = mock(ScenarioSteps.class);
    var run = new DemoRun("run", "automatic", List.of("commit-boundary-recovery"), 1);
    run.beginScenario("commit-boundary-recovery");
    var json = new JsonMapper();
    when(instances.read(anyString(), anyString()))
        .thenReturn(
            Map.of(
                "data",
                json.readTree("{\"running\":true,\"assignments\":[{\"partition\":\"0\"}]}")));
    var armed =
        json.readTree("{\"token\":\"token\",\"generation\":\"process\",\"state\":\"ARMED\"}");
    when(instances.recovery(anyString(), anyString(), any())).thenReturn(armed);
    when(kafka.read(anyString(), anyInt(), anyString(), anyInt()))
        .thenReturn(Map.of("endOffset", "0"));
    when(steps.emit(any(), anyLong(), anyInt(), anyString(), anyInt(), anyInt()))
        .thenReturn(new ConsoleKafka.Record("execution-reports", "0", "1", "1", "{}"));
    assertThatThrownBy(
            () ->
                recovery.execute(run, "commit-boundary-recovery", steps, new java.util.HashSet<>()))
        .hasMessageContaining("barrera BEFORE_SQL_COMMIT", "dentro del plazo");
    verifyNoInteractions(docker);
  }

  @Test
  void retainsTheObservedBoundaryWhenCancelledBeforeTheDestructiveAction() {
    var kafka = mock(ConsoleKafka.class);
    var queries = mock(ConsoleQueries.class);
    var system = mock(ConsoleSystem.class);
    var instances = mock(InstanceAccess.class);
    var docker = mock(DockerAccess.class);
    var steps = mock(ScenarioSteps.class);
    var recovery = new RecoveryScenarios(kafka, queries, system, instances, docker);
    var run = new DemoRun("run", "automatic", List.of("commit-boundary-recovery"), 1000);
    run.beginScenario("commit-boundary-recovery");
    var json = new JsonMapper();
    var armed =
        json.readTree("{\"token\":\"token\",\"generation\":\"process\",\"state\":\"ARMED\"}");
    var reached =
        json.readTree(
            "{\"token\":\"token\",\"generation\":\"process\",\"state\":\"REACHED\",\"position\":{\"partition\":\"0\",\"offset\":\"1\"}}");
    when(instances.read(anyString(), anyString()))
        .thenReturn(
            Map.of(
                "data",
                json.readTree("{\"running\":true,\"assignments\":[{\"partition\":\"0\"}]}")));
    when(instances.recovery(anyString(), eq("arm"), any())).thenReturn(armed);
    when(instances.recovery(anyString(), eq(""), isNull())).thenReturn(reached);
    when(kafka.read(anyString(), anyInt(), anyString(), anyInt()))
        .thenReturn(Map.of("endOffset", "0"));
    when(steps.emit(any(), anyLong(), anyInt(), anyString(), anyInt(), anyInt()))
        .thenReturn(new ConsoleKafka.Record("execution-reports", "0", "1", "1", "{}"));
    when(queries.result(anyString(), anyInt(), anyString()))
        .thenAnswer(
            call -> {
              run.cancel();
              return java.util.Collections.singletonMap("result", null);
            });
    assertThatThrownBy(
            () ->
                recovery.execute(run, "commit-boundary-recovery", steps, new java.util.HashSet<>()))
        .isInstanceOf(CancellationException.class);
    verifyNoInteractions(docker);
    run.finish("CANCELLED", "cancelled");
    var scenario = (Map<?, ?>) ((List<?>) run.snapshot().get("scenarioResults")).getFirst();
    var check =
        ((List<?>) scenario.get("checks"))
            .stream()
                .map(value -> (Map<?, ?>) value)
                .filter(value -> value.get("id").equals("sql-open"))
                .findFirst()
                .orElseThrow();
    assertThat(check.get("status")).isEqualTo("INCOMPLETE");
    assertThat(((Map<?, ?>) check.get("references")).get("barrier")).isEqualTo(reached);
  }

  @Test
  void traversesPagesAcrossBothPartitionsAndRejectsAnIncompleteRange() {
    var kafka = mock(ConsoleKafka.class);
    var recovery =
        new RecoveryScenarios(
            kafka,
            mock(ConsoleQueries.class),
            mock(ConsoleSystem.class),
            mock(InstanceAccess.class),
            mock(DockerAccess.class));
    when(kafka.read("settlements", 0, "0", 1)).thenReturn(Map.of("endOffset", "4"));
    when(kafka.read("settlements", 1, "0", 1)).thenReturn(Map.of("endOffset", "0"));
    var first = new ConsoleKafka.Record("settlements", "0", "0", "1", "{}");
    var second = new ConsoleKafka.Record("settlements", "0", "2", "2", "{}");
    when(kafka.read("settlements", 0, "0", 200))
        .thenReturn(Map.of("records", List.of(first), "nextCursor", "2"));
    when(kafka.read("settlements", 0, "2", 200))
        .thenReturn(Map.of("records", List.of(second), "nextCursor", "4"));
    assertThat(recovery.outputs(Map.of(0, "0", 1, "0"))).containsExactly(first, second);
    when(kafka.read("settlements", 0, "2", 200))
        .thenReturn(Map.of("records", List.of(), "nextCursor", "2"));
    assertThatThrownBy(() -> recovery.outputs(Map.of(0, "0", 1, "0")))
        .hasMessageContaining("Lectura parcial");
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void cancellationInvalidatesTheRunAndRestartsAStoppedListenerBeforeReleasingTheEnvironment(
      boolean controlFails) {
    var kafka = mock(ConsoleKafka.class);
    var queries = mock(ConsoleQueries.class);
    var system = mock(ConsoleSystem.class);
    var instances = mock(InstanceAccess.class);
    var docker = mock(DockerAccess.class);
    var steps = mock(ScenarioSteps.class);
    var recovery = new RecoveryScenarios(kafka, queries, system, instances, docker);
    var run = new DemoRun("run", "automatic", List.of("commit-boundary-recovery"), 1000);
    run.beginScenario("commit-boundary-recovery");
    var json = new JsonMapper();
    var barrier =
        json.readTree("{\"token\":\"token\",\"generation\":\"process\",\"state\":\"ARMED\"}");
    when(instances.read(eq("app-1"), anyString()))
        .thenReturn(
            Map.of(
                "data",
                json.readTree("{\"running\":true,\"assignments\":[{\"partition\":\"0\"}]}")));
    when(instances.read(eq("app-2"), anyString()))
        .thenReturn(
            Map.of(
                "data",
                json.readTree("{\"running\":true,\"assignments\":[{\"partition\":\"1\"}]}")));
    when(instances.recovery(eq("app-1"), eq("arm"), any()))
        .thenAnswer(
            call -> {
              run.cancel();
              return barrier;
            });
    when(instances.recovery(eq("app-1"), eq(""), isNull())).thenReturn(barrier);
    when(steps.emit(any(), anyLong(), anyInt(), anyString(), anyInt(), anyInt()))
        .thenReturn(new ConsoleKafka.Record("execution-reports", "0", "1", "1", "{}"));
    when(kafka.read(anyString(), anyInt(), anyString(), anyInt()))
        .thenReturn(Map.of("endOffset", "0"));
    assertThatThrownBy(
            () ->
                recovery.execute(run, "commit-boundary-recovery", steps, new java.util.HashSet<>()))
        .isInstanceOf(CancellationException.class);
    when(instances.read(eq("app-1"), anyString()))
        .thenReturn(
            Map.of("data", json.readTree("{\"running\":false,\"assignments\":[]}")),
            Map.of(
                "data",
                json.readTree("{\"running\":true,\"assignments\":[{\"partition\":\"0\"}]}")));
    if (controlFails) {
      when(instances.recovery(eq("app-1"), eq("control"), any()))
          .thenThrow(new IllegalStateException("Control unavailable"));
      when(instances.read(eq("app-1"), anyString()))
          .thenReturn(
              Map.of(
                  "data",
                  json.readTree("{\"running\":true,\"assignments\":[{\"partition\":\"0\"}]}")));
    }
    when(system.group())
        .thenReturn(
            Map.of(
                "members",
                List.of(
                    Map.of("assignments", List.of(Map.of())),
                    Map.of("assignments", List.of(Map.of())))));
    recovery.restore(run);
    verify(instances)
        .recovery("app-1", "control", Map.of("runId", "run", "token", "token", "rollback", true));
    verify(docker).control("app-1", "restart");
    assertThat(((Map<?, ?>) run.snapshot().get("restoration")).get("status")).isEqualTo("RESTORED");
  }

  @Test
  void cleanupFailureNeverReportsSuccessAndKeepsTheGateUntilTheAttemptFinishes() throws Exception {
    var steps = mock(ScenarioSteps.class);
    var gate = new DemoGate();
    var cleaning = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    doAnswer(
            call -> {
              throw new CancellationException("cancelled");
            })
        .when(steps)
        .execute(any(), anyString(), any());
    doAnswer(
            call -> {
              cleaning.countDown();
              release.await(3, TimeUnit.SECONDS);
              throw new IllegalStateException("Docker unavailable");
            })
        .when(steps)
        .restoreRecovery(any());
    var runner = new ScenarioRunner(new ScenarioCatalog(), steps, gate, 1000);
    try {
      var started = runner.start("commit-boundary-recovery", "automatic");
      String id = started.get("id").toString();
      assertThat(cleaning.await(3, TimeUnit.SECONDS)).isTrue();
      assertThatThrownBy(() -> gate.manual(() -> "mutation")).hasMessageContaining("409");
      release.countDown();
      await()
          .atMost(Duration.ofSeconds(3))
          .until(() -> runner.get(id).get("status").equals("FAILED"));
      assertThat(runner.get(id).get("error").toString())
          .contains("restaurar", "Docker unavailable");
      assertThat(gate.manual(() -> "ready")).isEqualTo("ready");
    } finally {
      release.countDown();
      runner.close();
    }
  }

  @Test
  void restorationFailureInvalidatesAnOtherwiseVerifiedScenario() {
    var steps = mock(ScenarioSteps.class);
    doAnswer(
            call -> {
              DemoRun run = call.getArgument(0);
              run.step(
                  "verified",
                  "observed",
                  () -> {
                    for (String check :
                        List.of(
                            "ready",
                            "snapshot-1",
                            "snapshot-2",
                            "snapshot-3",
                            "snapshot-4",
                            "clock"))
                      run.observe(check, "observed", true, Map.of("source", "test"));
                    return Map.of();
                  });
              return null;
            })
        .when(steps)
        .execute(any(), anyString(), any());
    doThrow(new IllegalStateException("restoration failed")).when(steps).restoreRecovery(any());
    var runner = new ScenarioRunner(new ScenarioCatalog(), steps, new DemoGate(), 1000);
    try {
      String id = runner.start("quantity-snapshots", "automatic").get("id").toString();
      await()
          .atMost(Duration.ofSeconds(3))
          .until(() -> runner.get(id).get("status").equals("FAILED"));
      assertThat(runner.get(id).get("passedScenarios")).isEqualTo(0);
      var results = (List<?>) runner.get(id).get("scenarioResults");
      assertThat(((Map<?, ?>) results.getFirst()).get("status")).isEqualTo("FAILED");
    } finally {
      runner.close();
    }
  }
}
