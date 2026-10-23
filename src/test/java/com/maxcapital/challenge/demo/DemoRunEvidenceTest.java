package com.maxcapital.challenge.demo;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DemoRunEvidenceTest {
  @Test
  void exposesAdditiveScenarioEvidenceLinkedByFullPositionAndStep() {
    var run = new DemoRun("run-1", "automatic", List.of("interleaved-orders"), 1000);
    run.beginScenario("interleaved-orders");
    var record =
        new ConsoleKafka.Record(
            "execution-reports",
            "1",
            "42",
            "9007199254740993",
            """
        {"numericOrderId":9007199254740993,"fixId":2,"status":"PARTIALLY_FILLED",
        "nominalAmounts":10,"accumulativeNominalAmount":7.00000000,"leavesNominalAmount":3}
        """);

    run.step(
        "Publicar",
        "Se registra el mensaje",
        () -> {
          run.message(record);
          run.sqlObservation(
              record,
              "RESULT_FOUND",
              Map.of("result", Map.of("outcome", "APPLIED", "ledgerId", "88")));
          run.orderSnapshot(
              record.key(),
              "Orden A",
              "order-a",
              Map.of("status", "PARTIALLY_FILLED", "ledger", List.of()));
          return Map.of();
        });

    var result = (Map<?, ?>) ((List<?>) run.snapshot().get("scenarioResults")).getFirst();
    var evidence = (Map<?, ?>) result.get("evidence");
    var message = (Map<?, ?>) ((List<?>) evidence.get("messages")).getFirst();
    var position = (Map<?, ?>) message.get("source");
    assertThat(position.get("topic")).isEqualTo("execution-reports");
    assertThat(position.get("partition")).isEqualTo("1");
    assertThat(position.get("offset")).isEqualTo("42");
    assertThat(message.get("stepId")).isEqualTo("interleaved-orders-step-1");
    var observations = (List<?>) message.get("sqlObservations");
    assertThat(((Map<?, ?>) observations.getFirst()).get("status")).isEqualTo("RESULT_FOUND");
    assertThat(((Map<?, ?>) observations.getFirst()).get("result"))
        .isEqualTo(Map.of("outcome", "APPLIED", "ledgerId", "88"));
    var report = (Map<?, ?>) message.get("report");
    assertThat(report.get("numericOrderId")).isEqualTo("9007199254740993");
    assertThat(report.get("accumulativeNominalAmount")).isEqualTo("7.00000000");
    assertThat(evidence.get("orderSnapshots")).asList().hasSize(1);
    assertThat(evidence.get("outputs")).asList().isEmpty();
    assertThat(
            result.containsKey("scenarioId")
                && result.containsKey("status")
                && result.containsKey("conclusion")
                && result.containsKey("orders")
                && result.containsKey("checks"))
        .isTrue();
  }

  @Test
  void invalidPayloadKeepsOriginalWithoutInventingReportSummary() {
    var run = new DemoRun("run-2", "automatic", List.of("poison-recovery"), 1000);
    run.beginScenario("poison-recovery");
    var original = new ConsoleKafka.Record("execution-reports", "0", "9", "12", "{invalid");
    run.step(
        "Publicar original",
        "Se conserva el texto",
        () -> {
          run.message(original);
          return Map.of();
        });

    var result = (Map<?, ?>) ((List<?>) run.snapshot().get("scenarioResults")).getFirst();
    var evidence = (Map<?, ?>) result.get("evidence");
    var message = (Map<?, ?>) ((List<?>) evidence.get("messages")).getFirst();
    assertThat(message.get("payload")).isEqualTo("{invalid");
    assertThat(message.get("report")).isEqualTo(Map.of());
    assertThat(message.get("sqlObservationStatus")).isEqualTo("NOT_QUERIED");
  }

  @Test
  void keepsSqlObservationHistoryAndPartialEvidenceAfterFailure() {
    var run = new DemoRun("run-3", "automatic", List.of("terminal-duplicates"), 1000);
    run.beginScenario("terminal-duplicates");
    var record = new ConsoleKafka.Record("execution-reports", "0", "10", "22", "{}");
    run.step(
        "Publicar",
        "Se conserva el envío",
        () -> {
          run.message(record);
          return Map.of();
        });

    var initialRun = run.snapshot();
    var initialScenario = (Map<?, ?>) ((List<?>) initialRun.get("scenarioResults")).getFirst();
    var initialEvidence = (Map<?, ?>) initialScenario.get("evidence");
    var initial = (Map<?, ?>) ((List<?>) initialEvidence.get("messages")).getFirst();
    assertThat(initial.get("sqlObservations")).asList().isEmpty();
    assertThat(initial.get("sqlObservationStatus")).isEqualTo("NOT_QUERIED");

    run.sqlObservation(record, "NO_RESULT", java.util.Collections.singletonMap("result", null));
    run.sqlObservation(
        record, "RESULT_FOUND", Map.of("result", Map.of("outcome", "DUPLICATE", "ledgerId", "3")));
    run.sqlObservation(record, "QUERY_FAILED", Map.of());
    run.finish("FAILED", "fallo posterior");

    var result = (Map<?, ?>) ((List<?>) run.snapshot().get("scenarioResults")).getFirst();
    var evidence = (Map<?, ?>) result.get("evidence");
    var message = (Map<?, ?>) ((List<?>) evidence.get("messages")).getFirst();
    var observations = (List<?>) message.get("sqlObservations");
    assertThat(observations).hasSize(3);
    assertThat(((Map<?, ?>) observations.get(0)).get("status")).isEqualTo("NO_RESULT");
    assertThat(((Map<?, ?>) observations.get(1)).get("status")).isEqualTo("RESULT_FOUND");
    assertThat(((Map<?, ?>) observations.get(2)).get("status")).isEqualTo("QUERY_FAILED");
    assertThat(message.get("sqlObservationStatus")).isEqualTo("QUERY_FAILED");
    assertThat(evidence.get("messages")).asList().hasSize(1);
  }

  @Test
  void keepsAbortedProducerEvidenceAndSeparatesRuns() {
    var first = new DemoRun("run-a", "automatic", List.of("committed-input-only"), 1000);
    var second = new DemoRun("run-b", "automatic", List.of("committed-input-only"), 1000);
    first.beginScenario("committed-input-only");
    second.beginScenario("committed-input-only");
    var record = new ConsoleKafka.Record("execution-reports", "0", "11", "33", "{}");
    first.step(
        "Abortar",
        "Conservar el intento",
        () -> {
          first.message(record, "ABORTED");
          return Map.of();
        });
    first.finish("CANCELLED", "cancelado después del envío");

    var firstResult = (Map<?, ?>) ((List<?>) first.snapshot().get("scenarioResults")).getFirst();
    var secondResult = (Map<?, ?>) ((List<?>) second.snapshot().get("scenarioResults")).getFirst();
    var firstEvidence = (Map<?, ?>) firstResult.get("evidence");
    var secondEvidence = (Map<?, ?>) secondResult.get("evidence");
    var aborted = (Map<?, ?>) ((List<?>) firstEvidence.get("messages")).getFirst();
    assertThat(aborted.get("transaction")).isEqualTo("ABORTED");
    assertThat(aborted.get("sqlObservationStatus")).isEqualTo("NOT_QUERIED");
    assertThat(secondEvidence.get("messages")).asList().isEmpty();
  }
}
