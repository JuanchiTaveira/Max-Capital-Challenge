package com.maxcapital.challenge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.*;

import com.maxcapital.challenge.adapter.in.report.ExecutionReportParser;
import com.maxcapital.challenge.application.InputPosition;
import com.maxcapital.challenge.application.OrderRepository;
import com.maxcapital.challenge.application.ProcessExecutionReport;
import com.maxcapital.challenge.application.ProcessingResults;
import com.maxcapital.challenge.domain.ExecutionReport;
import com.maxcapital.challenge.domain.OrderState;
import com.maxcapital.challenge.domain.OrderStatus;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class OrderDomainTest {
  private final ExecutionReportParser parser = new ExecutionReportParser();

  @Test
  void detectsDuplicateBeforeEvaluatingTerminalTransition() {
    // GIVEN
    var repository = mock(OrderRepository.class);
    when(repository.lock(2)).thenReturn(stateAfterFill());
    when(repository.match(2, 1, "payload"))
        .thenReturn(Optional.of(new OrderRepository.ReportMatch(1, true)));
    // WHEN
    new ProcessExecutionReport(
            repository,
            mock(ProcessingResults.class),
            mock(com.maxcapital.challenge.application.QuarantineStore.class))
        .process(
            new InputPosition("unit-reports", 0, 0), report(OrderStatus.FILLED, 10, 0), "payload");
    // THEN
    verify(repository, never()).save(any());
    verify(repository, never()).append(any(), any());
  }

  @Test
  void parsesDocumentedPayloadWithoutLosingFieldsOrDecimalPrecision() throws Exception {
    // GIVEN
    var payload = Files.readAllBytes(Path.of("examples/execution-report.json"));

    // WHEN
    var parsed = parser.parse("13144742", payload);

    // THEN
    assertThat(parsed.report().fixId()).isEqualTo(523130930000307L);
    assertThat(parsed.report().transactionTime()).isEqualTo("2026-07-20T18:08:52.129");
    assertThat(parsed.payload()).contains("secondaryTradeId", "executionPrice", "marketOrderId");
    // GIVEN
    var decimal =
        json().replace("\"nominalAmounts\":10", "\"nominalAmounts\":999999999999.12345678");

    // WHEN
    var decimalReport = parse(decimal);

    // THEN
    assertThat(decimalReport.nominalAmounts()).isEqualByComparingTo("999999999999.12345678");
  }

  @Test
  void rejectsAmbiguousOrInvalidEnvelopes() {
    // GIVEN
    var payload = json().getBytes(StandardCharsets.UTF_8);

    // WHEN
    var invalidKey = catchThrowable(() -> parser.parse("02", payload));

    // THEN
    assertThat(invalidKey).isInstanceOf(IllegalArgumentException.class);

    // GIVEN
    for (var invalid :
        new String[] {
          json().replace("\"fixId\":1,", ""),
          json().replace("\"fixId\":1", "\"fixId\":9223372036854775808"),
          json().replace("\"nominalAmounts\":10", "\"nominalAmounts\":0.000000001"),
          json().replace("\"nominalAmounts\":10", "\"nominalAmounts\":1000000000000"),
          json().replace("NEW", "UNKNOWN"),
          json().replace("2026-07-20T18:08:52.129", "yesterday"),
          json().replace("\"fixId\":1", "\"fixId\":1,\"fixId\":2"),
          json() + " {}"
        }) {
      // WHEN
      var failure = catchThrowable(() -> parse(invalid));

      // THEN
      assertThat(failure).isInstanceOf(RuntimeException.class);
    }
  }

  @Test
  void computesStateFromPreviousStateAndSnapshots() {
    // GIVEN
    var first = report(OrderStatus.NEW, 0, 10);

    // WHEN
    var state = OrderState.initial(first).apply(first);
    state = state.apply(report(OrderStatus.PARTIALLY_FILLED, 3, 7));
    state = state.apply(report(OrderStatus.PARTIALLY_FILLED, 7, 3));
    // THEN
    assertThat(state.accumulativeNominalAmount()).isEqualByComparingTo("7");

    // WHEN
    state = state.apply(report(OrderStatus.FILLED, 10, 0));

    // THEN
    assertThat(state.appliedExecutionCount()).isEqualTo(4);

    // WHEN
    var failure = catchThrowable(() -> stateAfterFill().apply(report(OrderStatus.FILLED, 10, 0)));

    // THEN
    assertThat(failure).hasMessageContaining("transición inválida");
  }

  @Test
  void supportsCancellationAndRejectsMissingNewAndContradictorySnapshots() {
    // GIVEN
    var first = report(OrderStatus.NEW, 0, 10);
    var state = OrderState.initial(first).apply(first);
    // WHEN
    var cancelled = state.apply(report(OrderStatus.CANCELLED, 0, 0));

    // THEN
    assertThat(cancelled.status()).isEqualTo(OrderStatus.CANCELLED);

    // GIVEN
    for (var invalid :
        new ExecutionReport[] {
          report(OrderStatus.FILLED, 9, 0),
          report(OrderStatus.PARTIALLY_FILLED, 3, 8),
          report(OrderStatus.NEW, 0, 10)
        }) {
      // WHEN
      var failure = catchThrowable(() -> state.apply(invalid));

      // THEN
      assertThat(failure).isInstanceOf(IllegalArgumentException.class);
    }

    // WHEN
    var missingNew =
        catchThrowable(() -> OrderState.initial(first).apply(report(OrderStatus.FILLED, 10, 0)));

    // THEN
    assertThat(missingNew).isInstanceOf(IllegalArgumentException.class);

    // GIVEN
    var partial = state.apply(report(OrderStatus.PARTIALLY_FILLED, 7, 3));

    // WHEN
    var decreasingAccumulated =
        catchThrowable(() -> partial.apply(report(OrderStatus.CANCELLED, 6, 0)));

    // THEN
    assertThat(decreasingAccumulated).isInstanceOf(IllegalArgumentException.class);
  }

  private static OrderState stateAfterFill() {
    var first = report(OrderStatus.NEW, 0, 10);
    return OrderState.initial(first).apply(first).apply(report(OrderStatus.FILLED, 10, 0));
  }

  private static ExecutionReport report(OrderStatus status, int accumulated, int leaves) {
    return new ExecutionReport(
        1,
        2,
        status,
        BigDecimal.TEN,
        BigDecimal.valueOf(leaves),
        BigDecimal.valueOf(accumulated),
        "2026-07-20T18:08:52.129");
  }

  private ExecutionReport parse(String text) {
    return parser.parse("2", text.getBytes(StandardCharsets.UTF_8)).report();
  }

  private static String json() {
    return """
        {"fixId":1,"numericOrderId":2,"status":"NEW","nominalAmounts":10,
        "leavesNominalAmount":10,"accumulativeNominalAmount":0,
        "transactionTime":"2026-07-20T18:08:52.129"}
        """;
  }
}
