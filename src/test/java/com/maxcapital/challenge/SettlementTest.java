package com.maxcapital.challenge;

import static org.assertj.core.api.Assertions.assertThat;

import com.maxcapital.challenge.adapter.out.kafka.SettlementJson;
import com.maxcapital.challenge.application.InputPosition;
import com.maxcapital.challenge.application.ProcessingResult;
import com.maxcapital.challenge.application.Settlement;
import com.maxcapital.challenge.domain.OrderState;
import com.maxcapital.challenge.domain.OrderStatus;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class SettlementTest {
  @Test
  void selectsOriginalCompletionAndExcludesLateDuplicatesAndOtherStates() {
    for (var status : OrderStatus.values()) {
      for (var outcome : ProcessingResult.Outcome.values()) {
        var result = result(status, outcome, new BigDecimal("10"));
        assertThat(Settlement.from(result).isPresent())
            .isEqualTo(status == OrderStatus.FILLED && outcome == ProcessingResult.Outcome.APPLIED);
      }
    }
  }

  @Test
  void preservesExactIdentifiersAndCanonicalDecimalsAcrossSqlScale() {
    var first =
        Settlement.from(
                result(
                    OrderStatus.FILLED,
                    ProcessingResult.Outcome.APPLIED,
                    new BigDecimal("999999999999.12345678")))
            .orElseThrow();
    var restored =
        new Settlement(
            first.numericOrderId(),
            first.fixId(),
            first.ledgerId(),
            first.source(),
            first.nominalAmounts().setScale(8),
            first.accumulativeNominalAmount().setScale(8),
            BigDecimal.ZERO.setScale(8));
    assertThat(SettlementJson.encode(restored)).isEqualTo(SettlementJson.encode(first));
    assertThat(new String(SettlementJson.encode(first), StandardCharsets.UTF_8))
        .isEqualTo(
            """
        {"schemaVersion":1,"settlementId":"settlement:9007199254740993","numericOrderId":"9007199254740993","fixId":"9007199254740995","ledgerId":"9007199254740997","source":{"topic":"execution-reports","partition":1,"offset":"9007199254740999"},"nominalAmounts":"999999999999.12345678","accumulativeNominalAmount":"999999999999.12345678","leavesNominalAmount":"0"}\
        """);
  }

  private static ProcessingResult result(
      OrderStatus status, ProcessingResult.Outcome outcome, BigDecimal total) {
    return new ProcessingResult(
        new InputPosition("execution-reports", 1, 9007199254740999L),
        outcome,
        9007199254740993L,
        9007199254740995L,
        9007199254740997L,
        new OrderState(9007199254740993L, status, total, BigDecimal.ZERO, total, 3));
  }
}
