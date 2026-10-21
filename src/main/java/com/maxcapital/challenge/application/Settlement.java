package com.maxcapital.challenge.application;

import com.maxcapital.challenge.domain.OrderStatus;
import java.math.BigDecimal;
import java.util.Optional;

public record Settlement(
    long numericOrderId,
    long fixId,
    long ledgerId,
    InputPosition source,
    BigDecimal nominalAmounts,
    BigDecimal accumulativeNominalAmount,
    BigDecimal leavesNominalAmount) {
  public static Optional<Settlement> from(ProcessingResult result) {
    if (result.outcome() != ProcessingResult.Outcome.APPLIED
        || result.state().status() != OrderStatus.FILLED) {
      return Optional.empty();
    }
    var state = result.state();
    return Optional.of(
        new Settlement(
            result.numericOrderId(),
            result.fixId(),
            result.ledgerId(),
            result.position(),
            state.nominalAmounts(),
            state.accumulativeNominalAmount(),
            state.leavesNominalAmount()));
  }
}
