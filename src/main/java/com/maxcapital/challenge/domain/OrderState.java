package com.maxcapital.challenge.domain;

import java.math.BigDecimal;

public record OrderState(
    long numericOrderId,
    OrderStatus status,
    BigDecimal nominalAmounts,
    BigDecimal leavesNominalAmount,
    BigDecimal accumulativeNominalAmount,
    long appliedExecutionCount) {

  public static OrderState initial(ExecutionReport report) {
    return new OrderState(
        report.numericOrderId(),
        OrderStatus.NEW,
        report.nominalAmounts(),
        report.nominalAmounts(),
        BigDecimal.ZERO,
        0);
  }

  public OrderState apply(ExecutionReport report) {
    if (numericOrderId != report.numericOrderId()
        || nominalAmounts.compareTo(report.nominalAmounts()) != 0) {
      throw new IllegalArgumentException("el identificador y nominalAmounts no pueden cambiar");
    }
    if ((appliedExecutionCount == 0 && report.status() != OrderStatus.NEW)
        || (appliedExecutionCount > 0
            && (status.isTerminal() || report.status() == OrderStatus.NEW))) {
      throw new IllegalArgumentException(
          "transición inválida desde " + status + " a " + report.status());
    }
    var accumulated = report.accumulativeNominalAmount();
    var leaves = report.leavesNominalAmount();
    boolean valid =
        accumulated.compareTo(accumulativeNominalAmount) >= 0
            && accumulated.compareTo(nominalAmounts) <= 0;
    valid &=
        switch (report.status()) {
          case NEW -> accumulated.signum() == 0 && leaves.compareTo(nominalAmounts) == 0;
          case PARTIALLY_FILLED ->
              accumulated.compareTo(accumulativeNominalAmount) > 0
                  && accumulated.compareTo(nominalAmounts) < 0
                  && leaves.compareTo(nominalAmounts.subtract(accumulated)) == 0;
          case FILLED -> accumulated.compareTo(nominalAmounts) == 0 && leaves.signum() == 0;
          case CANCELLED -> leaves.signum() == 0;
        };
    if (!valid) {
      throw new IllegalArgumentException(
          "snapshot de cantidades incompatible con " + report.status());
    }
    return new OrderState(
        numericOrderId,
        report.status(),
        nominalAmounts,
        leaves,
        accumulated,
        Math.incrementExact(appliedExecutionCount));
  }
}
