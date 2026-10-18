package com.maxcapital.challenge.domain;

import java.math.BigDecimal;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

public record ExecutionReport(
    long fixId,
    long numericOrderId,
    OrderStatus status,
    BigDecimal nominalAmounts,
    BigDecimal leavesNominalAmount,
    BigDecimal accumulativeNominalAmount,
    String transactionTime) {
  public ExecutionReport {
    if (fixId <= 0 || numericOrderId <= 0) {
      throw new IllegalArgumentException("fixId y numericOrderId deben ser positivos");
    }
    Objects.requireNonNull(status, "status obligatorio");
    validateQuantity(nominalAmounts);
    validateQuantity(leavesNominalAmount);
    validateQuantity(accumulativeNominalAmount);
    if (nominalAmounts.signum() == 0) {
      throw new IllegalArgumentException("nominalAmounts debe ser positivo");
    }
    DateTimeFormatter.ISO_DATE_TIME.parse(Objects.requireNonNull(transactionTime));
  }

  private static void validateQuantity(BigDecimal value) {
    Objects.requireNonNull(value, "cantidad obligatoria");
    var normalized = value.stripTrailingZeros();
    if (value.signum() < 0
        || normalized.scale() > 8
        || normalized.precision() - normalized.scale() > 12) {
      throw new IllegalArgumentException("cantidad fuera de rango: hasta 12 enteros y 8 decimales");
    }
  }
}
