package com.maxcapital.challenge.application;

import com.maxcapital.challenge.domain.OrderState;

public record ProcessingResult(
    InputPosition position,
    Outcome outcome,
    long numericOrderId,
    long fixId,
    long ledgerId,
    OrderState state) {
  public enum Outcome {
    APPLIED,
    DUPLICATE
  }
}
