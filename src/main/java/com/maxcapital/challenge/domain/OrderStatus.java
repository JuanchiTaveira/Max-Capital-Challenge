package com.maxcapital.challenge.domain;

public enum OrderStatus {
  NEW,
  PARTIALLY_FILLED,
  FILLED,
  CANCELLED;

  public boolean isTerminal() {
    return this == FILLED || this == CANCELLED;
  }
}
