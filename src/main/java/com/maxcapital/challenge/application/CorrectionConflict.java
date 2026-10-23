package com.maxcapital.challenge.application;

public final class CorrectionConflict extends IllegalStateException {
  public CorrectionConflict(String message) {
    super(message);
  }
}
