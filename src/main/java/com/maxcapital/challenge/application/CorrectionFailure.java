package com.maxcapital.challenge.application;

import com.maxcapital.challenge.domain.InvalidReport;

/** Identifica la corrección cuyo intento de negocio revirtió. */
public final class CorrectionFailure extends RuntimeException {
  private final String requestId;

  public CorrectionFailure(String requestId, InvalidReport cause) {
    super(cause);
    this.requestId = requestId;
  }

  public String requestId() {
    return requestId;
  }
}
