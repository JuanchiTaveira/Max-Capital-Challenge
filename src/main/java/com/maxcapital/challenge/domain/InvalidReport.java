package com.maxcapital.challenge.domain;

/** Error de datos reconocido; no clasifica fallas inesperadas del programa. */
public final class InvalidReport extends IllegalArgumentException {
  private final String code;

  public InvalidReport(String code, String message) {
    super(message);
    this.code = code;
  }

  public String code() {
    return code;
  }
}
