package com.maxcapital.challenge.application;

public record InputPosition(String topic, int partition, long offset) {
  public InputPosition {
    if (topic == null || topic.isBlank() || partition < 0 || offset < 0) {
      throw new IllegalArgumentException("posición de entrada inválida");
    }
  }
}
