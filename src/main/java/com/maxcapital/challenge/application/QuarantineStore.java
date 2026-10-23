package com.maxcapital.challenge.application;

import java.util.List;
import java.util.Optional;

public interface QuarantineStore {
  record Correction(
      String requestId,
      InputPosition source,
      String effectiveKey,
      String effectiveReport,
      String reason,
      String declaredOperator,
      String outcome,
      String diagnosticCode,
      String createdAt) {}

  record Entry(
      InputPosition source,
      byte[] originalKey,
      byte[] originalValue,
      String causeCode,
      String state,
      boolean notified,
      Correction correction,
      String createdAt) {}

  Optional<Entry> find(InputPosition source, boolean lock);

  Entry capture(InputPosition source, byte[] key, byte[] value, String code);

  void notified(InputPosition source);

  Correction accept(Correction request, int expectedPartition);

  void applied(InputPosition source, String outcome);

  void rejectCorrection(InputPosition source, String requestId, String code);

  List<Correction> corrections(InputPosition source);
}
