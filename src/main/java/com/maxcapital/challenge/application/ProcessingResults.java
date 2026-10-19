package com.maxcapital.challenge.application;

import java.util.Optional;

public interface ProcessingResults {
  Optional<StoredResult> find(InputPosition position, String payload);

  void insert(ProcessingResult result);

  record StoredResult(ProcessingResult result, boolean identicalReport) {}
}
