package com.maxcapital.challenge.application;

import com.maxcapital.challenge.domain.ExecutionReport;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Service
@Profile("!evaluation-console")
public class HandleExecutionReport {
  private final ProcessExecutionReport processor;
  private final SettlementPublisher publisher;

  public HandleExecutionReport(ProcessExecutionReport processor, SettlementPublisher publisher) {
    this.processor = processor;
    this.publisher = publisher;
  }

  public ProcessingResult handle(InputPosition position, ExecutionReport report, String payload) {
    return handle(position, report, payload, null);
  }

  public ProcessingResult handle(
      InputPosition position, ExecutionReport report, String payload, String correctionId) {
    // El proxy JDBC confirma antes de retornar. Publicar no puede adelantar ese commit.
    ProcessingResult result;
    try {
      result = processor.process(position, report, payload, correctionId);
    } catch (com.maxcapital.challenge.domain.InvalidReport invalid) {
      if (correctionId != null) throw new CorrectionFailure(correctionId, invalid);
      throw invalid;
    }
    Settlement.from(result).ifPresent(publisher::publish);
    return result;
  }
}
