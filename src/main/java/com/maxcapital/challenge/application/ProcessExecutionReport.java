package com.maxcapital.challenge.application;

import com.maxcapital.challenge.application.ProcessingResult.Outcome;
import com.maxcapital.challenge.domain.ExecutionReport;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ProcessExecutionReport {
  private final OrderRepository repository;
  private final ProcessingResults results;

  public ProcessExecutionReport(OrderRepository repository, ProcessingResults results) {
    this.repository = repository;
    this.results = results;
  }

  @Transactional("orderTransactionManager")
  public ProcessingResult process(InputPosition position, ExecutionReport report, String payload) {
    var stored = results.find(position, payload);
    if (stored.isPresent()) {
      return recover(report, stored.get());
    }
    repository.initialize(report);
    var previous = repository.lock(report.numericOrderId());
    // Otra transacción pudo confirmar esta posición mientras esperábamos el lock.
    stored = results.find(position, payload);
    if (stored.isPresent()) {
      return recover(report, stored.get());
    }
    var match = repository.match(report.numericOrderId(), report.fixId(), payload);
    ProcessingResult result;
    if (match.isPresent()) {
      if (!match.get().identical()) {
        throw new IllegalArgumentException("contenido distinto para fixId " + report.fixId());
      }
      result =
          new ProcessingResult(
              position,
              Outcome.DUPLICATE,
              report.numericOrderId(),
              report.fixId(),
              match.get().ledgerId(),
              previous);
    } else {
      var next = previous.apply(report);
      repository.save(next);
      long ledgerId = repository.append(report, payload);
      result =
          new ProcessingResult(
              position, Outcome.APPLIED, report.numericOrderId(), report.fixId(), ledgerId, next);
    }
    results.insert(result);
    return result;
  }

  private static ProcessingResult recover(
      ExecutionReport report, ProcessingResults.StoredResult stored) {
    var result = stored.result();
    if (!stored.identicalReport()
        || result.numericOrderId() != report.numericOrderId()
        || result.fixId() != report.fixId()) {
      throw new IllegalArgumentException("contenido distinto para posición " + result.position());
    }
    return result;
  }
}
