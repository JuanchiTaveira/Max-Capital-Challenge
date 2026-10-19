package com.maxcapital.challenge.application;

import com.maxcapital.challenge.domain.ExecutionReport;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ProcessExecutionReport {
  private final OrderRepository repository;

  public ProcessExecutionReport(OrderRepository repository) {
    this.repository = repository;
  }

  @Transactional("orderTransactionManager")
  public void process(ExecutionReport report, String payload) {
    repository.initialize(report);
    var previous = repository.lock(report.numericOrderId());
    var match = repository.match(report.numericOrderId(), report.fixId(), payload);
    if (match.isPresent()) {
      if (!match.get().identical()) {
        throw new IllegalArgumentException("contenido distinto para fixId " + report.fixId());
      }
      return;
    }
    repository.save(previous.apply(report));
    repository.append(report, payload);
  }
}
