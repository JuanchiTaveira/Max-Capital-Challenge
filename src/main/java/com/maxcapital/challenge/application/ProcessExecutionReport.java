package com.maxcapital.challenge.application;

import com.maxcapital.challenge.domain.ExecutionReport;
import org.springframework.transaction.annotation.Transactional;

public class ProcessExecutionReport {
  private final OrderRepository repository;

  public ProcessExecutionReport(OrderRepository repository) {
    this.repository = repository;
  }

  @Transactional
  public void process(ExecutionReport report, String payload) {
    repository.initialize(report);
    var previous = repository.lock(report.numericOrderId());
    var match = repository.match(report.numericOrderId(), report.fixId(), payload);
    if (match == OrderRepository.ReportMatch.IDENTICAL) {
      return;
    }
    if (match == OrderRepository.ReportMatch.CONFLICTING) {
      throw new IllegalArgumentException("contenido distinto para fixId " + report.fixId());
    }
    repository.save(previous.apply(report));
    repository.append(report, payload);
  }
}
