package com.maxcapital.challenge.application;

import com.maxcapital.challenge.domain.ExecutionReport;
import com.maxcapital.challenge.domain.OrderState;

public interface OrderRepository {
  void initialize(ExecutionReport report);

  OrderState lock(long numericOrderId);

  ReportMatch match(long numericOrderId, long fixId, String payload);

  void save(OrderState state);

  void append(ExecutionReport report, String payload);

  enum ReportMatch {
    ABSENT,
    IDENTICAL,
    CONFLICTING
  }
}
