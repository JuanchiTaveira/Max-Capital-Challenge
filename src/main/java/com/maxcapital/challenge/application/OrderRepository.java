package com.maxcapital.challenge.application;

import com.maxcapital.challenge.domain.ExecutionReport;
import com.maxcapital.challenge.domain.OrderState;
import java.util.Optional;

public interface OrderRepository {
  void initialize(ExecutionReport report);

  OrderState lock(long numericOrderId);

  Optional<ReportMatch> match(long numericOrderId, long fixId, String payload);

  void save(OrderState state);

  long append(ExecutionReport report, String payload);

  record ReportMatch(long ledgerId, boolean identical) {}
}
