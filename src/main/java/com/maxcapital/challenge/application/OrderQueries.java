package com.maxcapital.challenge.application;

import com.maxcapital.challenge.domain.OrderState;
import java.util.List;
import java.util.Optional;

public interface OrderQueries {
  Optional<OrderDetails> find(long numericOrderId);

  record LedgerEntry(long ledgerId, String report) {}

  record OrderDetails(OrderState state, List<LedgerEntry> ledger) {}
}
