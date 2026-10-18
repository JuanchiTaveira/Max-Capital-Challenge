package com.maxcapital.challenge.adapter.in.http;

import com.maxcapital.challenge.application.OrderQueries;
import com.maxcapital.challenge.domain.OrderStatus;
import java.math.BigDecimal;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@RestController
public class OrderController {
  private final OrderQueries queries;
  private final JsonMapper json = new JsonMapper();

  public OrderController(OrderQueries queries) {
    this.queries = queries;
  }

  @GetMapping("/orders/{numericOrderId}")
  public OrderResponse find(@PathVariable long numericOrderId) {
    if (numericOrderId <= 0) throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
    var details =
        queries
            .find(numericOrderId)
            .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
    var state = details.state();
    return new OrderResponse(
        state.numericOrderId(),
        state.status(),
        state.appliedExecutionCount(),
        state.nominalAmounts(),
        state.accumulativeNominalAmount(),
        state.leavesNominalAmount(),
        details.ledger().stream()
            .map(entry -> new LedgerResponse(entry.ledgerId(), json.readTree(entry.report())))
            .toList());
  }

  public record LedgerResponse(long ledgerId, JsonNode report) {}

  public record OrderResponse(
      long numericOrderId,
      OrderStatus status,
      long appliedExecutionCount,
      BigDecimal nominalAmounts,
      BigDecimal accumulativeNominalAmount,
      BigDecimal leavesNominalAmount,
      List<LedgerResponse> ledger) {}
}
