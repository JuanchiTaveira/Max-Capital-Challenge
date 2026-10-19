package com.maxcapital.challenge.adapter.out.jdbc;

import com.maxcapital.challenge.application.OrderRepository;
import com.maxcapital.challenge.domain.ExecutionReport;
import com.maxcapital.challenge.domain.OrderState;
import com.maxcapital.challenge.domain.OrderStatus;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class JdbcOrderRepository implements OrderRepository {
  private final JdbcTemplate jdbc;

  public JdbcOrderRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  @Override
  public void initialize(ExecutionReport report) {
    jdbc.update(
        """
        INSERT INTO app.orders (numeric_order_id, status, nominal_amounts,
            leaves_nominal_amount, accumulative_nominal_amount, applied_execution_count)
        VALUES (?, 'NEW', ?, ?, 0, 0) ON CONFLICT (numeric_order_id) DO NOTHING
        """,
        report.numericOrderId(),
        report.nominalAmounts(),
        report.nominalAmounts());
  }

  @Override
  public OrderState lock(long numericOrderId) {
    return jdbc.queryForObject(
        "SELECT * FROM app.orders WHERE numeric_order_id = ? FOR UPDATE",
        (row, index) ->
            new OrderState(
                row.getLong("numeric_order_id"),
                OrderStatus.valueOf(row.getString("status")),
                row.getBigDecimal("nominal_amounts"),
                row.getBigDecimal("leaves_nominal_amount"),
                row.getBigDecimal("accumulative_nominal_amount"),
                row.getLong("applied_execution_count")),
        numericOrderId);
  }

  @Override
  public Optional<ReportMatch> match(long numericOrderId, long fixId, String payload) {
    var matches =
        jdbc.query(
            """
        SELECT ledger_id, report = CAST(? AS jsonb) AS identical FROM app.execution_report_ledger
        WHERE numeric_order_id = ? AND fix_id = ?
        """,
            (row, index) -> new ReportMatch(row.getLong("ledger_id"), row.getBoolean("identical")),
            payload,
            numericOrderId,
            fixId);
    return matches.stream().findFirst();
  }

  @Override
  public void save(OrderState state) {
    jdbc.update(
        """
        UPDATE app.orders SET status = ?, leaves_nominal_amount = ?,
            accumulative_nominal_amount = ?, applied_execution_count = ?
        WHERE numeric_order_id = ?
        """,
        state.status().name(),
        state.leavesNominalAmount(),
        state.accumulativeNominalAmount(),
        state.appliedExecutionCount(),
        state.numericOrderId());
  }

  @Override
  public long append(ExecutionReport report, String payload) {
    return jdbc.queryForObject(
        """
        INSERT INTO app.execution_report_ledger (numeric_order_id, fix_id, report)
        VALUES (?, ?, CAST(? AS jsonb)) RETURNING ledger_id
        """,
        Long.class,
        report.numericOrderId(),
        report.fixId(),
        payload);
  }
}
