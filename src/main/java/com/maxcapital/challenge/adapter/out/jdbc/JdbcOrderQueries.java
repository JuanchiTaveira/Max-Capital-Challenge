package com.maxcapital.challenge.adapter.out.jdbc;

import com.maxcapital.challenge.application.OrderQueries;
import com.maxcapital.challenge.domain.OrderState;
import com.maxcapital.challenge.domain.OrderStatus;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class JdbcOrderQueries implements OrderQueries {
  private final JdbcTemplate jdbc;

  public JdbcOrderQueries(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  @Override
  public Optional<OrderDetails> find(long numericOrderId) {
    // Una sola sentencia obtiene estado y ledger desde el mismo snapshot MVCC.
    return jdbc.query(
        """
        SELECT o.*, l.ledger_id, l.report::text AS report
        FROM app.orders o LEFT JOIN app.execution_report_ledger l
          ON l.numeric_order_id = o.numeric_order_id
        WHERE o.numeric_order_id = ? ORDER BY l.ledger_id
        """,
        rows -> {
          if (!rows.next()) return Optional.empty();
          var state =
              new OrderState(
                  rows.getLong("numeric_order_id"),
                  OrderStatus.valueOf(rows.getString("status")),
                  rows.getBigDecimal("nominal_amounts"),
                  rows.getBigDecimal("leaves_nominal_amount"),
                  rows.getBigDecimal("accumulative_nominal_amount"),
                  rows.getLong("applied_execution_count"));
          var ledger = new ArrayList<LedgerEntry>();
          do {
            long id = rows.getLong("ledger_id");
            if (!rows.wasNull()) ledger.add(new LedgerEntry(id, rows.getString("report")));
          } while (rows.next());
          return Optional.of(new OrderDetails(state, List.copyOf(ledger)));
        },
        numericOrderId);
  }
}
