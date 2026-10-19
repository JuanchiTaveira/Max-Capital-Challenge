package com.maxcapital.challenge.adapter.out.jdbc;

import com.maxcapital.challenge.application.InputPosition;
import com.maxcapital.challenge.application.ProcessingResult;
import com.maxcapital.challenge.application.ProcessingResult.Outcome;
import com.maxcapital.challenge.application.ProcessingResults;
import com.maxcapital.challenge.domain.OrderState;
import com.maxcapital.challenge.domain.OrderStatus;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class JdbcProcessingResults implements ProcessingResults {
  private final JdbcTemplate jdbc;

  public JdbcProcessingResults(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  @Override
  public Optional<StoredResult> find(InputPosition position, String payload) {
    var found =
        jdbc.query(
            """
        SELECT r.*, l.report = CAST(? AS jsonb) AS identical_report
        FROM app.processing_results r JOIN app.execution_report_ledger l
          ON l.ledger_id = r.ledger_id
        WHERE r.topic = ? AND r.partition_id = ? AND r.record_offset = ?
        """,
            (row, index) ->
                new StoredResult(
                    new ProcessingResult(
                        position,
                        Outcome.valueOf(row.getString("outcome")),
                        row.getLong("numeric_order_id"),
                        row.getLong("fix_id"),
                        row.getLong("ledger_id"),
                        new OrderState(
                            row.getLong("numeric_order_id"),
                            OrderStatus.valueOf(row.getString("status")),
                            row.getBigDecimal("nominal_amounts"),
                            row.getBigDecimal("leaves_nominal_amount"),
                            row.getBigDecimal("accumulative_nominal_amount"),
                            row.getLong("applied_execution_count"))),
                    row.getBoolean("identical_report")),
            payload,
            position.topic(),
            position.partition(),
            position.offset());
    return found.stream().findFirst();
  }

  @Override
  public void insert(ProcessingResult result) {
    var position = result.position();
    var state = result.state();
    jdbc.update(
        """
        INSERT INTO app.processing_results (topic, partition_id, record_offset, outcome,
          numeric_order_id, fix_id, ledger_id, status, nominal_amounts, leaves_nominal_amount,
          accumulative_nominal_amount, applied_execution_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        position.topic(),
        position.partition(),
        position.offset(),
        result.outcome().name(),
        result.numericOrderId(),
        result.fixId(),
        result.ledgerId(),
        state.status().name(),
        state.nominalAmounts(),
        state.leavesNominalAmount(),
        state.accumulativeNominalAmount(),
        state.appliedExecutionCount());
  }
}
