package com.maxcapital.challenge.demo;

import com.maxcapital.challenge.adapter.in.report.ExecutionReportParser;
import com.maxcapital.challenge.application.OrderQueries;
import com.maxcapital.challenge.domain.OrderState;
import java.nio.charset.StandardCharsets;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

@Service
@Profile("evaluation-console")
public class ReportPreflight {
  private final ExecutionReportParser parser;
  private final OrderQueries orders;
  private final JdbcTemplate jdbc;

  public ReportPreflight(ExecutionReportParser parser, OrderQueries orders, JdbcTemplate jdbc) {
    this.parser = parser;
    this.orders = orders;
    this.jdbc = jdbc;
  }

  @Transactional(
      value = "orderTransactionManager",
      readOnly = true,
      isolation = Isolation.REPEATABLE_READ)
  public void validate(String key, String payload) {
    if (key == null || payload == null)
      throw new IllegalArgumentException("Key y JSON son obligatorios");
    ExecutionReportParser.ParsedReport parsed;
    try {
      parsed = parser.parse(key, payload.getBytes(StandardCharsets.UTF_8));
    } catch (RuntimeException ex) {
      throw new IllegalArgumentException("ER inválido: revisá JSON, key, campos y cantidades", ex);
    }
    var report = parsed.report();
    var matches =
        jdbc.queryForList(
            """
        SELECT report = CAST(? AS jsonb) AS identical FROM app.execution_report_ledger
        WHERE numeric_order_id=? AND fix_id=?
        """,
            Boolean.class,
            parsed.payload(),
            report.numericOrderId(),
            report.fixId());
    if (!matches.isEmpty()) {
      if (!matches.getFirst())
        throw new IllegalArgumentException("La identidad del ER ya existe con otro contenido");
      return;
    }
    orders
        .find(report.numericOrderId())
        .map(OrderQueries.OrderDetails::state)
        .orElseGet(() -> OrderState.initial(report))
        .apply(report);
  }
}
