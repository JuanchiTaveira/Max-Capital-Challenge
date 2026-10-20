package com.maxcapital.challenge.demo;

import com.maxcapital.challenge.application.OrderQueries;
import java.util.ArrayList;
import java.util.Map;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
@Profile("evaluation-console")
public class ConsoleQueries {
  private final JdbcTemplate jdbc;
  private final OrderQueries orders;

  public ConsoleQueries(JdbcTemplate jdbc, OrderQueries orders) {
    this.jdbc = jdbc;
    this.orders = orders;
  }

  public Map<String, Object> list(String cursor, int limit) {
    if (limit < 1 || limit > 200)
      throw new IllegalArgumentException("El límite debe estar entre 1 y 200");
    var params = new ArrayList<Object>();
    String where = "";
    if (cursor != null && !cursor.isBlank()) {
      where = " WHERE numeric_order_id < ?";
      params.add(ConsoleValues.positive(cursor));
    }
    params.add(limit + 1);
    var rows =
        jdbc.queryForList(
            "SELECT * FROM app.orders" + where + " ORDER BY numeric_order_id DESC LIMIT ?",
            params.toArray());
    boolean more = rows.size() > limit;
    if (more) rows = rows.subList(0, limit);
    var result = ConsoleValues.observed("PostgreSQL");
    result.put("orders", rows.stream().map(ConsoleValues::sqlRow).toList());
    result.put("nextCursor", more ? rows.getLast().get("numeric_order_id").toString() : null);
    return result;
  }

  public Map<String, Object> order(String id) {
    var details =
        orders
            .find(ConsoleValues.positive(id))
            .orElseThrow(
                () -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Orden no encontrada"));
    var result = ConsoleValues.observed("PostgreSQL");
    result.putAll(ConsoleValues.order(details));
    return result;
  }

  public Map<String, Object> result(String topic, int partition, String offset) {
    if (topic == null || topic.isBlank() || partition < 0)
      throw new IllegalArgumentException("Posición Kafka inválida");
    var rows =
        jdbc.queryForList(
            """
        SELECT r.*, l.report::text AS payload FROM app.processing_results r
        JOIN app.execution_report_ledger l ON r.ledger_id=l.ledger_id
        WHERE r.topic=? AND r.partition_id=? AND r.record_offset=?
        """,
            topic,
            partition,
            ConsoleValues.offset(offset));
    var result = ConsoleValues.observed("PostgreSQL");
    result.put("result", rows.isEmpty() ? null : ConsoleValues.sqlRow(rows.getFirst()));
    return result;
  }
}
