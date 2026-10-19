package com.maxcapital.challenge;

import static com.maxcapital.challenge.TestInfrastructure.report;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.maxcapital.challenge.adapter.in.report.ExecutionReportParser;
import com.maxcapital.challenge.adapter.out.jdbc.JdbcOrderRepository;
import com.maxcapital.challenge.adapter.out.jdbc.JdbcProcessingResults;
import com.maxcapital.challenge.application.InputPosition;
import com.maxcapital.challenge.application.ProcessingResult;
import com.maxcapital.challenge.application.ProcessingResult.Outcome;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
class ProcessingResultsSchemaIT {
  @Container
  static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer(TestInfrastructure.image("postgres", "postgres"));

  static DriverManagerDataSource dataSource;
  static JdbcTemplate jdbc;

  @BeforeAll
  static void migrateFreshDatabase() {
    dataSource = source(POSTGRES.getJdbcUrl());
    Flyway.configure().dataSource(dataSource).schemas("app").load().migrate();
    jdbc = new JdbcTemplate(dataSource);
  }

  @Test
  void roundTripsResultsAndComparesFullJsonWithoutDependingOnFormatting() {
    var parsed =
        new ExecutionReportParser()
            .parse("301", report(301, 1, "NEW", 0, 10).getBytes(StandardCharsets.UTF_8));
    var orders = new JdbcOrderRepository(jdbc);
    orders.initialize(parsed.report());
    var state = orders.lock(301).apply(parsed.report());
    orders.save(state);
    long ledger = orders.append(parsed.report(), parsed.payload());
    var results = new JdbcProcessingResults(jdbc);
    for (var position :
        new InputPosition[] {
          new InputPosition("reports", 0, 7),
          new InputPosition("reports", 1, 7),
          new InputPosition("other", 0, 7)
        }) {
      var expected = new ProcessingResult(position, Outcome.APPLIED, 301, 1, ledger, state);
      results.insert(expected);
      var equivalent = parsed.payload().replace("10", "10.0");
      var stored = results.find(position, equivalent).orElseThrow();
      assertThat(stored.identicalReport()).isTrue();
      assertThat(stored.result())
          .usingRecursiveComparison()
          .withComparatorForType(BigDecimal::compareTo, BigDecimal.class)
          .isEqualTo(expected);
      assertThat(
              results
                  .find(position, parsed.payload().replace("VSCPC", "OTHER"))
                  .orElseThrow()
                  .identicalReport())
          .isFalse();
    }
    var reordered = new tools.jackson.databind.json.JsonMapper().readTree(parsed.payload());
    var tree = new tools.jackson.databind.json.JsonMapper().createObjectNode();
    tree.put("ticker", "VSCPC");
    reordered.properties().forEach(entry -> tree.set(entry.getKey(), entry.getValue()));
    assertThat(
            results
                .find(new InputPosition("reports", 0, 7), tree.toString())
                .orElseThrow()
                .identicalReport())
        .isTrue();
    var match = orders.match(301, 1, parsed.payload()).orElseThrow();
    assertThat(match.ledgerId()).isEqualTo(ledger);
    assertThat(match.identical()).isTrue();
    assertThat(orders.match(301, 2, parsed.payload())).isEmpty();
    assertThat(results.find(new InputPosition("reports", 0, 8), parsed.payload())).isEmpty();
    assertThatThrownBy(
            () ->
                results.insert(
                    new ProcessingResult(
                        new InputPosition("reports", 0, 7),
                        Outcome.DUPLICATE,
                        301,
                        1,
                        ledger,
                        state)))
        .isInstanceOf(DataIntegrityViolationException.class);
    assertThatThrownBy(
            () ->
                results.insert(
                    new ProcessingResult(
                        new InputPosition("reports", 0, 9),
                        Outcome.APPLIED,
                        302,
                        1,
                        ledger,
                        state)))
        .isInstanceOf(DataIntegrityViolationException.class);
    assertThatThrownBy(
            () ->
                jdbc.update(
                    "UPDATE app.processing_results SET record_offset=-1 WHERE numeric_order_id=301"))
        .isInstanceOf(DataIntegrityViolationException.class);
    assertThatThrownBy(
            () ->
                jdbc.update(
                    "UPDATE app.processing_results SET outcome='FAILED' WHERE numeric_order_id=301"))
        .isInstanceOf(DataIntegrityViolationException.class);
    assertThatThrownBy(
            () ->
                jdbc.update(
                    "UPDATE app.processing_results SET status=NULL WHERE numeric_order_id=301"))
        .isInstanceOf(DataIntegrityViolationException.class);
  }

  @Test
  void upgradesV1WithDataWithoutInventingHistoricalResults() {
    jdbc.execute("CREATE DATABASE legacy_results");
    var legacy =
        source(
            "jdbc:postgresql://"
                + POSTGRES.getHost()
                + ":"
                + POSTGRES.getMappedPort(5432)
                + "/legacy_results");
    Flyway.configure().dataSource(legacy).schemas("app").target("1").load().migrate();
    var oldJdbc = new JdbcTemplate(legacy);
    oldJdbc.update("INSERT INTO app.orders VALUES (401,'NEW',10,10,0,1)");
    oldJdbc.update(
        "INSERT INTO app.execution_report_ledger(numeric_order_id,fix_id,report) VALUES (401,1,CAST(? AS jsonb))",
        report(401, 1, "NEW", 0, 10));
    var before = oldJdbc.queryForList("SELECT * FROM app.execution_report_ledger");
    Flyway.configure().dataSource(legacy).schemas("app").load().migrate();
    assertThat(oldJdbc.queryForList("SELECT * FROM app.execution_report_ledger")).isEqualTo(before);
    assertThat(
            oldJdbc.queryForObject(
                "SELECT applied_execution_count FROM app.orders WHERE numeric_order_id=401",
                Long.class))
        .isEqualTo(1);
    assertThat(oldJdbc.queryForObject("SELECT count(*) FROM app.processing_results", Long.class))
        .isZero();
  }

  private static DriverManagerDataSource source(String url) {
    return new DriverManagerDataSource(url, POSTGRES.getUsername(), POSTGRES.getPassword());
  }
}
