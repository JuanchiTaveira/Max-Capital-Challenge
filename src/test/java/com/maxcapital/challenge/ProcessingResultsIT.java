package com.maxcapital.challenge;

import static com.maxcapital.challenge.TestInfrastructure.report;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.maxcapital.challenge.adapter.in.report.ExecutionReportParser;
import com.maxcapital.challenge.adapter.out.jdbc.JdbcOrderRepository;
import com.maxcapital.challenge.adapter.out.jdbc.JdbcProcessingResults;
import com.maxcapital.challenge.adapter.out.jdbc.OrderTransactionConfiguration;
import com.maxcapital.challenge.application.InputPosition;
import com.maxcapital.challenge.application.ProcessExecutionReport;
import com.maxcapital.challenge.application.ProcessingResult;
import com.maxcapital.challenge.application.ProcessingResult.Outcome;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
class ProcessingResultsIT {
  @Container
  static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer(TestInfrastructure.image("postgres", "postgres"));

  static JdbcTemplate jdbc;
  AnnotationConfigApplicationContext context;
  final ExecutionReportParser parser = new ExecutionReportParser();

  @Configuration(proxyBeanMethods = false)
  @EnableTransactionManagement
  static class Transactions {}

  @BeforeAll
  static void database() {
    var source = source();
    Flyway.configure().dataSource(source).schemas("app").load().migrate();
    jdbc = new JdbcTemplate(source);
  }

  @BeforeEach
  void start() {
    context = application();
  }

  @AfterEach
  void close() {
    context.close();
  }

  @Test
  void recoversOriginalAppliedAndDuplicateSnapshotsAfterTheOrderAdvances() {
    var first = apply(context, 501, 0, report(501, 1, "NEW", 0, 10));
    var duplicate = apply(context, 501, 1, report(501, 1, "NEW", 0, 10));
    assertThat(first.outcome()).isEqualTo(Outcome.APPLIED);
    assertThat(duplicate.outcome()).isEqualTo(Outcome.DUPLICATE);
    assertThat(duplicate.ledgerId()).isEqualTo(first.ledgerId());
    apply(context, 501, 2, report(501, 2, "PARTIALLY_FILLED", 7, 3));
    apply(context, 501, 3, report(501, 3, "FILLED", 10, 0));
    var replay = apply(context, 501, 0, report(501, 1, "NEW", 0, 10).replace("10,", "10.0,"));
    equalResult(replay, first);
    equalResult(apply(context, 501, 1, report(501, 1, "NEW", 0, 10)), duplicate);
    var lateDuplicate = apply(context, 501, 4, report(501, 1, "NEW", 0, 10));
    assertThat(lateDuplicate.outcome()).isEqualTo(Outcome.DUPLICATE);
    assertThat(lateDuplicate.state().status().name()).isEqualTo("FILLED");
    assertThat(lateDuplicate.ledgerId()).isEqualTo(first.ledgerId());
    assertCounts(501, 3, 3, 5);
    assertThat(replay.state().status().name()).isEqualTo("NEW");
    assertThat(replay.state().appliedExecutionCount()).isEqualTo(1);
  }

  @Test
  void rejectsChangedPositionAndBusinessIdentityWithoutRecordingSuccess() {
    var original = apply(context, 502, 0, report(502, 1, "NEW", 0, 10));
    assertThatThrownBy(
            () -> apply(context, 502, 0, report(502, 1, "NEW", 0, 10).replace("VSCPC", "OTHER")))
        .hasMessageContaining("contenido distinto para posición");
    assertThatThrownBy(() -> apply(context, 502, 0, report(599, 1, "NEW", 0, 10)))
        .hasMessageContaining("contenido distinto para posición");
    assertThatThrownBy(
            () ->
                parser.parse("599", report(502, 1, "NEW", 0, 10).getBytes(StandardCharsets.UTF_8)))
        .hasMessageContaining("key Kafka distinta");
    assertThatThrownBy(
            () -> apply(context, 502, 1, report(502, 1, "NEW", 0, 10).replace("VSCPC", "OTHER")))
        .hasMessageContaining("contenido distinto para fixId");
    assertThatThrownBy(() -> apply(context, 502, 2, report(502, 2, "NEW", 0, 10)))
        .hasMessageContaining("transición inválida");
    equalResult(apply(context, 502, 0, report(502, 1, "NEW", 0, 10)), original);
    assertCounts(502, 1, 1, 1);
    assertCounts(599, 0, 0, 0);
  }

  @Test
  void serializesConcurrentSamePositionAndConcurrentRepublications() throws Exception {
    try (var other = application();
        var executor = Executors.newFixedThreadPool(2)) {
      for (long order : new long[] {503, 504}) {
        var ready = new CountDownLatch(2);
        var go = new CountDownLatch(1);
        long secondOffset = order == 503 ? 0 : 1;
        var first =
            executor.submit(
                () -> {
                  ready.countDown();
                  assertThat(go.await(10, TimeUnit.SECONDS)).isTrue();
                  return apply(context, order, 0, report(order, 1, "NEW", 0, 10));
                });
        var second =
            executor.submit(
                () -> {
                  ready.countDown();
                  assertThat(go.await(10, TimeUnit.SECONDS)).isTrue();
                  return apply(other, order, secondOffset, report(order, 1, "NEW", 0, 10));
                });
        assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
        go.countDown();
        var a = first.get(20, TimeUnit.SECONDS);
        var b = second.get(20, TimeUnit.SECONDS);
        if (order == 503) {
          equalResult(a, b);
          assertThat(a.outcome()).isEqualTo(Outcome.APPLIED);
        } else {
          assertThat(List.of(a.outcome(), b.outcome()))
              .containsExactlyInAnyOrder(Outcome.APPLIED, Outcome.DUPLICATE);
          assertThat(a.ledgerId()).isEqualTo(b.ledgerId());
        }
        assertCounts(order, 1, 1, order == 503 ? 1 : 2);
      }
    }
  }

  @Test
  void rollsBackAllWritesWhenResultOrLedgerInsertionFailsAndRetriesCleanly() {
    for (String table : List.of("processing_results", "execution_report_ledger")) {
      long base = table.equals("processing_results") ? 510 : 520;
      for (long order : new long[] {base, base + 1}) {
        boolean existing = order != base;
        if (existing) apply(context, order, 0, report(order, 1, "NEW", 0, 10));
        jdbc.execute(
            """
            CREATE FUNCTION app.fail_result_test() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN IF NEW.numeric_order_id = %d THEN
            RAISE EXCEPTION 'test atomic rollback'; END IF; RETURN NEW; END $$
            """
                .formatted(order));
        jdbc.execute(
            "CREATE TRIGGER fail_result_test BEFORE INSERT ON app."
                + table
                + " FOR EACH ROW EXECUTE FUNCTION app.fail_result_test()");
        var payload =
            report(
                order,
                existing ? 2 : 1,
                existing ? "FILLED" : "NEW",
                existing ? 10 : 0,
                existing ? 0 : 10);
        try {
          assertThatThrownBy(() -> apply(context, order, 1, payload))
              .isInstanceOf(RuntimeException.class);
          assertCounts(order, existing ? 1 : 0, existing ? 1 : 0, existing ? 1 : 0);
          if (existing)
            assertThat(
                    jdbc.queryForObject(
                        "SELECT status FROM app.orders WHERE numeric_order_id=?",
                        String.class,
                        order))
                .isEqualTo("NEW");
        } finally {
          jdbc.execute("DROP TRIGGER fail_result_test ON app." + table);
          jdbc.execute("DROP FUNCTION app.fail_result_test()");
        }
        var resolved = apply(context, order, 1, payload);
        equalResult(apply(context, order, 1, payload), resolved);
        assertCounts(order, existing ? 2 : 1, existing ? 2 : 1, existing ? 2 : 1);
      }
    }
  }

  @Test
  void recoversFromAnotherApplicationAndAfterClosingAllContexts() {
    var applied = apply(context, 530, 0, report(530, 1, "NEW", 0, 10));
    var duplicate = apply(context, 530, 1, report(530, 1, "NEW", 0, 10));
    apply(context, 530, 2, report(530, 2, "FILLED", 10, 0));
    try (var second = application()) {
      equalResult(apply(second, 530, 0, report(530, 1, "NEW", 0, 10)), applied);
      equalResult(apply(second, 530, 1, report(530, 1, "NEW", 0, 10)), duplicate);
    }
    context.close();
    try (var restarted = application()) {
      equalResult(apply(restarted, 530, 0, report(530, 1, "NEW", 0, 10)), applied);
      equalResult(apply(restarted, 530, 1, report(530, 1, "NEW", 0, 10)), duplicate);
    }
    assertCounts(530, 2, 2, 3);
  }

  @Test
  void recordsDuplicateForLegacyLedgerWithoutBackfillingUnknownPositions() {
    var orders = new JdbcOrderRepository(jdbc);
    var parsed = parser.parse("540", report(540, 1, "NEW", 0, 10).getBytes(StandardCharsets.UTF_8));
    orders.initialize(parsed.report());
    orders.save(orders.lock(540).apply(parsed.report()));
    long ledger = orders.append(parsed.report(), parsed.payload());
    assertCounts(540, 1, 1, 0);
    var known = apply(context, 540, 99, report(540, 1, "NEW", 0, 10));
    assertThat(known.outcome()).isEqualTo(Outcome.DUPLICATE);
    assertThat(known.ledgerId()).isEqualTo(ledger);
    assertCounts(540, 1, 1, 1);
  }

  private ProcessingResult apply(
      AnnotationConfigApplicationContext app, long order, long offset, String payload) {
    var tree = new tools.jackson.databind.json.JsonMapper().readTree(payload);
    var parsed =
        parser.parse(
            tree.path("numericOrderId").asString(), payload.getBytes(StandardCharsets.UTF_8));
    return app.getBean(ProcessExecutionReport.class)
        .process(
            new InputPosition("results-" + order, 0, offset), parsed.report(), parsed.payload());
  }

  private static void equalResult(ProcessingResult actual, ProcessingResult expected) {
    assertThat(actual)
        .usingRecursiveComparison()
        .withComparatorForType(BigDecimal::compareTo, BigDecimal.class)
        .isEqualTo(expected);
  }

  private static void assertCounts(long order, long counter, long ledger, long results) {
    assertThat(
            jdbc.queryForObject(
                "SELECT coalesce(sum(applied_execution_count),0) FROM app.orders WHERE numeric_order_id=?",
                Long.class,
                order))
        .isEqualTo(counter);
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM app.execution_report_ledger WHERE numeric_order_id=?",
                Long.class,
                order))
        .isEqualTo(ledger);
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM app.processing_results WHERE numeric_order_id=?",
                Long.class,
                order))
        .isEqualTo(results);
  }

  private static DriverManagerDataSource source() {
    return new DriverManagerDataSource(
        POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
  }

  private static AnnotationConfigApplicationContext application() {
    var app = new AnnotationConfigApplicationContext();
    app.registerBean(DataSource.class, ProcessingResultsIT::source);
    app.registerBean(JdbcTemplate.class, () -> new JdbcTemplate(app.getBean(DataSource.class)));
    app.register(
        Transactions.class,
        OrderTransactionConfiguration.class,
        JdbcOrderRepository.class,
        JdbcProcessingResults.class,
        ProcessExecutionReport.class);
    app.refresh();
    return app;
  }
}
