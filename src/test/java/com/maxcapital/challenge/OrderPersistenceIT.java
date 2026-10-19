package com.maxcapital.challenge;

import static com.maxcapital.challenge.TestInfrastructure.report;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.maxcapital.challenge.adapter.in.report.ExecutionReportParser;
import com.maxcapital.challenge.adapter.out.kafka.KafkaHealthIndicator;
import com.maxcapital.challenge.application.InputPosition;
import com.maxcapital.challenge.application.ProcessExecutionReport;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = "spring.kafka.listener.auto-startup=false")
class OrderPersistenceIT {
  private static final AtomicLong OFFSETS = new AtomicLong();
  @MockitoBean KafkaHealthIndicator kafkaHealth;

  @Value("${local.server.port}")
  int port;

  @Container
  static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer(TestInfrastructure.image("postgres", "postgres"));

  @DynamicPropertySource
  static void database(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
  }

  @Autowired ProcessExecutionReport processor;
  @Autowired ExecutionReportParser parser;
  @Autowired JdbcTemplate jdbc;

  @Test
  void returnsCommittedStateAndOrderedLedgerTogetherAndHandlesInvalidOrAbsentOrders()
      throws Exception {
    apply(report(106, 1, "NEW", 0, 10));
    apply(report(106, 2, "PARTIALLY_FILLED", 7, 3));
    var filled = report(106, 3, "FILLED", 10, 0);
    apply(filled);
    apply(filled);
    try (var client = HttpClient.newHttpClient()) {
      var response =
          client.send(
              HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/orders/106"))
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      assertThat(response.statusCode()).isEqualTo(200);
      var body = new tools.jackson.databind.json.JsonMapper().readTree(response.body());
      assertThat(body.path("status").asString()).isEqualTo("FILLED");
      assertThat(body.path("appliedExecutionCount").asInt()).isEqualTo(3);
      assertThat(body.path("accumulativeNominalAmount").decimalValue()).isEqualByComparingTo("10");
      var ledger = body.path("ledger");
      assertThat(ledger.size()).isEqualTo(3);
      long previous = 0;
      for (int i = 0; i < ledger.size(); i++) {
        var entry = ledger.get(i);
        assertThat(entry.path("ledgerId").asLong()).isGreaterThan(previous);
        previous = entry.path("ledgerId").asLong();
        assertThat(entry.path("report").path("fixId").asLong()).isEqualTo(i + 1);
        assertThat(entry.path("report").path("ticker").asString()).isEqualTo("VSCPC");
      }
      for (var invalid : new String[] {"0", "-1", "abc", "9223372036854775808", "999999"}) {
        var result =
            client.send(
                HttpRequest.newBuilder(
                        URI.create("http://localhost:" + port + "/orders/" + invalid))
                    .build(),
                HttpResponse.BodyHandlers.discarding());
        assertThat(result.statusCode()).isEqualTo(invalid.equals("999999") ? 404 : 400);
      }
    }
  }

  @Test
  void appliesSnapshotsAndRecognizesTerminalReplayBeforeTransition() {
    apply(report(101, 1, "NEW", 0, 10));
    apply(report(101, 2, "PARTIALLY_FILLED", 3, 7));
    apply(report(101, 3, "PARTIALLY_FILLED", 7, 3));
    assertThat(
            jdbc.queryForObject(
                "SELECT accumulative_nominal_amount FROM app.orders WHERE numeric_order_id=101",
                java.math.BigDecimal.class))
        .isEqualByComparingTo("7");
    var filled = report(101, 4, "FILLED", 10, 0);
    apply(filled);
    apply(filled.replace("10,", "10.0,"));
    assertThat(count(101)).isEqualTo(4);
    var ids =
        jdbc.queryForList(
            "SELECT ledger_id FROM app.execution_report_ledger WHERE numeric_order_id=101 ORDER BY ledger_id",
            Long.class);
    assertThat(ids).hasSize(4).isSorted().doesNotHaveDuplicates();
    assertThatThrownBy(() -> apply(filled.replace("VSCPC", "OTHER")))
        .hasMessageContaining("contenido distinto");
    assertThatThrownBy(() -> apply(report(101, 5, "FILLED", 10, 0)))
        .hasMessageContaining("transición inválida");
    assertThat(count(101)).isEqualTo(4);
  }

  @Test
  void rollsBackStateCounterAndLedgerWhenAppendFails() {
    apply(report(102, 1, "NEW", 0, 10));
    jdbc.execute(
        """
        CREATE FUNCTION app.fail_test_append() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF NEW.numeric_order_id = 102 AND NEW.fix_id = 2 THEN
        RAISE EXCEPTION 'test append failure'; END IF; RETURN NEW; END $$
        """);
    jdbc.execute(
        "CREATE TRIGGER fail_test_append BEFORE INSERT ON app.execution_report_ledger FOR EACH ROW EXECUTE FUNCTION app.fail_test_append()");
    try {
      assertThatThrownBy(() -> apply(report(102, 2, "FILLED", 10, 0)))
          .isInstanceOf(RuntimeException.class);
      assertThat(count(102)).isEqualTo(1);
      assertThat(
              jdbc.queryForObject(
                  "SELECT status FROM app.orders WHERE numeric_order_id=102", String.class))
          .isEqualTo("NEW");
      assertThat(
              jdbc.queryForObject(
                  "SELECT count(*) FROM app.execution_report_ledger WHERE numeric_order_id=102",
                  Long.class))
          .isEqualTo(1);
    } finally {
      jdbc.execute("DROP TRIGGER fail_test_append ON app.execution_report_ledger");
      jdbc.execute("DROP FUNCTION app.fail_test_append()");
    }
    apply(report(102, 2, "FILLED", 10, 0));
    assertThat(count(102)).isEqualTo(2);
  }

  @Test
  void serializesConcurrentCreationAndDuplicateChecks() throws Exception {
    try (var executor = Executors.newFixedThreadPool(2)) {
      var first = executor.submit(() -> apply(report(103, 1, "NEW", 0, 10)));
      var second = executor.submit(() -> apply(report(103, 1, "NEW", 0, 10)));
      first.get();
      second.get();
    }
    assertThat(count(103)).isEqualTo(1);
    assertThatThrownBy(() -> apply(report(104, 1, "FILLED", 10, 0)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM app.orders WHERE numeric_order_id=104", Long.class))
        .isZero();
  }

  @Test
  void enforcesPrimaryForeignAndReportIdentityConstraints() {
    apply(report(105, 1, "NEW", 0, 10));
    assertThatThrownBy(
            () ->
                jdbc.update(
                    "INSERT INTO app.execution_report_ledger(numeric_order_id,fix_id,report) VALUES(105,1,'{}')"))
        .isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
    assertThatThrownBy(
            () ->
                jdbc.update(
                    "INSERT INTO app.execution_report_ledger(numeric_order_id,fix_id,report) VALUES(999,1,'{}')"))
        .isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
    assertThatThrownBy(
            () ->
                jdbc.update(
                    "INSERT INTO app.orders SELECT * FROM app.orders WHERE numeric_order_id=105"))
        .isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
  }

  private void apply(String payload) {
    var parsed =
        parser.parse(
            new tools.jackson.databind.json.JsonMapper()
                .readTree(payload)
                .path("numericOrderId")
                .asString(),
            payload.getBytes(StandardCharsets.UTF_8));
    processor.process(
        new InputPosition("sql-it", 0, OFFSETS.getAndIncrement()),
        parsed.report(),
        parsed.payload());
  }

  private long count(long order) {
    return jdbc.queryForObject(
        "SELECT applied_execution_count FROM app.orders WHERE numeric_order_id=?",
        Long.class,
        order);
  }
}
