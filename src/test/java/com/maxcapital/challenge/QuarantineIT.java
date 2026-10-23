package com.maxcapital.challenge;

import static com.maxcapital.challenge.TestInfrastructure.report;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.maxcapital.challenge.adapter.in.report.ExecutionReportParser;
import com.maxcapital.challenge.adapter.out.jdbc.*;
import com.maxcapital.challenge.application.*;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.utils.Utils;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.*;
import org.springframework.test.context.*;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.json.JsonMapper;

@Testcontainers
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = "spring.kafka.listener.auto-startup=false")
class QuarantineIT {
  @Container
  static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer(TestInfrastructure.image("postgres", "postgres"));

  @Container
  static final KafkaContainer KAFKA =
      new KafkaContainer(TestInfrastructure.image("kafka", "apache/kafka"));

  @DynamicPropertySource
  static void connections(DynamicPropertyRegistry r) {
    r.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    r.add("spring.datasource.username", POSTGRES::getUsername);
    r.add("spring.datasource.password", POSTGRES::getPassword);
    r.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
  }

  @Autowired QuarantineStore store;
  @Autowired ProcessExecutionReport processor;
  @Autowired ExecutionReportParser parser;
  @Autowired JdbcTemplate jdbc;

  @Value("${local.server.port}")
  int port;

  final JsonMapper json = new JsonMapper();

  @BeforeAll
  static void topic() throws Exception {
    try (var admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
      admin
          .createTopics(List.of(new NewTopic("quarantine-it", 2, (short) 1)))
          .all()
          .get(20, TimeUnit.SECONDS);
    }
  }

  @Test
  void migrationFromV2PreservesPriorEffectsAndAddsOnlyTwoTables() {
    jdbc.execute("CREATE DATABASE f06_upgrade");
    var ds =
        new DriverManagerDataSource(
            POSTGRES.getJdbcUrl().replace("/" + POSTGRES.getDatabaseName(), "/f06_upgrade"),
            POSTGRES.getUsername(),
            POSTGRES.getPassword());
    Flyway.configure().dataSource(ds).schemas("app").target("2").load().migrate();
    var sql = new JdbcTemplate(ds);
    var p = parser.parse("901", report(901, 1, "NEW", 0, 10).getBytes(StandardCharsets.UTF_8));
    var process =
        new ProcessExecutionReport(
            new JdbcOrderRepository(sql),
            new JdbcProcessingResults(sql),
            mock(QuarantineStore.class));
    new TransactionTemplate(new DataSourceTransactionManager(ds))
        .executeWithoutResult(
            tx -> process.process(new InputPosition("upgrade", 0, 0), p.report(), p.payload()));
    var before = snapshots(sql);
    Flyway.configure().dataSource(ds).schemas("app").load().migrate();
    assertThat(snapshots(sql)).isEqualTo(before);
    assertThat(
            sql.queryForList(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='app' AND table_name<>'flyway_schema_history' ORDER BY table_name",
                String.class))
        .containsExactly(
            "execution_report_ledger",
            "orders",
            "processing_results",
            "quarantined_reports",
            "report_corrections");
  }

  @Test
  void appliesAndAuditsCorrectionsAtomicallyAndReplaysTheEffectiveReport() {
    long order = 910;
    var p = position(order, 20);
    byte[] original = {0, -1, 123};
    var e = store.capture(p, key(order), original, "INVALID_FORMAT");
    assertThat(store.capture(p, key(order), original, "OTHER").causeCode())
        .isEqualTo("INVALID_FORMAT");
    assertThat(e.originalValue()).containsExactly(original);
    var request = correction("atomic", p, order, report(order, 1, "NEW", 0, 10));
    var accepted = store.accept(request, p.partition());
    assertThat(store.accept(request, p.partition())).isEqualTo(accepted);
    assertThatThrownBy(
            () ->
                store.accept(
                    correction("other", p, order, request.effectiveReport()), p.partition()))
        .isInstanceOf(CorrectionConflict.class);
    jdbc.execute(
        "CREATE FUNCTION app.fail_correction() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.record_offset=20 AND NEW.state='RESOLVED' THEN RAISE EXCEPTION 'rollback correction'; END IF; RETURN NEW; END $$");
    jdbc.execute(
        "CREATE TRIGGER fail_correction BEFORE UPDATE ON app.quarantined_reports FOR EACH ROW EXECUTE FUNCTION app.fail_correction()");
    try {
      assertThatThrownBy(() -> apply(p, accepted))
          .isInstanceOf(org.springframework.dao.DataAccessException.class);
      assertThat(
              jdbc.queryForObject(
                  "SELECT count(*) FROM app.orders WHERE numeric_order_id=?", Integer.class, order))
          .isZero();
      assertThat(store.find(p, false).orElseThrow().state()).isEqualTo("READY");
    } finally {
      jdbc.execute("DROP TRIGGER fail_correction ON app.quarantined_reports");
      jdbc.execute("DROP FUNCTION app.fail_correction()");
    }
    var first = apply(p, accepted);
    var end =
        parser.parse(
            Long.toString(order),
            report(order, 2, "CANCELLED", 0, 0).getBytes(StandardCharsets.UTF_8));
    var cancelledPosition = position(order, 21);
    store.capture(cancelledPosition, key(order), new byte[] {0}, "INVALID_FORMAT");
    var cancelled =
        store.accept(
            correction("cancelled", cancelledPosition, order, end.payload()), p.partition());
    assertThat(Settlement.from(apply(cancelledPosition, cancelled))).isEmpty();
    assertThat(apply(p, accepted))
        .usingRecursiveComparison()
        .withComparatorForType(java.math.BigDecimal::compareTo, java.math.BigDecimal.class)
        .isEqualTo(first);
    assertThat(store.find(p, false).orElseThrow().state()).isEqualTo("RESOLVED");
    assertThat(store.find(p, false).orElseThrow().originalValue()).containsExactly(original);
    assertThatThrownBy(
            () ->
                store.accept(
                    correction("after-success", p, order, request.effectiveReport()),
                    p.partition()))
        .isInstanceOf(CorrectionConflict.class);
    var duplicatePosition = position(order, 22);
    store.capture(duplicatePosition, key(order), null, "INVALID_FORMAT");
    var duplicate =
        store.accept(
            correction("duplicate", duplicatePosition, order, request.effectiveReport()),
            p.partition());
    var result = apply(duplicatePosition, duplicate);
    assertThat(result.outcome()).isEqualTo(ProcessingResult.Outcome.DUPLICATE);
    assertThat(Settlement.from(result)).isEmpty();
    assertThat(Settlement.from(apply(cancelledPosition, cancelled))).isEmpty();
    assertThat(apply(duplicatePosition, duplicate).outcome())
        .isEqualTo(ProcessingResult.Outcome.DUPLICATE);
    assertThat(
            jdbc.queryForObject(
                "SELECT applied_execution_count FROM app.orders WHERE numeric_order_id=?",
                Long.class,
                order))
        .isEqualTo(2);

    long rejectedOrder = 911;
    var q = position(rejectedOrder, 30);
    store.capture(q, key(rejectedOrder), new byte[0], "INVALID_FORMAT");
    var bad =
        store.accept(
            correction(
                "bad-transition", q, rejectedOrder, report(rejectedOrder, 1, "FILLED", 10, 0)),
            q.partition());
    assertThatThrownBy(() -> apply(q, bad))
        .isInstanceOf(com.maxcapital.challenge.domain.InvalidReport.class);
    store.rejectCorrection(q, "another-request", "INVALID_TRANSITION");
    assertThat(store.find(q, false).orElseThrow().state()).isEqualTo("READY");
    store.rejectCorrection(q, bad.requestId(), "INVALID_TRANSITION");
    assertThat(store.find(q, false).orElseThrow().state()).isEqualTo("OPEN");
    var valid =
        store.accept(
            correction(
                "valid-after-rejection", q, rejectedOrder, report(rejectedOrder, 1, "NEW", 0, 10)),
            q.partition());
    apply(q, valid);
    assertThat(store.corrections(q))
        .extracting(QuarantineStore.Correction::outcome)
        .containsExactly("REJECTED", "APPLIED");
  }

  @Test
  void exposesBinaryOriginalAndAcceptsIdempotentRequestsWithoutApplyingBusiness() throws Exception {
    long order = 920;
    var p = position(order, 40);
    store.capture(p, key(order), new byte[] {123}, "INVALID_FORMAT");
    var body =
        Map.of(
            "requestId",
            "http-fix",
            "source",
            Map.of(
                "topic",
                p.topic(),
                "partition",
                p.partition(),
                "offset",
                Long.toString(p.offset())),
            "effectiveKey",
            Long.toString(order),
            "effectiveReport",
            json.readTree(report(order, 1, "NEW", 0, 10)),
            "reason",
            "Corrección del ensayo",
            "declaredOperator",
            "operador local");
    var first = post(body);
    assertThat(first.statusCode()).isEqualTo(202);
    assertThat(json.readTree(first.body()).path("outcome").asString()).isEqualTo("ACCEPTED");
    assertThat(post(body).body()).isEqualTo(first.body());
    var changed = new LinkedHashMap<String, Object>(body);
    changed.put("reason", "Otro contenido");
    assertThat(post(changed).statusCode()).isEqualTo(409);
    changed.put("requestId", "another-request");
    assertThat(post(changed).statusCode()).isEqualTo(409);
    changed.put(
        "source", Map.of("topic", p.topic(), "partition", p.partition(), "offset", "99999"));
    assertThat(post(changed).statusCode()).isEqualTo(404);
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM app.orders WHERE numeric_order_id=?", Integer.class, order))
        .isZero();
    try (var client = HttpClient.newHttpClient()) {
      var response =
          client.send(
              HttpRequest.newBuilder(
                      URI.create(
                          "http://localhost:"
                              + port
                              + "/quarantine?topic=quarantine-it&partition="
                              + p.partition()
                              + "&offset=40"))
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      assertThat(response.statusCode()).isEqualTo(200);
      var view = json.readTree(response.body());
      assertThat(view.path("originalValueBase64").asString()).isEqualTo("ew==");
      assertThat(view.path("state").asString()).isEqualTo("READY");
      assertThat(view.path("corrections").size()).isEqualTo(1);
    }
    var wrong = position(930, 50);
    store.capture(wrong, key(930), new byte[] {1}, "INVALID_KEY");
    assertThatThrownBy(
            () ->
                store.accept(
                    correction("change-key", wrong, 931, report(931, 1, "NEW", 0, 10)),
                    wrong.partition()))
        .isInstanceOf(CorrectionConflict.class);
    assertThatThrownBy(
            () ->
                store.accept(
                    correction("wrong-part", wrong, 930, report(930, 1, "NEW", 0, 10)),
                    1 - wrong.partition()))
        .isInstanceOf(CorrectionConflict.class);
  }

  private ProcessingResult apply(InputPosition p, QuarantineStore.Correction c) {
    var parsed =
        parser.parse(c.effectiveKey(), c.effectiveReport().getBytes(StandardCharsets.UTF_8));
    return processor.process(p, parsed.report(), parsed.payload(), c.requestId());
  }

  private static QuarantineStore.Correction correction(
      String id, InputPosition p, long order, String report) {
    return new QuarantineStore.Correction(
        id,
        p,
        Long.toString(order),
        report,
        "Corrección del ensayo",
        "operador local",
        "ACCEPTED",
        null,
        null);
  }

  private static InputPosition position(long order, long offset) {
    return new InputPosition("quarantine-it", (Utils.murmur2(key(order)) & 0x7fffffff) % 2, offset);
  }

  private static byte[] key(long order) {
    return Long.toString(order).getBytes(StandardCharsets.UTF_8);
  }

  private HttpResponse<String> post(Object body) throws Exception {
    try (var client = HttpClient.newHttpClient()) {
      return client.send(
          HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/quarantine/corrections"))
              .header("Content-Type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(json.writeValueAsString(body)))
              .build(),
          HttpResponse.BodyHandlers.ofString());
    }
  }

  private static List<Object> snapshots(JdbcTemplate jdbc) {
    return List.of(
        jdbc.queryForList("SELECT * FROM app.orders"),
        jdbc.queryForList("SELECT * FROM app.execution_report_ledger"),
        jdbc.queryForList("SELECT * FROM app.processing_results"));
  }
}
