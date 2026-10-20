package com.maxcapital.challenge;

import static org.assertj.core.api.Assertions.assertThat;

import com.maxcapital.challenge.adapter.in.report.ExecutionReportParser;
import com.maxcapital.challenge.application.InputPosition;
import com.maxcapital.challenge.application.ProcessExecutionReport;
import com.maxcapital.challenge.demo.InstanceAccess;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@Testcontainers
@ActiveProfiles("evaluation-console")
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = "spring.kafka.bootstrap-servers=127.0.0.1:1")
class EvaluationConsoleQueriesIT {
  @Container
  static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer(TestInfrastructure.image("postgres", "postgres"));

  @DynamicPropertySource
  static void database(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
  }

  @BeforeAll
  static void migrate() {
    Flyway.configure()
        .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
        .schemas("app")
        .defaultSchema("app")
        .load()
        .migrate();
  }

  @Autowired ProcessExecutionReport processor;
  @Autowired ExecutionReportParser parser;

  @Value("${local.server.port}")
  int port;

  final JsonMapper json = new JsonMapper();

  @Test
  void preservesLongIdsDecimalsAdditionalFieldsAndPublicContract() throws Exception {
    String order = "9223372036854775806",
        fix = "9007199254740993",
        amount = "999999999999.12345678";
    String payload =
        """
        {"numericOrderId":%s,"fixId":%s,"status":"NEW","nominalAmounts":%s,
        "leavesNominalAmount":%s,"accumulativeNominalAmount":0,
        "transactionTime":"2026-07-20T18:08:52.129","extra":"<b>texto</b>"}
        """
            .formatted(order, fix, amount, amount);
    apply("exact", 0, 0, order, payload);
    var details = read("/demo/api/orders/" + order);
    assertThat(details.path("numericOrderId").isString()).isTrue();
    assertThat(details.path("numericOrderId").asString()).isEqualTo(order);
    assertThat(details.path("nominalAmounts").asString()).isEqualTo(amount);
    var report = details.path("ledger").get(0).path("payload").asString();
    assertThat(report).contains(fix, amount, "<b>texto</b>");
    var publicView = read("/orders/" + order);
    assertThat(publicView.path("numericOrderId").isIntegralNumber()).isTrue();
    var access = new InstanceAccess("http://localhost:" + port, "http://localhost:" + port);
    var comparison = access.order("app-1", order);
    assertThat(((Map<?, ?>) comparison.get("data")).get("numericOrderId")).isEqualTo(order);
    assertThat(((Map<?, ?>) comparison.get("data")).get("nominalAmounts")).isEqualTo(amount);
    var page = read("/demo/api/orders?limit=1");
    assertThat(page.path("orders").get(0).path("numericOrderId").asString()).isEqualTo(order);
    for (var id : new String[] {"0", "abc", "9223372036854775808", "999999999999"}) {
      assertThat(get("/demo/api/orders/" + id).statusCode())
          .isEqualTo(id.equals("999999999999") ? 404 : 400);
    }
  }

  @Test
  void distinguishesHistoricalResultsAndFullPositionAndPaginates() throws Exception {
    var initial = TestInfrastructure.report(11111, 1, "NEW", 0, 10);
    apply("history", 0, 1, "11111", initial);
    apply("history", 0, 2, "11111", TestInfrastructure.report(11111, 2, "FILLED", 10, 0));
    apply("history", 0, 3, "11111", initial);
    apply("history", 1, 1, "11112", TestInfrastructure.report(11112, 1, "NEW", 0, 10));
    var original = read("/demo/api/results?topic=history&partition=0&offset=1").path("result");
    var late = read("/demo/api/results?topic=history&partition=0&offset=3").path("result");
    assertThat(original.path("status").asString()).isEqualTo("NEW");
    assertThat(late.path("outcome").asString()).isEqualTo("DUPLICATE");
    assertThat(late.path("status").asString()).isEqualTo("FILLED");
    assertThat(late.path("ledgerId").asString()).isEqualTo(original.path("ledgerId").asString());
    assertThat(
            read("/demo/api/results?topic=history&partition=1&offset=1")
                .path("result")
                .path("numericOrderId")
                .asString())
        .isEqualTo("11112");
    assertThat(
            read("/demo/api/results?topic=history&partition=0&offset=999").path("result").isNull())
        .isTrue();
    assertThat(read("/demo/api/orders/11111").path("ledger").size()).isEqualTo(2);
    var first = read("/demo/api/orders?limit=1");
    var cursor = first.path("nextCursor").asString();
    assertThat(cursor).isNotBlank();
    var second = read("/demo/api/orders?limit=1&cursor=" + cursor);
    assertThat(second.path("orders").get(0).path("numericOrderId").asLong())
        .isLessThan(first.path("orders").get(0).path("numericOrderId").asLong());
  }

  private void apply(String topic, int partition, long offset, String key, String payload) {
    var parsed = parser.parse(key, payload.getBytes(StandardCharsets.UTF_8));
    processor.process(
        new InputPosition(topic, partition, offset), parsed.report(), parsed.payload());
  }

  private JsonNode read(String path) throws Exception {
    var response = get(path);
    assertThat(response.statusCode()).isEqualTo(200);
    return json.readTree(response.body());
  }

  private HttpResponse<String> get(String path) throws Exception {
    try (var client = HttpClient.newHttpClient()) {
      return client.send(
          HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).build(),
          HttpResponse.BodyHandlers.ofString());
    }
  }
}
