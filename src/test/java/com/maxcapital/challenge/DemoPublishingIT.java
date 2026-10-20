package com.maxcapital.challenge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.maxcapital.challenge.demo.ConsoleKafka;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.json.JsonMapper;

@Testcontainers
@ActiveProfiles("evaluation-console")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class DemoPublishingIT {
  @Container
  static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer(TestInfrastructure.image("postgres", "postgres"));

  @Container
  static final KafkaContainer KAFKA =
      new KafkaContainer(TestInfrastructure.image("kafka", "apache/kafka"))
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
          .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");

  static ConfigurableApplicationContext core;

  @DynamicPropertySource
  static void connections(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
  }

  @BeforeAll
  static void startCore() throws Exception {
    try (var admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
      admin
          .createTopics(
              ConsoleKafka.TOPICS.stream().map(t -> new NewTopic(t, 2, (short) 1)).toList())
          .all()
          .get(20, TimeUnit.SECONDS);
    }
    core =
        new SpringApplicationBuilder(MaxCapitalChallengeApplication.class)
            .run(
                "--server.port=0",
                "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                "--spring.datasource.username=" + POSTGRES.getUsername(),
                "--spring.datasource.password=" + POSTGRES.getPassword(),
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--spring.kafka.consumer.group-id=publishing-it");
  }

  @AfterAll
  static void closeCore() {
    if (core != null) core.close();
  }

  @Autowired ConsoleKafka kafka;
  @Autowired JdbcTemplate jdbc;

  @Value("${local.server.port}")
  int port;

  final JsonMapper json = new JsonMapper();

  @Test
  void publishesAndRepublishesExactTerminalAndRejectsKnownInvalidDrafts() throws Exception {
    String key = "81001", initial = TestInfrastructure.report(81001, 1, "NEW", 0, 10);
    var first = post("/reports", Map.of("key", key, "payload", initial));
    assertThat(first.statusCode()).isEqualTo(200);
    var metadata = json.readTree(first.body());
    applied(key, 1);
    var terminal =
        post(
            "/reports",
            Map.of("key", key, "payload", TestInfrastructure.report(81001, 2, "FILLED", 10, 0)));
    assertThat(terminal.statusCode()).isEqualTo(200);
    applied(key, 2);
    var terminalMetadata = json.readTree(terminal.body());
    assertThat(
            post(
                    "/messages/republication",
                    Map.of(
                        "topic",
                        "execution-reports",
                        "partition",
                        terminalMetadata.path("partition").asInt(),
                        "offset",
                        terminalMetadata.path("offset").asString()))
                .statusCode())
        .isEqualTo(200);
    var repeated =
        post(
            "/messages/republication",
            Map.of(
                "topic",
                "execution-reports",
                "partition",
                metadata.path("partition").asInt(),
                "offset",
                metadata.path("offset").asString()));
    assertThat(repeated.statusCode()).isEqualTo(200);
    var duplicate = json.readTree(repeated.body());
    assertThat(duplicate.path("payload").asString()).isEqualTo(initial);
    assertThat(duplicate.path("offset").asLong()).isGreaterThan(metadata.path("offset").asLong());
    await()
        .untilAsserted(
            () ->
                assertThat(
                        jdbc.queryForObject(
                            "SELECT count(*) FROM app.processing_results WHERE numeric_order_id=81001",
                            Long.class))
                    .isEqualTo(4));
    assertThat(
            jdbc.queryForObject(
                "SELECT applied_execution_count FROM app.orders WHERE numeric_order_id=81001",
                Long.class))
        .isEqualTo(2);
    for (var payload :
        List.of(
            "{",
            initial.replace("VSCPC", "OTHER"),
            TestInfrastructure.report(81001, 3, "FILLED", 10, 0))) {
      assertThat(post("/reports", Map.of("key", key, "payload", payload)).statusCode())
          .isEqualTo(400);
    }
    assertThat(post("/reports", Map.of("key", "81002", "payload", initial)).statusCode())
        .isEqualTo(400);
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM app.processing_results WHERE numeric_order_id=81001",
                Long.class))
        .isEqualTo(4);
  }

  @Test
  void preservesExactNumericPayloadAcrossPublicationInspectionAndRepublication() throws Exception {
    String key = "9223372036854775806";
    String payload =
        """
        {"numericOrderId":9223372036854775806,"fixId":9007199254740993,"status":"NEW",
        "nominalAmounts":999999999999.12345678,"leavesNominalAmount":999999999999.12345678,
        "accumulativeNominalAmount":0,"transactionTime":"2026-07-20T18:08:52.129","extra":"intacto"}
        """;
    var response = post("/reports", Map.of("key", key, "payload", payload));
    assertThat(response.statusCode()).isEqualTo(200);
    applied(key, 1);
    var position = json.readTree(response.body());
    assertThat(
            kafka
                .exact(
                    "execution-reports",
                    position.path("partition").asInt(),
                    position.path("offset").asString())
                .payload())
        .isEqualTo(payload);
    var repeated =
        post(
            "/messages/republication",
            Map.of(
                "topic",
                "execution-reports",
                "partition",
                position.path("partition").asInt(),
                "offset",
                position.path("offset").asString()));
    assertThat(repeated.statusCode()).isEqualTo(200);
    assertThat(json.readTree(repeated.body()).path("payload").asString()).isEqualTo(payload);
    assertThat(
            jdbc.queryForObject(
                "SELECT nominal_amounts FROM app.orders WHERE numeric_order_id=?",
                java.math.BigDecimal.class,
                Long.parseLong(key)))
        .isEqualByComparingTo("999999999999.12345678");
  }

  @Test
  void readsCommittedWithGapsAndDoesNotChangeGroupMembershipOrReplayAbortedPosition()
      throws Exception {
    String key = "82001";
    var first = kafka.transaction(key, TestInfrastructure.report(82001, 1, "NEW", 0, 10), false);
    applied(key, 1);
    var aborted =
        kafka.transaction(key, TestInfrastructure.report(82001, 2, "PARTIALLY_FILLED", 7, 3), true);
    var last = kafka.transaction(key, TestInfrastructure.report(82001, 3, "FILLED", 10, 0), false);
    applied(key, 2);
    var page = kafka.read(first.topic(), Integer.parseInt(first.partition()), first.offset(), 1);
    @SuppressWarnings("unchecked")
    var records = (List<ConsoleKafka.Record>) page.get("records");
    assertThat(records).containsExactly(first);
    var next =
        kafka.read(
            first.topic(),
            Integer.parseInt(first.partition()),
            page.get("nextCursor").toString(),
            200);
    @SuppressWarnings("unchecked")
    var remaining = (List<ConsoleKafka.Record>) next.get("records");
    assertThat(remaining).contains(last).doesNotContain(aborted);
    assertThatThrownBy(
            () ->
                kafka.exact(
                    aborted.topic(), Integer.parseInt(aborted.partition()), aborted.offset()))
        .isInstanceOf(org.springframework.web.server.ResponseStatusException.class);
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM app.processing_results WHERE numeric_order_id=82001 AND fix_id=2",
                Long.class))
        .isZero();
    try (var admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
      assertThat(
              admin
                  .describeConsumerGroups(List.of("publishing-it"))
                  .all()
                  .get()
                  .get("publishing-it")
                  .members())
          .hasSize(1);
      assertThat(admin.listConsumerGroups().all().get())
          .allSatisfy(g -> assertThat(g.groupId()).isEqualTo("publishing-it"));
    }
  }

  @Test
  void reportsUnavailableAcknowledgementWithoutInventingMetadata() {
    assertThatThrownBy(
            () ->
                new ConsoleKafka("127.0.0.1:1")
                    .publish("1", TestInfrastructure.report(1, 1, "NEW", 0, 10)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("incierto");
  }

  void applied(String key, long count) {
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () ->
                assertThat(
                        jdbc.queryForObject(
                            "SELECT applied_execution_count FROM app.orders WHERE numeric_order_id=?",
                            Long.class,
                            Long.parseLong(key)))
                    .isEqualTo(count));
  }

  HttpResponse<String> post(String path, Object body) throws Exception {
    try (var client = HttpClient.newHttpClient()) {
      return client.send(
          HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/demo/api" + path))
              .header("Content-Type", "application/json")
              .header("Origin", "http://localhost:8083")
              .header("X-Demo-Console", "1")
              .POST(HttpRequest.BodyPublishers.ofString(json.writeValueAsString(body)))
              .build(),
          HttpResponse.BodyHandlers.ofString());
    }
  }
}
