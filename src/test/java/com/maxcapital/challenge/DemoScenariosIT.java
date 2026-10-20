package com.maxcapital.challenge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

import com.maxcapital.challenge.demo.DockerAccess;
import com.maxcapital.challenge.demo.InstanceAccess;
import com.maxcapital.challenge.demo.ScenarioRunner;
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
import org.junit.jupiter.api.BeforeEach;
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
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@Testcontainers
@ActiveProfiles("evaluation-console")
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = "demo.step-timeout-ms=15000")
class DemoScenariosIT {
  @Container
  static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer(TestInfrastructure.image("postgres", "postgres"));

  @Container
  static final KafkaContainer KAFKA =
      new KafkaContainer(TestInfrastructure.image("kafka", "apache/kafka"))
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
          .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");

  static ConfigurableApplicationContext first, second;

  @DynamicPropertySource
  static void connections(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    registry.add("spring.kafka.consumer.group-id", () -> "scenarios-it");
    registry.add(
        "demo.app-1-url",
        () -> "http://localhost:" + first.getEnvironment().getProperty("local.server.port"));
    registry.add(
        "demo.app-2-url",
        () -> "http://localhost:" + second.getEnvironment().getProperty("local.server.port"));
  }

  @BeforeAll
  static void start() throws Exception {
    try (var admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
      admin
          .createTopics(
              List.of("execution-reports", "settlements", "execution-reports-rejected").stream()
                  .map(t -> new NewTopic(t, 2, (short) 1))
                  .toList())
          .all()
          .get(20, TimeUnit.SECONDS);
      first = application("first");
      second = application("second");
      await()
          .atMost(Duration.ofSeconds(45))
          .untilAsserted(
              () ->
                  assertThat(
                          admin
                              .describeConsumerGroups(List.of("scenarios-it"))
                              .all()
                              .get()
                              .get("scenarios-it")
                              .members())
                      .hasSize(2));
    }
  }

  static ConfigurableApplicationContext application(String instance) {
    return new SpringApplicationBuilder(MaxCapitalChallengeApplication.class)
        .run(
            "--server.port=0",
            "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
            "--spring.datasource.username=" + POSTGRES.getUsername(),
            "--spring.datasource.password=" + POSTGRES.getPassword(),
            "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
            "--spring.kafka.consumer.group-id=scenarios-it",
            "--spring.kafka.consumer.client-id=" + instance,
            "--app.instance-id=" + instance,
            "--app.demo.observation-enabled=true");
  }

  @AfterAll
  static void stop() {
    if (second != null) second.close();
    if (first != null) first.close();
  }

  @MockitoBean DockerAccess docker;
  @MockitoSpyBean InstanceAccess instances;
  @Autowired ScenarioRunner runner;
  @Autowired JdbcTemplate jdbc;

  @Value("${local.server.port}")
  int port;

  final JsonMapper json = new JsonMapper();

  @BeforeEach
  void controls() {
    when(docker.control(anyString(), anyString()))
        .thenAnswer(
            call ->
                Map.of(
                    "service",
                    call.getArgument(0),
                    "action",
                    call.getArgument(1),
                    "containerState",
                    "running"));
  }

  @Test
  void executesAvailableWithRealKafkaSqlAndTwoInstancesAndKeepsPendingExcluded() throws Exception {
    // GIVEN: Dos aplicaciones reales comparten Kafka y PostgreSQL; Docker está sustituido.
    // WHEN
    var pending = post("/runs", Map.of("scenario", "settlement-once", "mode", "automatic"));

    // THEN
    assertThat(pending.statusCode()).isEqualTo(409);
    verifyNoInteractions(docker);
    // WHEN
    var response = post("/runs", Map.of("scenario", "available", "mode", "automatic"));

    // THEN
    assertThat(response.statusCode()).isEqualTo(200);

    // WHEN
    String id = json.readTree(response.body()).path("id").asString();
    var state = terminal(id);
    // THEN
    assertThat(state.path("status").asString())
        .describedAs(state.toPrettyString())
        .isEqualTo("PASSED");
    assertThat(state.path("passedScenarios").asInt()).isEqualTo(5);
    assertThat(state.path("excludedPending").asInt()).isEqualTo(3);
    assertThat(state.path("messages").size()).isEqualTo(27);
    verify(docker).control("app-1", "restart");
    verify(docker).control("app-2", "restart");
    verify(docker).control("app-1", "start");
    verify(docker).control("app-2", "start");
  }

  @Test
  void resumesStepRunByIdRejectsConcurrentMutationsAndPreservesEffectsOnCancel() throws Exception {
    // GIVEN: El runner y las dos aplicaciones están iniciados por el fixture.
    // WHEN
    var response = post("/runs", Map.of("scenario", "terminal-duplicates", "mode", "step"));
    String id = json.readTree(response.body()).path("id").asString();
    waiting(id, 1);
    var recovered = get("/runs/" + id);

    // THEN
    assertThat(recovered.statusCode()).isEqualTo(200);

    // WHEN
    var concurrentRun =
        post("/runs", Map.of("scenario", "quantity-snapshots", "mode", "automatic"));

    // THEN
    assertThat(concurrentRun.statusCode()).isEqualTo(409);

    // WHEN
    var concurrentPublication = post("/reports", Map.of());

    // THEN
    assertThat(concurrentPublication.statusCode()).isEqualTo(409);

    // WHEN
    var next = post("/runs/" + id + "/next", Map.of());

    // THEN
    assertThat(next.statusCode()).isEqualTo(200);

    // WHEN
    waiting(id, 2);
    post("/runs/" + id + "/next", Map.of());
    var paused = waiting(id, 3);
    String order = paused.path("orders").get(0).asString();
    post("/runs/" + id + "/cancel", Map.of());
    var cancelled = terminal(id);

    // THEN
    assertThat(cancelled.path("status").asString()).isEqualTo("CANCELLED");
    assertThat(
            jdbc.queryForObject(
                "SELECT applied_execution_count FROM app.orders WHERE numeric_order_id=?",
                Long.class,
                Long.parseLong(order)))
        .isEqualTo(1);
  }

  @Test
  void exposesTimeoutWithLastDiagnosisAndNoDataLoss() throws Exception {
    // GIVEN
    doThrow(new IllegalStateException("Falla transitoria visible"))
        .when(instances)
        .read("app-1", "/internal/demo/consumer");
    // WHEN
    var response = post("/runs", Map.of("scenario", "quantity-snapshots", "mode", "automatic"));
    String id = json.readTree(response.body()).path("id").asString();
    var state = terminal(id);
    // THEN
    assertThat(state.path("status").asString()).isEqualTo("FAILED");
    assertThat(state.path("error").asString()).contains("plazo", "Falla transitoria visible");
    assertThat(state.path("orders").size()).isZero();
  }

  @Test
  void restoresOwnedServicesWhenRestartFails() throws Exception {
    // GIVEN
    when(docker.control("app-2", "restart"))
        .thenThrow(new IllegalStateException("Reinicio fallido controlado"));
    // WHEN
    var response = post("/runs", Map.of("scenario", "restart-persistence", "mode", "automatic"));
    String id = json.readTree(response.body()).path("id").asString();
    var state = terminal(id);
    // THEN
    assertThat(state.path("status").asString()).isEqualTo("FAILED");
    assertThat(state.path("error").asString()).contains("Reinicio fallido controlado");
    verify(docker).control("app-1", "start");
    verify(docker).control("app-2", "start");
    for (var order : state.path("orders")) {
      assertThat(
              jdbc.queryForObject(
                  "SELECT applied_execution_count FROM app.orders WHERE numeric_order_id=?",
                  Long.class,
                  Long.parseLong(order.asString())))
          .isPositive();
    }
  }

  JsonNode terminal(String id) {
    await()
        .atMost(Duration.ofSeconds(120))
        .until(
            () -> List.of("PASSED", "FAILED", "CANCELLED").contains(runner.get(id).get("status")));
    return json.valueToTree(runner.get(id));
  }

  JsonNode waiting(String id, int steps) {
    await()
        .atMost(Duration.ofSeconds(35))
        .until(
            () -> {
              var state = json.valueToTree(runner.get(id));
              if (state.path("status").asString().equals("FAILED"))
                throw new AssertionError(state.toPrettyString());
              return state.path("status").asString().equals("WAITING")
                  && state.path("steps").size() == steps;
            });
    return json.valueToTree(runner.get(id));
  }

  HttpResponse<String> get(String path) throws Exception {
    try (var client = HttpClient.newHttpClient()) {
      return client.send(
          HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/demo/api" + path))
              .build(),
          HttpResponse.BodyHandlers.ofString());
    }
  }

  HttpResponse<String> post(String path, Object body) throws Exception {
    try (var client = HttpClient.newHttpClient()) {
      return client.send(
          HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/demo/api" + path))
              .header("Origin", "http://localhost:8083")
              .header("X-Demo-Console", "1")
              .header("Content-Type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(json.writeValueAsString(body)))
              .build(),
          HttpResponse.BodyHandlers.ofString());
    }
  }
}
