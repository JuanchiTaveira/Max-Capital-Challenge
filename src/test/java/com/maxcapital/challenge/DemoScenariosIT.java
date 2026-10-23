package com.maxcapital.challenge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

import com.maxcapital.challenge.demo.DockerAccess;
import com.maxcapital.challenge.demo.InstanceAccess;
import com.maxcapital.challenge.demo.ScenarioCatalog;
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
      first = application("app-1");
      second = application("app-2");
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
  @MockitoSpyBean ScenarioCatalog scenarioCatalog;
  @Autowired ScenarioRunner runner;
  @Autowired JdbcTemplate jdbc;

  @Value("${local.server.port}")
  int port;

  final JsonMapper json = new JsonMapper();

  @BeforeEach
  void controls() {
    // Este fixture sustituye Docker: conserva el recorrido de los cinco escenarios
    // originales. Las caídas de F-05 se ejercitan con procesos reales y Compose.
    doReturn(
            List.of(
                "interleaved-orders",
                "quantity-snapshots",
                "terminal-duplicates",
                "restart-persistence",
                "committed-input-only"))
        .when(scenarioCatalog)
        .available();
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
  void executesLegacyScenariosWithRealKafkaSqlAndAdvertisesEightScenarios() throws Exception {
    // GIVEN: Dos aplicaciones reales comparten Kafka y PostgreSQL; Docker está sustituido.
    var catalog = json.readTree(get("/catalog").body());
    assertThat(catalog.size()).isEqualTo(8);
    assertThat(catalog.get(0).path("title").asString()).isEqualTo("Dos órdenes en paralelo");
    for (int index = 0; index < 8; index++) {
      assertThat(catalog.get(index).path("explanation").asString()).isNotBlank();
      assertThat(catalog.get(index).path("actions").size()).isEqualTo(3);
    }
    // WHEN
    assertThat(catalog.valueStream().filter(entry -> entry.path("available").asBoolean()).count())
        .isEqualTo(8);
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
    assertThat(state.path("excludedPending").asInt()).isEqualTo(0);
    assertThat(state.path("messages").size()).isEqualTo(27);
    assertThat(state.path("scenarioResults").size()).isEqualTo(5);
    for (var scenario : state.path("scenarioResults")) {
      String scenarioId = scenario.path("scenarioId").asString();
      assertThat(scenario.path("status").asString()).isEqualTo("PASSED");
      assertThat(scenario.path("conclusion").asString()).isNotBlank();
      for (var check : scenario.path("checks")) {
        assertThat(check.path("status").asString()).isEqualTo("PASSED");
        assertThat(check.path("observed").asString()).isNotBlank();
        assertThat(check.path("observedAt").asString()).isNotBlank();
        assertThat(check.path("references").isEmpty()).isFalse();
        var step =
            java.util.stream.StreamSupport.stream(state.path("steps").spliterator(), false)
                .filter(item -> item.path("id").asString().equals(check.path("stepId").asString()))
                .findFirst()
                .orElseThrow();
        assertThat(step.path("scenarioId").asString()).isEqualTo(scenarioId);
      }
    }
    for (var message : state.path("messages")) {
      assertThat(message.path("id").asString())
          .isEqualTo(
              message.path("topic").asString()
                  + "/"
                  + message.path("partition").asString()
                  + "/"
                  + message.path("offset").asString());
      assertThat(
              java.util.stream.StreamSupport.stream(state.path("steps").spliterator(), false)
                  .anyMatch(
                      step ->
                          step.path("id").asString().equals(message.path("stepId").asString())
                              && step.path("scenarioId")
                                  .asString()
                                  .equals(message.path("scenarioId").asString())))
          .isTrue();
    }
    var duplicateScenario = state.path("scenarioResults").get(2);
    assertThat(
            check(duplicateScenario, "new")
                .path("references")
                .path("snapshot")
                .path("status")
                .asString())
        .isEqualTo("NEW");
    assertThat(
            check(duplicateScenario, "final")
                .path("references")
                .path("snapshot")
                .path("status")
                .asString())
        .isEqualTo("FILLED");
    assertThat(
            check(duplicateScenario, "final")
                .path("references")
                .path("snapshot")
                .path("appliedExecutionCount")
                .isString())
        .isTrue();
    assertThat(
            check(state.path("scenarioResults").get(4), "visibility").path("observed").asString())
        .contains("excluido");
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
    var partial = cancelled.path("scenarioResults").get(0);
    assertThat(partial.path("status").asString()).isEqualTo("CANCELLED");
    assertThat(check(partial, "new").path("status").asString()).isEqualTo("PASSED");
    assertThat(check(partial, "final").path("status").asString()).isEqualTo("NOT_OBSERVED");
    assertThat(check(partial, "final").path("observed").isNull()).isTrue();
    assertThat(partial.path("conclusion").asString()).contains("cancelada");
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
    var response = post("/runs", Map.of("scenario", "available", "mode", "automatic"));
    String id = json.readTree(response.body()).path("id").asString();
    var state = terminal(id);
    // THEN
    assertThat(state.path("status").asString()).isEqualTo("FAILED");
    assertThat(state.path("error").asString()).contains("plazo", "Falla transitoria visible");
    assertThat(state.path("orders").size()).isZero();
    var partial = state.path("scenarioResults").get(0);
    assertThat(check(partial, "ready").path("status").asString()).isEqualTo("FAILED");
    assertThat(check(partial, "ready").path("observed").isNull()).isTrue();
    assertThat(check(partial, "ready").path("diagnostic").asString())
        .contains("Falla transitoria visible");
    for (int index = 1; index < 5; index++) {
      assertThat(state.path("scenarioResults").get(index).path("status").asString())
          .isEqualTo("NOT_RUN");
      for (var check : state.path("scenarioResults").get(index).path("checks")) {
        assertThat(check.path("status").asString()).isEqualTo("NOT_OBSERVED");
        assertThat(check.path("observed").isNull()).isTrue();
      }
    }
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
    var partial = state.path("scenarioResults").get(0);
    assertThat(check(partial, "order-a").path("status").asString()).isEqualTo("PASSED");
    assertThat(check(partial, "preserved").path("status").asString()).isEqualTo("NOT_OBSERVED");
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

  JsonNode check(JsonNode scenario, String id) {
    return java.util.stream.StreamSupport.stream(scenario.path("checks").spliterator(), false)
        .filter(item -> item.path("id").asString().equals(id))
        .findFirst()
        .orElseThrow();
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
