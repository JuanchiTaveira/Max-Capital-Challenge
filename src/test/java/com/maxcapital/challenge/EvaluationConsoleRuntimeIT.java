package com.maxcapital.challenge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.maxcapital.challenge.adapter.in.kafka.ExecutionReportListener;
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
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class EvaluationConsoleRuntimeIT {
  @Container
  static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer(TestInfrastructure.image("postgres", "postgres"));

  @Container
  static final KafkaContainer KAFKA =
      new KafkaContainer(TestInfrastructure.image("kafka", "apache/kafka"))
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
          .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");

  @Test
  @Order(2)
  void startsWithoutDependenciesAndDoesNotRegisterProcessingOrFlyway() throws Exception {
    // GIVEN
    var application =
        new SpringApplicationBuilder(MaxCapitalChallengeApplication.class)
            .profiles("evaluation-console");

    // WHEN
    try (var app =
        application.run(
            "--server.port=0",
            "--spring.datasource.url=jdbc:postgresql://127.0.0.1:1/missing?connectTimeout=1",
            "--spring.kafka.bootstrap-servers=127.0.0.1:1")) {
      // THEN
      assertThat(app.getBeansOfType(ExecutionReportListener.class)).isEmpty();
      assertThat(app.getBeansOfType(Flyway.class)).isEmpty();
      var port = app.getEnvironment().getRequiredProperty("local.server.port");
      // WHEN
      var page = get(port, "/");

      // THEN
      assertThat(page.body()).contains("Consola de evaluación");

      // WHEN
      var readiness = get(port, "/actuator/health/readiness");

      // THEN
      assertThat(readiness.statusCode()).isEqualTo(200);
    }
  }

  @Test
  @Order(1)
  void doesNotMigrateAnEmptyDatabase() {
    // GIVEN
    var application =
        new SpringApplicationBuilder(MaxCapitalChallengeApplication.class)
            .profiles("evaluation-console");

    // WHEN
    try (var app =
        application.run(
            "--server.port=0",
            "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
            "--spring.datasource.username=" + POSTGRES.getUsername(),
            "--spring.datasource.password=" + POSTGRES.getPassword(),
            "--spring.kafka.bootstrap-servers=127.0.0.1:1")) {
      // THEN
      assertThat(
              app.getBean(JdbcTemplate.class)
                  .queryForObject(
                      "SELECT count(*) FROM information_schema.schemata WHERE schema_name='app'",
                      Long.class))
          .isZero();
    }
  }

  @Test
  @Order(3)
  void keepsExactlyTwoConsumersAndObservesStoppedListenerSeparatelyFromHttp() throws Exception {
    // GIVEN
    try (var admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
      admin
          .createTopics(List.of(new NewTopic("execution-reports", 2, (short) 1)))
          .all()
          .get(20, TimeUnit.SECONDS);
      // WHEN
      try (var first = application("first", true);
          var second = application("second", false);
          var console =
              new SpringApplicationBuilder(MaxCapitalChallengeApplication.class)
                  .profiles("evaluation-console")
                  .run(arguments("console", false))) {
        // THEN: El grupo converge a dos consumidores, con una partición por aplicación.
        await()
            .atMost(Duration.ofSeconds(45))
            .untilAsserted(
                () -> {
                  var members =
                      admin
                          .describeConsumerGroups(List.of("console-runtime-it"))
                          .all()
                          .get(5, TimeUnit.SECONDS)
                          .get("console-runtime-it")
                          .members();
                  assertThat(members).hasSize(2);
                  assertThat(members)
                      .allSatisfy(
                          member -> assertThat(member.assignment().topicPartitions()).hasSize(1));
                });
        for (int i = 0; i < 3; i++) {
          // WHEN
          var page = get(port(console), "/");

          // THEN
          assertThat(page.statusCode()).isEqualTo(200);

          // WHEN
          var consumer = get(port(first), "/internal/demo/consumer");

          // THEN
          assertThat(consumer.body()).contains("\"running\":true");
        }
        // WHEN
        var observed = get(port(first), "/internal/demo/consumer");
        var clientId =
            new tools.jackson.databind.json.JsonMapper()
                .readTree(observed.body())
                .path("clientId")
                .asString();

        // THEN
        assertThat(clientId).contains("first");

        // WHEN
        var members =
            admin
                .describeConsumerGroups(List.of("console-runtime-it"))
                .all()
                .get()
                .get("console-runtime-it")
                .members();

        // THEN
        assertThat(members).hasSize(2);

        // WHEN
        var applicationPage = get(port(first), "/");

        // THEN
        assertThat(applicationPage.statusCode()).isEqualTo(404);

        // WHEN
        var disabledObservation = get(port(second), "/internal/demo/consumer");

        // THEN
        assertThat(disabledObservation.statusCode()).isEqualTo(404);

        // WHEN
        first
            .getBean(KafkaListenerEndpointRegistry.class)
            .getListenerContainer("execution-reports")
            .stop();
        var stoppedConsumer = get(port(first), "/internal/demo/consumer");

        // THEN
        assertThat(stoppedConsumer.body()).contains("\"running\":false");

        // WHEN
        var readiness = get(port(first), "/actuator/health/readiness");

        // THEN
        assertThat(readiness.statusCode()).isEqualTo(200);
      }
    }
  }

  static ConfigurableApplicationContext application(String instance, boolean observation) {
    return new SpringApplicationBuilder(MaxCapitalChallengeApplication.class)
        .run(arguments(instance, observation));
  }

  static String[] arguments(String instance, boolean observation) {
    return new String[] {
      "--server.port=0",
      "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
      "--spring.datasource.username=" + POSTGRES.getUsername(),
      "--spring.datasource.password=" + POSTGRES.getPassword(),
      "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
      "--spring.kafka.consumer.group-id=console-runtime-it",
      "--spring.kafka.consumer.client-id=" + instance,
      "--app.instance-id=" + instance,
      "--app.demo.observation-enabled=" + observation
    };
  }

  static String port(ConfigurableApplicationContext app) {
    return app.getEnvironment().getRequiredProperty("local.server.port");
  }

  private static HttpResponse<String> get(String port, String path) throws Exception {
    try (var client = HttpClient.newHttpClient()) {
      return client.send(
          HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).build(),
          HttpResponse.BodyHandlers.ofString());
    }
  }
}
