package com.maxcapital.challenge;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@Testcontainers
class RecoveryKafkaIT {
  @Container
  static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer(TestInfrastructure.image("postgres", "postgres"));

  @Container
  static final KafkaContainer KAFKA =
      new KafkaContainer(TestInfrastructure.image("kafka", "apache/kafka"))
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
          .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
          .withEnv("KAFKA_TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS", "500");

  @TempDir Path dir;

  @Test
  void observesRealSqlBoundariesExpiresWithoutSkippingAndDisablesControlsByDefault()
      throws Exception {
    try (var h = new Harness(dir)) {
      var disabled = h.start("disabled", false, false);
      assertThat(h.request(disabled, "", null).statusCode()).isEqualTo(404);
      disabled.close();
      var a = h.start("a", false, true);
      assertThat(
              h.request(
                      a,
                      "/arm",
                      Map.of(
                          "runId",
                          "invalid",
                          "numericOrderId",
                          -1,
                          "fixId",
                          1,
                          "boundary",
                          "BEFORE_SQL_COMMIT",
                          "ttlMillis",
                          1000))
                  .statusCode())
          .isEqualTo(400);
      assertThat(
              h.request(
                      a,
                      "/control",
                      Map.of("runId", "foreign", "token", "stale", "rollback", false))
                  .statusCode())
          .isEqualTo(409);
      for (int index = 0; index < 3; index++) {
        long order = 20101 + index;
        String boundary =
            List.of("BEFORE_SQL_COMMIT", "AFTER_SQL_COMMIT", "AFTER_SEND_ACK_BEFORE_KAFKA_COMMIT")
                .get(index);
        h.committed(h.send(order, 1, "NEW", 0, 10, 0));
        var arm = h.arm(a, order, 2, boundary, 3000);
        var terminal = h.send(order, 2, "FILLED", 10, 0, 0);
        h.reached(a);
        assertThat(h.results(order).size()).isEqualTo(index == 0 ? 1 : 2);
        assertThat(h.offset(0)).isLessThanOrEqualTo(terminal.offset());
        assertThat(h.output(order)).isEmpty();
        h.waitState(a, "EXPIRED");
        h.stopped(a);
        assertThat(h.results(order).size()).isEqualTo(index == 0 ? 1 : 2);
        h.listener(a).start();
        h.committed(terminal);
        h.check(order, 1, 2);
        assertThat(h.output(order)).hasSize(1);
        assertThat(arm.path("token").asString()).isNotBlank();
      }
      assertThat(h.members()).isEqualTo(1);
    }
  }

  @Test
  void killsOpenSqlDuringNewAndUpdateAndRecoversTheEntireSequence() throws Exception {
    try (var h = new Harness(dir)) {
      for (int index = 0; index < 3; index++) {
        var a = h.start("a" + index, true, true);
        var b = h.start("b" + index, false, true);
        h.twoMembers();
        int partition = 1 - h.partition(b);
        long order = 20201 + index;
        int target = index == 0 ? 1 : 2;
        boolean terminalTarget = index == 2;
        if (target == 2) h.committed(h.send(order, 1, "NEW", 0, 10, partition));
        h.arm(a, order, target, "BEFORE_SQL_COMMIT", 45000);
        var pending =
            h.send(
                order,
                target,
                terminalTarget ? "FILLED" : target == 1 ? "NEW" : "PARTIALLY_FILLED",
                terminalTarget ? 10 : target == 1 ? 0 : 4,
                terminalTarget ? 0 : target == 1 ? 10 : 6,
                partition);
        h.reached(a);
        assertThat(h.results(order)).hasSize(target - 1);
        if (target == 1)
          assertThat(
                  h.jdbc.queryForObject(
                      "SELECT count(*) FROM app.orders WHERE numeric_order_id=?",
                      Integer.class,
                      order))
              .isZero();
        var terminal =
            terminalTarget
                ? h.send(20299, 1, "NEW", 0, 10, partition)
                : h.send(order, target + 1, "FILLED", 10, 0, partition);
        h.kill(a);
        h.committed(terminal);
        h.check(order, target == 1 || terminalTarget ? new int[] {1, 2} : new int[] {1, 2, 3});
        assertThat(h.output(order)).hasSize(1);
        assertThat(h.results(order)).hasSize(terminalTarget ? target : target + 1);
        if (terminalTarget) assertThat(h.results(20299)).hasSize(1);
        a.close();
        b.close();
      }
    }
  }

  @Test
  void reassignsWithOpenSqlAndRejectsOldOwnerForCommitAndRollback() throws Exception {
    try (var h = new Harness(dir)) {
      for (boolean rollback : List.of(false, true)) {
        var a = h.start("a" + rollback, false, true);
        var b = h.start("b" + rollback, false, true);
        h.twoMembers();
        int partition = h.partition(a);
        long order = rollback ? 20302 : 20301;
        h.committed(h.send(order, 1, "NEW", 0, 10, partition));
        var armed = h.arm(a, order, 2, "BEFORE_SQL_COMMIT", 45000);
        var pending = h.send(order, 2, "PARTIALLY_FILLED", 4, 6, partition);
        var barrier = h.reached(a);
        var terminal = h.send(order, 3, "FILLED", 10, 0, partition);
        await()
            .atMost(Duration.ofSeconds(30))
            .until(
                () ->
                    h.listener(b)
                        .getAssignedPartitions()
                        .contains(new TopicPartition(h.input, partition)));
        h.control(a, armed, rollback);
        h.stopped(a);
        h.committed(terminal);
        h.check(order, 1, 2, 3);
        assertThat(h.output(order)).hasSize(1);
        assertThat(h.observe(a).path("failure").asString()).isNotBlank();
        assertThat(barrier.path("position").path("offset").asString())
            .isEqualTo(Long.toString(pending.offset()));
        if (!rollback)
          assertThat(h.observe(a).path("failure").asString()).doesNotContain("CANCELLED");
        a.close();
        b.close();
      }
    }
  }

  @Test
  void killsAfterAcknowledgedSendAndAfterBothProcessesAndResumesOnBothPartitions()
      throws Exception {
    try (var h = new Harness(dir)) {
      var a = h.start("a", true, true);
      var b = h.start("b", true, true);
      h.twoMembers();
      for (int index = 0; index < 2; index++) {
        int partition = h.assigned(a);
        long order = 20401 + index;
        h.committed(h.send(order, 1, "NEW", 0, 10, partition));
        h.arm(
            a,
            order,
            2,
            index == 0 ? "AFTER_SEND_ACK_BEFORE_KAFKA_COMMIT" : "AFTER_SQL_COMMIT",
            45000);
        var terminal = h.send(order, 2, "FILLED", 10, 0, partition);
        h.reached(a);
        var snapshot = h.results(order);
        assertThat(snapshot).hasSize(2);
        assertThat(h.output(order)).isEmpty();
        var successor = h.send(20501 + index, 1, "NEW", 0, 10, partition);
        h.kill(a);
        if (index == 1) h.kill(b);
        if (index == 0) h.committed(successor);
        a = h.start("a", true, true);
        if (index == 1) b = h.start("b", true, true);
        h.twoMembers();
        h.committed(successor);
        assertThat(h.results(order)).isEqualTo(snapshot);
        assertThat(h.output(order)).hasSize(1);
        h.check(order, 1, 2);
      }
      for (int p = 0; p < 2; p++) {
        long order = 20601 + p;
        h.committed(h.send(order, 1, "NEW", 0, 10, p));
        h.committed(h.send(order, 2, "CANCELLED", 0, 0, p));
        h.check(order, 1, 2);
        assertThat(h.output(order)).isEmpty();
      }
    }
  }

  @Test
  void quarantinesOnePartitionWhileTwoMembersKeepPollingAndRecoversInOrder() throws Exception {
    try (var h = new Harness(dir)) {
      var a = h.start("quarantine-a", false, false);
      var b = h.start("quarantine-b", false, false);
      h.twoMembers();
      int partition = h.partition(a);
      long order = routedOrder(26001, partition);
      long healthy = routedOrder(27001, 1 - partition);
      h.committed(h.send(order, 1, "NEW", 0, 10, partition));
      byte[] original = "{invalid-json".getBytes(StandardCharsets.UTF_8);
      var poison =
          h.producer
              .send(new ProducerRecord<>(h.input, partition, Long.toString(order), original))
              .get();
      var terminal = h.send(order, 3, "FILLED", 10, 0, partition);
      var initialStore =
          b.context.getBean(com.maxcapital.challenge.application.QuarantineStore.class);
      var source =
          new com.maxcapital.challenge.application.InputPosition(
              h.input, partition, poison.offset());
      await()
          .atMost(Duration.ofSeconds(20))
          .until(() -> initialStore.find(source, false).map(e -> e.notified()).orElse(false));
      h.committed(h.send(healthy, 1, "NEW", 0, 10, 1 - partition));
      h.committed(h.send(healthy, 2, "FILLED", 10, 0, 1 - partition));
      await()
          .during(Duration.ofSeconds(7))
          .atMost(Duration.ofSeconds(15))
          .untilAsserted(
              () -> {
                assertThat(h.members()).isEqualTo(2);
                assertThat(h.offset(partition)).isEqualTo(poison.offset());
                assertThat(h.results(order)).hasSize(1);
              });
      // Un nuevo propietario vuelve a detectar OPEN y pausa sólo esta partición.
      a.close();
      await()
          .atMost(Duration.ofSeconds(25))
          .until(() -> h.listener(b).isPartitionPaused(new TopicPartition(h.input, partition)));
      var notices =
          h.records(h.rejected, "rejection:" + h.input + ":" + partition + ":" + poison.offset());
      assertThat(notices).hasSize(1);
      // Simula caída después del commit DLQ y antes del marcador SQL: aviso repetible.
      h.jdbc.update(
          "UPDATE app.quarantined_reports SET dlq_notified_at=NULL WHERE topic=?", h.input);
      b.close();
      var resumed = h.start("quarantine-resumed", false, false);
      await()
          .atMost(Duration.ofSeconds(20))
          .until(
              () -> h.listener(resumed).isPartitionPaused(new TopicPartition(h.input, partition)));
      var repeated = h.records(h.rejected, notices.getFirst().key());
      assertThat(repeated).hasSize(2);
      assertThat(repeated.get(0).value()).isEqualTo(repeated.get(1).value());
      var store =
          resumed.context.getBean(com.maxcapital.challenge.application.QuarantineStore.class);
      var correction =
          new com.maxcapital.challenge.application.QuarantineStore.Correction(
              "ordered:" + h.group,
              source,
              Long.toString(order),
              TestInfrastructure.report(order, 2, "PARTIALLY_FILLED", 4, 6),
              "Reparar parcial del ensayo",
              "test",
              "ACCEPTED",
              null,
              null);
      store.accept(correction, partition);
      assertThat(h.results(order)).hasSize(1); // POST/accept sólo guarda metadata.
      resumed.close();
      h.start("quarantine-final-a", false, false);
      var finalB = h.start("quarantine-final-b", false, false);
      h.twoMembers();
      h.committed(terminal);
      h.check(order, 1, 2, 3);
      assertThat(h.output(order)).hasSize(1);
      var resolved =
          finalB
              .context
              .getBean(com.maxcapital.challenge.application.QuarantineStore.class)
              .find(source, false)
              .orElseThrow();
      assertThat(resolved.state()).isEqualTo("RESOLVED");
      assertThat(resolved.originalValue()).isEqualTo(original);
      assertThat(resolved.correction().outcome()).isEqualTo("APPLIED");
    }
  }

  @Test
  void recoversCorrectedFilledAfterKillingItsJvmFollowingSqlCommit() throws Exception {
    try (var h = new Harness(dir)) {
      var a = h.start("corrected-child", true, true);
      var b = h.start("corrected-survivor", false, true);
      h.twoMembers();
      int partition = 1 - h.partition(b);
      long order = routedOrder(28001, partition);
      h.committed(h.send(order, 1, "NEW", 0, 10, partition));
      var source =
          new com.maxcapital.challenge.application.InputPosition(
              h.input, partition, h.offset(partition));
      byte[] original = "{invalid-filled".getBytes(StandardCharsets.UTF_8);
      var store = b.context.getBean(com.maxcapital.challenge.application.QuarantineStore.class);
      store.capture(
          source,
          Long.toString(order).getBytes(StandardCharsets.UTF_8),
          original,
          "INVALID_FORMAT");
      String effective =
          TestInfrastructure.report(order, 2, "FILLED", 10, 0)
              .replaceFirst("}\\s*$", ",\"scenarioRun\":\"" + h.group + "\"}");
      store.accept(
          new com.maxcapital.challenge.application.QuarantineStore.Correction(
              "kill:" + h.group,
              source,
              Long.toString(order),
              effective,
              "Corregir FILLED",
              "test",
              "ACCEPTED",
              null,
              null),
          partition);
      h.arm(a, order, 2, "AFTER_SQL_COMMIT", 45000);
      var pending =
          h.producer
              .send(new ProducerRecord<>(h.input, partition, Long.toString(order), original))
              .get();
      assertThat(pending.offset()).isEqualTo(source.offset());
      h.reached(a);
      var snapshot = h.results(order);
      assertThat(snapshot).hasSize(2);
      assertThat(h.output(order)).isEmpty();
      assertThat(store.find(source, false).orElseThrow().state()).isEqualTo("RESOLVED");
      h.kill(a);
      h.committed(pending);
      h.check(order, 1, 2);
      assertThat(h.results(order)).isEqualTo(snapshot);
      assertThat(h.output(order))
          .singleElement()
          .satisfies(
              r -> {
                var output = h.json.readTree(r.value());
                assertThat(output.path("source").path("offset").asString())
                    .isEqualTo(Long.toString(source.offset()));
              });
      assertThat(store.corrections(source))
          .singleElement()
          .satisfies(c -> assertThat(c.outcome()).isEqualTo("APPLIED"));
    }
  }

  private static long routedOrder(long first, int partition) {
    while (org.apache.kafka.common.utils.Utils.toPositive(
                org.apache.kafka.common.utils.Utils.murmur2(
                    Long.toString(first).getBytes(StandardCharsets.UTF_8)))
            % 2
        != partition) first++;
    return first;
  }

  static final class App implements AutoCloseable {
    String id;
    int port;
    Process process;
    ConfigurableApplicationContext context;
    boolean closed;

    @Override
    public void close() {
      if (closed) return;
      closed = true;
      if (process != null && process.isAlive()) {
        process.destroyForcibly();
        try {
          assertThat(process.waitFor(15, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }
      }
      if (context != null) context.close();
    }
  }

  static final class Harness implements AutoCloseable {
    final String input = "f05-" + UUID.randomUUID();
    final String output = input + "-out";
    final String group = input;
    final String rejected = input + "-rejected";
    final Path dir;
    final List<App> apps = new ArrayList<>();
    final JsonMapper json = new JsonMapper();
    final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    final AdminClient admin =
        AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()));
    final JdbcTemplate jdbc =
        new JdbcTemplate(
            new DriverManagerDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
    final KafkaProducer<String, byte[]> producer =
        new KafkaProducer<>(
            Map.of(
                "bootstrap.servers",
                KAFKA.getBootstrapServers(),
                "key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer"));

    Harness(Path dir) throws Exception {
      this.dir = dir;
      admin
          .createTopics(
              List.of(
                  new NewTopic(input, 2, (short) 1),
                  new NewTopic(output, 2, (short) 1),
                  new NewTopic(rejected, 2, (short) 1)))
          .all()
          .get(20, TimeUnit.SECONDS);
    }

    App start(String id, boolean child, boolean enabled) throws Exception {
      var app = new App();
      app.id = id;
      var args =
          new ArrayList<>(
              List.of(
                  "--server.port=0",
                  "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                  "--spring.datasource.username=" + POSTGRES.getUsername(),
                  "--spring.datasource.password=" + POSTGRES.getPassword(),
                  "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                  "--app.instance-id=" + group + id,
                  "--app.execution-reports-topic=" + input,
                  "--app.settlements-topic=" + output,
                  "--app.rejected-topic=" + rejected,
                  "--spring.kafka.consumer.group-id=" + group,
                  "--spring.kafka.consumer.client-id=" + id,
                  "--app.demo.observation-enabled=true",
                  "--app.demo.recovery-enabled=" + enabled,
                  "--spring.kafka.consumer.properties.session.timeout.ms=6000",
                  "--spring.kafka.consumer.properties.heartbeat.interval.ms=1000",
                  "--spring.kafka.consumer.properties.max.poll.interval.ms=5000",
                  "--spring.kafka.producer.properties.transaction.timeout.ms=10000",
                  "--logging.level.root=WARN"));
      apps.add(app);
      if (!child) {
        app.context =
            new SpringApplicationBuilder(MaxCapitalChallengeApplication.class)
                .run(args.toArray(String[]::new));
        app.port =
            Integer.parseInt(app.context.getEnvironment().getRequiredProperty("local.server.port"));
      } else {
        var marker = dir.resolve(id + "-" + UUID.randomUUID() + ".ready");
        args.add("--test.ready=" + marker.toAbsolutePath());
        var launch =
            new ArrayList<String>(
                List.of(
                    "-cp",
                    System.getProperty(
                        "surefire.test.class.path", System.getProperty("java.class.path")),
                    RecoveryProcess.class.getName()));
        launch.addAll(args);
        var argfile = dir.resolve(UUID.randomUUID() + ".args");
        Files.write(argfile, launch.stream().map(v -> "\"" + v.replace('\\', '/') + "\"").toList());
        var log = dir.resolve(id + "-" + UUID.randomUUID() + ".log");
        app.process =
            new ProcessBuilder(
                    Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "@" + argfile.toAbsolutePath())
                .redirectErrorStream(true)
                .redirectOutput(log.toFile())
                .start();
        await()
            .atMost(Duration.ofSeconds(60))
            .untilAsserted(
                () -> {
                  assertThat(app.process.isAlive())
                      .withFailMessage(
                          () -> {
                            try {
                              return Files.readString(log);
                            } catch (Exception ex) {
                              return ex.toString();
                            }
                          })
                      .isTrue();
                  assertThat(Files.exists(marker)).isTrue();
                });
        app.port = Integer.parseInt(Files.readString(marker));
      }
      return app;
    }

    HttpResponse<String> request(App app, String path, Object body) throws Exception {
      var builder =
          HttpRequest.newBuilder(
                  URI.create("http://localhost:" + app.port + "/internal/demo/recovery" + path))
              .timeout(Duration.ofSeconds(3));
      if (body != null)
        builder
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(json.writeValueAsString(body)));
      return http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    JsonNode observe(App app) throws Exception {
      var response = request(app, "", null);
      assertThat(response.statusCode()).isEqualTo(200);
      return json.readTree(response.body());
    }

    JsonNode arm(App app, long order, int fix, String boundary, int ttl) throws Exception {
      var response =
          request(
              app,
              "/arm",
              Map.of(
                  "runId",
                  group,
                  "numericOrderId",
                  order,
                  "fixId",
                  fix,
                  "boundary",
                  boundary,
                  "ttlMillis",
                  ttl));
      assertThat(response.statusCode()).describedAs(response.body()).isEqualTo(200);
      return json.readTree(response.body());
    }

    void control(App app, JsonNode armed, boolean rollback) throws Exception {
      assertThat(
              request(
                      app,
                      "/control",
                      Map.of(
                          "runId",
                          group,
                          "token",
                          armed.path("token").asString(),
                          "rollback",
                          rollback))
                  .statusCode())
          .isEqualTo(200);
    }

    JsonNode reached(App app) throws Exception {
      waitState(app, "REACHED");
      var state = observe(app);
      if (app.process != null)
        assertThat(state.path("pid").asString()).isEqualTo(Long.toString(app.process.pid()));
      return state;
    }

    void waitState(App app, String state) {
      await()
          .atMost(Duration.ofSeconds(30))
          .until(() -> observe(app).path("state").asString().equals(state));
    }

    org.springframework.kafka.listener.MessageListenerContainer listener(App app) {
      return app.context
          .getBean(org.springframework.kafka.config.KafkaListenerEndpointRegistry.class)
          .getListenerContainer("execution-reports");
    }

    void stopped(App app) {
      await().atMost(Duration.ofSeconds(25)).until(() -> !listener(app).isRunning());
      listener(app).stop();
    }

    int partition(App app) {
      return listener(app).getAssignedPartitions().iterator().next().partition();
    }

    int assigned(App app) throws Exception {
      var response =
          http.send(
              HttpRequest.newBuilder(
                      URI.create("http://localhost:" + app.port + "/internal/demo/consumer"))
                  .timeout(Duration.ofSeconds(3))
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      return Integer.parseInt(
          json.readTree(response.body()).path("assignments").get(0).path("partition").asString());
    }

    void kill(App app) throws Exception {
      app.process.destroyForcibly();
      assertThat(app.process.waitFor(15, TimeUnit.SECONDS)).isTrue();
      assertThat(app.process.exitValue()).isNotZero();
      app.closed = true;
    }

    RecordMetadata send(long order, int fix, String status, int amount, int leaves, int partition)
        throws Exception {
      return producer
          .send(
              new ProducerRecord<>(
                  input,
                  partition,
                  Long.toString(order),
                  TestInfrastructure.report(order, fix, status, amount, leaves)
                      .replaceFirst("}\\s*$", ",\"scenarioRun\":\"" + group + "\"}")
                      .getBytes(StandardCharsets.UTF_8)))
          .get(15, TimeUnit.SECONDS);
    }

    long offset(int partition) throws Exception {
      var value =
          admin
              .listConsumerGroupOffsets(group)
              .partitionsToOffsetAndMetadata()
              .get(5, TimeUnit.SECONDS)
              .get(new TopicPartition(input, partition));
      return value == null ? 0 : value.offset();
    }

    void committed(RecordMetadata meta) {
      await().atMost(Duration.ofSeconds(45)).until(() -> offset(meta.partition()) > meta.offset());
    }

    int members() throws Exception {
      return admin
          .describeConsumerGroups(List.of(group))
          .all()
          .get(5, TimeUnit.SECONDS)
          .get(group)
          .members()
          .size();
    }

    void twoMembers() {
      await().atMost(Duration.ofSeconds(40)).until(() -> members() == 2);
      for (var app : apps)
        if (!app.closed)
          await()
              .atMost(Duration.ofSeconds(25))
              .until(
                  () -> {
                    try {
                      return app.context == null
                          ? assigned(app) >= 0
                          : listener(app).getAssignedPartitions().size() == 1;
                    } catch (Exception ex) {
                      return false;
                    }
                  });
    }

    List<Map<String, Object>> results(long order) {
      return jdbc.queryForList(
          "SELECT * FROM app.processing_results WHERE numeric_order_id=? ORDER BY record_offset",
          order);
    }

    void check(long order, int... fixes) {
      assertThat(
              jdbc.queryForObject(
                  "SELECT applied_execution_count FROM app.orders WHERE numeric_order_id=?",
                  Integer.class,
                  order))
          .isEqualTo(fixes.length);
      assertThat(
              jdbc.queryForList(
                  "SELECT fix_id FROM app.execution_report_ledger WHERE numeric_order_id=? ORDER BY ledger_id",
                  Long.class,
                  order))
          .containsExactly(
              java.util.Arrays.stream(fixes).mapToObj(i -> (long) i).toArray(Long[]::new));
      assertThat(results(order)).hasSize(fixes.length);
      var current =
          jdbc.queryForMap(
              "SELECT status, accumulative_nominal_amount, leaves_nominal_amount FROM app.orders WHERE numeric_order_id=?",
              order);
      var lastPayload =
          json.readTree(
              jdbc.queryForObject(
                  "SELECT report::text FROM app.execution_report_ledger WHERE numeric_order_id=? ORDER BY ledger_id DESC LIMIT 1",
                  String.class,
                  order));
      assertThat(current.get("status")).isEqualTo(lastPayload.path("status").asString());
      assertThat((java.math.BigDecimal) current.get("accumulative_nominal_amount"))
          .isEqualByComparingTo(lastPayload.path("accumulativeNominalAmount").asString());
      assertThat((java.math.BigDecimal) current.get("leaves_nominal_amount"))
          .isEqualByComparingTo(lastPayload.path("leavesNominalAmount").asString());
    }

    List<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> output(long order) {
      return records(output, Long.toString(order));
    }

    List<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> records(
        String topic, String key) {
      try (var reader =
          new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(
              Map.of(
                  "bootstrap.servers",
                  KAFKA.getBootstrapServers(),
                  "enable.auto.commit",
                  false,
                  "isolation.level",
                  "read_committed",
                  "key.deserializer",
                  "org.apache.kafka.common.serialization.StringDeserializer",
                  "value.deserializer",
                  "org.apache.kafka.common.serialization.StringDeserializer"))) {
        var parts = List.of(new TopicPartition(topic, 0), new TopicPartition(topic, 1));
        reader.assign(parts);
        reader.seekToBeginning(parts);
        var ends = reader.endOffsets(parts);
        var found =
            new ArrayList<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>>();
        await()
            .atMost(Duration.ofSeconds(20))
            .until(
                () -> {
                  reader
                      .poll(Duration.ofMillis(100))
                      .forEach(
                          r -> {
                            if (r.key().equals(key)) found.add(r);
                          });
                  return parts.stream().allMatch(p -> reader.position(p) >= ends.get(p));
                });
        return found;
      }
    }

    @Override
    public void close() {
      for (var app : apps.reversed()) app.close();
      producer.close();
      admin.close();
      http.close();
    }
  }
}
