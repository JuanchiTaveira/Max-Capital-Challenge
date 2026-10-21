package com.maxcapital.challenge;

import static com.maxcapital.challenge.TestInfrastructure.report;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.maxcapital.challenge.adapter.in.report.ExecutionReportParser;
import com.maxcapital.challenge.adapter.out.kafka.KafkaSettlementPublisher;
import com.maxcapital.challenge.adapter.out.kafka.SettlementJson;
import com.maxcapital.challenge.application.InputPosition;
import com.maxcapital.challenge.application.ProcessExecutionReport;
import com.maxcapital.challenge.application.Settlement;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
class SettlementKafkaIT {
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

  @Test
  void publishesOnePerFilledWithTwoMembersAndNoCancellationOrLateDuplicateOutput()
      throws Exception {
    try (var h = new Harness("normal")) {
      var a = h.start("a", null);
      var b = h.start("b", null);
      h.twoMembers();
      h.send(10001, 1, "NEW", 0, 10, 0);
      h.send(10002, 1, "NEW", 0, 10, 1);
      h.send(10003, 1, "NEW", 0, 10, 0);
      h.send(10001, 2, "PARTIALLY_FILLED", 3, 7, 0);
      h.send(10002, 2, "PARTIALLY_FILLED", 7, 3, 1);
      h.send(10003, 2, "PARTIALLY_FILLED", 4, 6, 0);
      var filled = h.send(10001, 3, "FILLED", 10, 0, 0);
      var filledB = h.send(10002, 3, "FILLED", 10, 0, 1);
      h.send(10003, 3, "CANCELLED", 4, 0, 0);
      h.send(10001, 3, "FILLED", 10, 0, 0);
      h.send(10001, 1, "NEW", 0, 10, 0);
      var last = h.send(10003, 3, "CANCELLED", 4, 0, 0);
      h.committed(last);
      h.committed(filledB);
      assertThat(listener(a).getAssignedPartitions()).hasSize(1);
      assertThat(listener(b).getAssignedPartitions()).hasSize(1);
      assertThat(h.output()).hasSize(2);
      assertThat(h.outputFor(10003)).isEmpty();
      var original = a.getBean(ProcessExecutionReport.class);
      var payload = report(10001, 3, "FILLED", 10, 0);
      var parsed =
          new ExecutionReportParser().parse("10001", payload.getBytes(StandardCharsets.UTF_8));
      var recovered =
          original.process(
              new InputPosition(h.input, 0, filled.offset()), parsed.report(), parsed.payload());
      assertThat(h.outputFor(10001).getFirst().value().getBytes(StandardCharsets.UTF_8))
          .isEqualTo(SettlementJson.encode(Settlement.from(recovered).orElseThrow()));
      h.count(10001, 3);
      h.count(10002, 3);
      h.count(10003, 3);
      assertThat(h.resultCount(10001)).isEqualTo(5);
      assertThat(h.members()).isEqualTo(2);
      assertThatThrownBy(
              () ->
                  a.getBean(KafkaSettlementPublisher.class)
                      .publish(Settlement.from(recovered).orElseThrow()))
          .isInstanceOf(IllegalStateException.class);
    }
  }

  @Test
  void keepsAcknowledgedSendInvisibleUntilAbortAndRecoversOriginalResult() throws Exception {
    long order = 10101;
    try (var h = new Harness("abort")) {
      var probe = h.probe(order, TransactionProbe.Mode.AFTER_SEND_ABORT);
      var app = h.start("a", probe);
      h.committed(h.send(order, 1, "NEW", 0, 10, 0));
      var terminal = h.send(order, 2, "FILLED", 10, 0, 0);
      assertThat(probe.entered.await(30, TimeUnit.SECONDS)).isTrue();
      h.count(order, 2);
      var snapshot = h.results(order);
      assertThat(h.offset(0)).isLessThanOrEqualTo(terminal.offset());
      assertThat(h.output()).isEmpty();
      probe.release.countDown();
      h.stopped(app);
      assertThat(probe.failures.get()).isEqualTo(1);
      listener(app).start();
      h.committed(terminal);
      h.count(order, 2);
      assertThat(h.results(order)).isEqualTo(snapshot);
      assertThat(h.outputFor(order)).hasSize(1);
      assertThat(probe.sends.get()).isEqualTo(2);
    }
  }

  @Test
  @org.junit.jupiter.api.extension.ExtendWith(
      org.springframework.boot.test.system.OutputCaptureExtension.class)
  void resumesAfterActualCommitWithLostResponseWithoutResending(
      org.springframework.boot.test.system.CapturedOutput captured) throws Exception {
    long order = 10201;
    try (var h = new Harness("lost-reply")) {
      var probe = h.probe(order, TransactionProbe.Mode.LOST_COMMIT_REPLY);
      var app = h.start("a", probe);
      h.committed(h.send(order, 1, "NEW", 0, 10, 0));
      var terminal = h.send(order, 2, "FILLED", 10, 0, 0);
      assertThat(probe.entered.await(30, TimeUnit.SECONDS)).isTrue();
      h.committed(terminal);
      assertThat(h.outputFor(order)).hasSize(1);
      var original = h.results(order);
      var next = h.send(10202, 1, "NEW", 0, 10, 0);
      probe.release.countDown();
      h.stopped(app);
      assertThat(probe.failures.get()).isEqualTo(1);
      assertThat(captured.getOut())
          .contains(
              "ER pendiente; transacción Kafka fallida o incierta",
              "topic=" + h.input,
              "controlled LOST_COMMIT_REPLY");
      await()
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(() -> assertThat(probe.closes.get()).isPositive());
      assertThat(h.resultCount(10202)).isZero();
      listener(app).start();
      h.committed(next);
      assertThat(h.results(order)).isEqualTo(original);
      assertThat(h.outputFor(order)).hasSize(1);
      assertThat(probe.sends.get()).isEqualTo(1);
    }
  }

  @Test
  void waitsForPendingTransactionalOffsetsThenReplaysAfterResolution() throws Exception {
    long order = 10301;
    try (var h = new Harness("pending")) {
      var probe = h.probe(order, TransactionProbe.Mode.PENDING_COMMIT);
      var app = h.start("a", probe);
      h.committed(h.send(order, 1, "NEW", 0, 10, 0));
      var terminal = h.send(order, 2, "FILLED", 10, 0, 0);
      assertThat(probe.entered.await(30, TimeUnit.SECONDS)).isTrue();
      var stable =
          java.util.concurrent.CompletableFuture.supplyAsync(
              () -> {
                try (var reader = h.reader(h.input, h.group)) {
                  return reader.committed(
                      java.util.Set.of(new TopicPartition(h.input, 0)), Duration.ofSeconds(30));
                }
              });
      await()
          .during(Duration.ofMillis(300))
          .atMost(Duration.ofSeconds(2))
          .until(() -> !stable.isDone());
      assertThat(h.output()).isEmpty();
      probe.release.countDown();
      h.stopped(app);
      listener(app).start();
      h.committed(terminal);
      assertThat(stable.get(30, TimeUnit.SECONDS)).isNotNull();
      assertThat(probe.failures.get()).isEqualTo(1);
      assertThat(probe.sends.get()).isEqualTo(2);
      h.count(order, 2);
      assertThat(h.outputFor(order)).hasSize(1);
    }
  }

  @Test
  void fencesOldOwnerAfterRealPollTimeoutAndReassignment() throws Exception {
    long order = 10401;
    try (var h = new Harness("rebalance")) {
      var probe = h.probe(order, TransactionProbe.Mode.BEFORE_SEND);
      var a = h.start("a", probe, "--spring.kafka.consumer.properties.max.poll.interval.ms=5000");
      var b = h.start("b", null);
      h.twoMembers();
      int partition = listener(a).getAssignedPartitions().iterator().next().partition();
      h.committed(h.send(order, 1, "NEW", 0, 10, partition));
      var terminal = h.send(order, 2, "FILLED", 10, 0, partition);
      assertThat(probe.entered.await(30, TimeUnit.SECONDS)).isTrue();
      var snapshot = h.results(order);
      await()
          .atMost(Duration.ofSeconds(45))
          .untilAsserted(
              () ->
                  assertThat(listener(b).getAssignedPartitions())
                      .contains(new TopicPartition(h.input, partition)));
      h.committed(terminal);
      probe.release.countDown();
      h.stopped(a);
      assertThat(probe.once.get()).isTrue();
      assertThat(h.results(order)).isEqualTo(snapshot);
      h.count(order, 2);
      assertThat(h.outputFor(order)).hasSize(1);
    }
  }

  @Test
  void activatesPendingF03CompletionWithoutBackfillingAcknowledgedHistory() throws Exception {
    try (var h = new Harness("activation")) {
      var app = h.startPaused("a", null);
      var processor = app.getBean(ProcessExecutionReport.class);
      for (int partition = 0; partition < 2; partition++) {
        long order = 10501 + partition;
        for (int fix = 1; fix <= 2; fix++) {
          var payload =
              report(order, fix, fix == 1 ? "NEW" : "FILLED", fix == 1 ? 0 : 10, fix == 1 ? 10 : 0);
          var meta = h.sendPayload(order, payload, partition);
          var parsed =
              new ExecutionReportParser()
                  .parse(Long.toString(order), payload.getBytes(StandardCharsets.UTF_8));
          processor.process(
              new InputPosition(h.input, partition, meta.offset()),
              parsed.report(),
              parsed.payload());
          if (partition == 1 && fix == 2) {
            try (var old = h.reader(h.input, h.group)) {
              old.assign(List.of(new TopicPartition(h.input, 1)));
              old.commitSync(
                  Map.of(new TopicPartition(h.input, 1), new OffsetAndMetadata(meta.offset() + 1)));
            }
          }
        }
      }
      var pending = h.results(10501);
      var historic = h.results(10502);
      listener(app).start();
      var barrier = h.send(10502, 2, "FILLED", 10, 0, 1);
      h.committed(barrier);
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(() -> assertThat(h.offset(0)).isEqualTo(2));
      assertThat(h.outputFor(10501)).hasSize(1);
      assertThat(h.outputFor(10502)).isEmpty();
      assertThat(h.results(10501)).isEqualTo(pending);
      assertThat(h.results(10502).subList(0, 2)).isEqualTo(historic);
      h.count(10501, 2);
      h.count(10502, 2);
    }
  }

  @Test
  void recoversAfterKillingTheProcessBetweenSqlAndPublication(
      @org.junit.jupiter.api.io.TempDir Path dir) throws Exception {
    long order = 10601;
    try (var h = new Harness("kill")) {
      var barrier = dir.resolve("after-sql");
      var log = dir.resolve("child.log");
      var args = new ArrayList<String>();
      args.add("-cp");
      args.add(
          System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")));
      args.add(SettlementProcess.class.getName());
      args.addAll(h.arguments("a"));
      args.add("--test.order=" + order);
      args.add("--test.barrier=" + barrier.toAbsolutePath());
      var argfile = dir.resolve("child.args");
      Files.write(argfile, args.stream().map(a -> "\"" + a.replace('\\', '/') + "\"").toList());
      var child =
          new ProcessBuilder(
                  Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                  "@" + argfile.toAbsolutePath())
              .redirectErrorStream(true)
              .redirectOutput(log.toFile())
              .start();
      h.children.add(child);
      var b = h.start("b", null);
      h.twoMembers();
      int childPartition = 1 - listener(b).getAssignedPartitions().iterator().next().partition();
      h.committed(h.send(order, 1, "NEW", 0, 10, childPartition));
      var terminal = h.send(order, 2, "FILLED", 10, 0, childPartition);
      await()
          .atMost(Duration.ofSeconds(45))
          .untilAsserted(
              () -> {
                assertThat(child.isAlive())
                    .withFailMessage(
                        () -> {
                          try {
                            return Files.readString(log);
                          } catch (Exception e) {
                            return e.toString();
                          }
                        })
                    .isTrue();
                assertThat(Files.exists(barrier)).isTrue();
              });
      assertThat(Long.parseLong(Files.readString(barrier))).isEqualTo(child.pid());
      var snapshot = h.results(order);
      h.count(order, 2);
      assertThat(h.output()).isEmpty();
      child.destroyForcibly();
      assertThat(child.waitFor(15, TimeUnit.SECONDS)).isTrue();
      assertThat(child.exitValue()).isNotZero();
      h.committed(terminal);
      h.start("a", null);
      h.twoMembers();
      assertThat(h.results(order)).isEqualTo(snapshot);
      h.count(order, 2);
      assertThat(h.outputFor(order)).hasSize(1);
    }
  }

  @Test
  void preservesSqlAndOffsetWhenSettlementSendFails() throws Exception {
    long order = 10701;
    try (var h = new Harness("send-failure")) {
      var probe = h.probe(order, TransactionProbe.Mode.SEND_FAILURE);
      var app = h.start("a", probe);
      h.committed(h.send(order, 1, "NEW", 0, 10, 0));
      var terminal = h.send(order, 2, "FILLED", 10, 0, 0);
      h.stopped(app);
      assertThat(probe.failures.get()).isEqualTo(1);
      h.count(order, 2);
      var snapshot = h.results(order);
      assertThat(h.offset(0)).isLessThanOrEqualTo(terminal.offset());
      assertThat(h.output()).isEmpty();
      listener(app).start();
      h.committed(terminal);
      assertThat(h.results(order)).isEqualTo(snapshot);
      assertThat(h.outputFor(order)).hasSize(1);
    }
  }

  @Test
  void rollsBackTerminalSqlBeforeAnyOutputAndResumesWithoutSkipping() throws Exception {
    long order = 10801;
    try (var h = new Harness("sql-failure")) {
      var app = h.start("a", null);
      h.committed(h.send(order, 1, "NEW", 0, 10, 0));
      h.jdbc.execute(
          "CREATE FUNCTION app.fail_terminal() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.numeric_order_id=10801 AND NEW.fix_id=2 THEN RAISE EXCEPTION 'controlled SQL rollback'; END IF; RETURN NEW; END $$");
      h.jdbc.execute(
          "CREATE TRIGGER fail_terminal BEFORE INSERT ON app.processing_results FOR EACH ROW EXECUTE FUNCTION app.fail_terminal()");
      RecordMetadata terminal;
      try {
        terminal = h.send(order, 2, "FILLED", 10, 0, 0);
        h.stopped(app);
        h.count(order, 1);
        assertThat(h.resultCount(order)).isEqualTo(1);
        assertThat(h.offset(0)).isLessThanOrEqualTo(terminal.offset());
        assertThat(h.output()).isEmpty();
      } finally {
        h.jdbc.execute("DROP TRIGGER fail_terminal ON app.processing_results");
        h.jdbc.execute("DROP FUNCTION app.fail_terminal()");
      }
      listener(app).start();
      h.committed(terminal);
      h.count(order, 2);
      assertThat(h.outputFor(order)).hasSize(1);
    }
  }

  static MessageListenerContainer listener(ConfigurableApplicationContext app) {
    return app.getBean(KafkaListenerEndpointRegistry.class)
        .getListenerContainer("execution-reports");
  }

  static final class Harness implements AutoCloseable {
    final String input, output, group;
    final AdminClient admin =
        AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()));
    final JdbcTemplate jdbc =
        new JdbcTemplate(
            new DriverManagerDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
    final List<ConfigurableApplicationContext> apps = new ArrayList<>();
    final List<TransactionProbe> probes = new ArrayList<>();
    final List<Process> children = new ArrayList<>();
    final KafkaProducer<String, byte[]> producer =
        new KafkaProducer<>(
            Map.of(
                "bootstrap.servers", KAFKA.getBootstrapServers(),
                "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer"));

    Harness(String name) throws Exception {
      input = "f04-" + name + "-input";
      output = "f04-" + name + "-output";
      group = "f04-" + name;
      admin
          .createTopics(
              List.of(new NewTopic(input, 2, (short) 1), new NewTopic(output, 2, (short) 1)))
          .all()
          .get(20, TimeUnit.SECONDS);
    }

    TransactionProbe probe(long order, TransactionProbe.Mode mode) {
      var probe = new TransactionProbe(order, mode);
      probes.add(probe);
      return probe;
    }

    ConfigurableApplicationContext start(String id, TransactionProbe probe, String... extra) {
      var app = startPaused(id, probe, extra);
      listener(app).start();
      return app;
    }

    @SuppressWarnings("unchecked")
    ConfigurableApplicationContext startPaused(String id, TransactionProbe probe, String... extra) {
      var args = arguments(id);
      args.addAll(List.of(extra));
      return createApplication(args, probe);
    }

    ArrayList<String> arguments(String id) {
      return new ArrayList<>(
          List.of(
              "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
              "--spring.datasource.username=" + POSTGRES.getUsername(),
              "--spring.datasource.password=" + POSTGRES.getPassword(),
              "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
              "--app.instance-id=" + group + "-" + id,
              "--app.execution-reports-topic=" + input,
              "--app.settlements-topic=" + output,
              "--spring.kafka.consumer.group-id=" + group,
              "--spring.kafka.listener.auto-startup=false",
              "--spring.kafka.consumer.properties.session.timeout.ms=6000",
              "--spring.kafka.consumer.properties.heartbeat.interval.ms=1000",
              "--spring.kafka.producer.properties.transaction.timeout.ms=10000",
              "--logging.level.root=WARN"));
    }

    @SuppressWarnings("unchecked")
    ConfigurableApplicationContext createApplication(List<String> args, TransactionProbe probe) {
      var builder =
          new SpringApplicationBuilder(MaxCapitalChallengeApplication.class)
              .web(WebApplicationType.NONE);
      if (probe != null)
        builder.initializers(
            ctx ->
                ctx.getBeanFactory()
                    .addBeanPostProcessor(
                        new BeanPostProcessor() {
                          @Override
                          public Object postProcessAfterInitialization(Object bean, String name) {
                            if (bean instanceof DefaultKafkaProducerFactory<?, ?> factory)
                              ((DefaultKafkaProducerFactory<Object, Object>) factory)
                                  .addPostProcessor(probe);
                            return bean;
                          }
                        }));
      var app = builder.run(args.toArray(String[]::new));
      apps.add(app);
      return app;
    }

    RecordMetadata send(
        long order, long fix, String status, int accumulated, int leaves, int partition)
        throws Exception {
      return sendPayload(order, report(order, fix, status, accumulated, leaves), partition);
    }

    RecordMetadata sendPayload(long order, String payload, int partition) throws Exception {
      return producer
          .send(
              new ProducerRecord<>(
                  input, partition, Long.toString(order), payload.getBytes(StandardCharsets.UTF_8)))
          .get(20, TimeUnit.SECONDS);
    }

    void committed(RecordMetadata meta) {
      await()
          .atMost(Duration.ofSeconds(45))
          .untilAsserted(() -> assertThat(offset(meta.partition())).isGreaterThan(meta.offset()));
    }

    long offset(int partition) throws Exception {
      var offsets =
          admin
              .listConsumerGroupOffsets(group)
              .partitionsToOffsetAndMetadata()
              .get(5, TimeUnit.SECONDS);
      var value = offsets.get(new TopicPartition(input, partition));
      return value == null ? 0 : value.offset();
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
      await()
          .atMost(Duration.ofSeconds(40))
          .untilAsserted(() -> assertThat(members()).isEqualTo(2));
      for (var app : apps)
        await()
            .atMost(Duration.ofSeconds(30))
            .untilAsserted(() -> assertThat(listener(app).getAssignedPartitions()).hasSize(1));
    }

    void stopped(ConfigurableApplicationContext app) {
      await().atMost(Duration.ofSeconds(30)).until(() -> !listener(app).isRunning());
      listener(app).stop();
    }

    void count(long order, long count) {
      assertThat(
              jdbc.queryForObject(
                  "SELECT applied_execution_count FROM app.orders WHERE numeric_order_id=?",
                  Long.class,
                  order))
          .isEqualTo(count);
      assertThat(
              jdbc.queryForObject(
                  "SELECT count(*) FROM app.execution_report_ledger WHERE numeric_order_id=?",
                  Long.class,
                  order))
          .isEqualTo(count);
    }

    int resultCount(long order) {
      return jdbc.queryForObject(
          "SELECT count(*) FROM app.processing_results WHERE numeric_order_id=?",
          Integer.class,
          order);
    }

    List<Map<String, Object>> results(long order) {
      return jdbc.queryForList(
          "SELECT * FROM app.processing_results WHERE numeric_order_id=? ORDER BY topic, partition_id, record_offset",
          order);
    }

    KafkaConsumer<String, String> reader(String topic, String readerGroup) {
      return new KafkaConsumer<>(
          Map.of(
              "bootstrap.servers",
              KAFKA.getBootstrapServers(),
              "group.id",
              readerGroup,
              "enable.auto.commit",
              false,
              "isolation.level",
              "read_committed",
              "key.deserializer",
              "org.apache.kafka.common.serialization.StringDeserializer",
              "value.deserializer",
              "org.apache.kafka.common.serialization.StringDeserializer"));
    }

    List<ConsumerRecord<String, String>> output() {
      try (var reader = reader(output, group + "-observer")) {
        var partitions = List.of(new TopicPartition(output, 0), new TopicPartition(output, 1));
        reader.assign(partitions);
        reader.seekToBeginning(partitions);
        var end = reader.endOffsets(partitions);
        var records = new ArrayList<ConsumerRecord<String, String>>();
        await()
            .atMost(Duration.ofSeconds(20))
            .until(
                () -> {
                  reader.poll(Duration.ofMillis(100)).forEach(records::add);
                  return partitions.stream().allMatch(p -> reader.position(p) >= end.get(p));
                });
        return records;
      }
    }

    List<ConsumerRecord<String, String>> outputFor(long order) {
      return output().stream().filter(r -> r.key().equals(Long.toString(order))).toList();
    }

    @Override
    public void close() {
      probes.forEach(p -> p.release.countDown());
      for (var child : children) {
        if (child.isAlive()) child.destroyForcibly();
        try {
          child.waitFor(15, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      for (var app : apps.reversed()) app.close();
      producer.close();
      admin.close();
    }
  }
}
