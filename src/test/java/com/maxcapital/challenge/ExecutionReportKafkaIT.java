package com.maxcapital.challenge;

import static com.maxcapital.challenge.TestInfrastructure.report;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@SpringBootTest(
    properties = {
      "app.execution-reports-topic=f02-it",
      "spring.kafka.consumer.group-id=f02-it",
      "spring.kafka.listener.auto-startup=false"
    })
class ExecutionReportKafkaIT {
  private static final String TOPIC = "f02-it";

  @Container
  static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer(TestInfrastructure.image("postgres", "postgres"));

  @Container
  static final KafkaContainer KAFKA =
      new KafkaContainer(TestInfrastructure.image("kafka", "apache/kafka"))
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
          .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");

  @DynamicPropertySource
  static void connections(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
  }

  @Autowired JdbcTemplate jdbc;
  @Autowired KafkaListenerEndpointRegistry registry;

  @BeforeAll
  static void topics() throws Exception {
    try (var admin = admin()) {
      admin
          .createTopics(List.of(new NewTopic(TOPIC, 2, (short) 1)))
          .all()
          .get(20, TimeUnit.SECONDS);
    }
  }

  @BeforeEach
  void start() {
    listener().start();
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(() -> assertThat(listener().getAssignedPartitions()).hasSize(2));
  }

  @Test
  @Order(1)
  void processesInterleavedOrdersAndReplayAndIgnoresAbortedReports() throws Exception {
    long a = orderForPartition(200, 0);
    long b = orderForPartition(300, 1);
    try (var producer = producer(false)) {
      assertThat(send(producer, a, report(a, 1, "NEW", 0, 10)).partition()).isZero();
      assertThat(send(producer, b, report(b, 1, "NEW", 0, 10)).partition()).isEqualTo(1);
      send(producer, a, report(a, 2, "PARTIALLY_FILLED", 3, 7));
      send(producer, b, report(b, 2, "PARTIALLY_FILLED", 7, 3));
      send(producer, a, report(a, 3, "PARTIALLY_FILLED", 7, 3));
      send(producer, b, report(b, 3, "FILLED", 10, 0));
      var terminal = report(a, 4, "FILLED", 10, 0);
      send(producer, a, terminal);
      var last = send(producer, a, terminal);
      await()
          .atMost(Duration.ofSeconds(20))
          .untilAsserted(
              () -> {
                assertThat(count(a)).isEqualTo(4);
                assertThat(count(b)).isEqualTo(3);
                assertThat(committed(last.partition())).isGreaterThan(last.offset());
              });
      assertThat(
              jdbc.queryForList(
                  "SELECT fix_id FROM app.execution_report_ledger WHERE numeric_order_id=? ORDER BY ledger_id",
                  Long.class,
                  a))
          .containsExactly(1L, 2L, 3L, 4L);
    }
    try (var producer = producer(true)) {
      producer.initTransactions();
      producer.beginTransaction();
      send(producer, 9999, report(9999, 1, "NEW", 0, 10));
      producer.abortTransaction();
    }
    try (var producer = producer(false)) {
      var barrier = send(producer, 9999, report(9999, 2, "NEW", 0, 10));
      await()
          .atMost(Duration.ofSeconds(20))
          .untilAsserted(
              () -> assertThat(committed(barrier.partition())).isGreaterThan(barrier.offset()));
    }
    assertThat(count(9999)).isEqualTo(1);
    assertThat(
            jdbc.queryForList(
                "SELECT fix_id FROM app.execution_report_ledger WHERE numeric_order_id=9999",
                Long.class))
        .containsExactly(2L);
  }

  @Test
  @Order(2)
  void keepsSqlFailureUncommittedAndResumesTheSameSequenceOnRestart() throws Exception {
    try (var producer = producer(false)) {
      var first = send(producer, 701, report(701, 1, "NEW", 0, 10));
      await()
          .atMost(Duration.ofSeconds(20))
          .untilAsserted(
              () -> assertThat(committed(first.partition())).isGreaterThan(first.offset()));
      jdbc.execute(
          """
          CREATE FUNCTION app.fail_kafka_append() RETURNS trigger LANGUAGE plpgsql AS $$
          BEGIN IF NEW.numeric_order_id = 701 AND NEW.fix_id = 2 THEN
          RAISE EXCEPTION 'simulated SQL outage'; END IF; RETURN NEW; END $$
          """);
      jdbc.execute(
          "CREATE TRIGGER fail_kafka_append BEFORE INSERT ON app.execution_report_ledger FOR EACH ROW EXECUTE FUNCTION app.fail_kafka_append()");
      try {
        var failed = send(producer, 701, report(701, 2, "PARTIALLY_FILLED", 7, 3));
        send(producer, 701, report(701, 3, "FILLED", 10, 0));
        awaitStopped();
        assertThat(count(701)).isEqualTo(1);
        assertThat(committed(failed.partition())).isEqualTo(failed.offset());
      } finally {
        jdbc.execute("DROP TRIGGER fail_kafka_append ON app.execution_report_ledger");
        jdbc.execute("DROP FUNCTION app.fail_kafka_append()");
      }
      start();
      await()
          .atMost(Duration.ofSeconds(20))
          .untilAsserted(() -> assertThat(count(701)).isEqualTo(3));
      assertThat(
              jdbc.queryForList(
                  "SELECT fix_id FROM app.execution_report_ledger WHERE numeric_order_id=701 ORDER BY ledger_id",
                  Long.class))
          .containsExactly(1L, 2L, 3L);
    }
  }

  @Test
  @Order(3)
  void preservesPoisonRecordAndDoesNotSkipToTheNextRecord() throws Exception {
    try (var producer = producer(false)) {
      var poison = send(producer, 801, "{invalid-json");
      send(producer, 801, report(801, 1, "NEW", 0, 10));
      awaitStopped();
      long offset = committed(poison.partition());
      assertThat(offset).isLessThanOrEqualTo(poison.offset());
      assertThat(count(801)).isZero();
      listener().start();
      awaitStopped();
      assertThat(committed(poison.partition())).isEqualTo(offset);
      assertThat(count(801)).isZero();
      var properties = new Properties();
      properties.put("bootstrap.servers", KAFKA.getBootstrapServers());
      properties.put(
          "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      properties.put(
          "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      properties.put("enable.auto.commit", false);
      properties.put("isolation.level", "read_committed");
      try (var reader = new KafkaConsumer<String, String>(properties)) {
        var partition = new TopicPartition(TOPIC, poison.partition());
        reader.assign(List.of(partition));
        reader.seek(partition, poison.offset());
        await()
            .atMost(Duration.ofSeconds(15))
            .untilAsserted(
                () -> {
                  var records = reader.poll(Duration.ofMillis(200));
                  assertThat(records).isNotEmpty();
                  assertThat(records.iterator().next().value()).isEqualTo("{invalid-json");
                });
      }
    }
  }

  private MessageListenerContainer listener() {
    return registry.getListenerContainer("execution-reports");
  }

  private void awaitStopped() {
    await().atMost(Duration.ofSeconds(20)).until(() -> !listener().isRunning());
  }

  private long count(long order) {
    return jdbc.queryForObject(
        "SELECT coalesce(sum(applied_execution_count),0) FROM app.orders WHERE numeric_order_id=?",
        Long.class,
        order);
  }

  private static long orderForPartition(long start, int partition) {
    while (Utils.toPositive(Utils.murmur2(Long.toString(start).getBytes(StandardCharsets.UTF_8)))
            % 2
        != partition) {
      start++;
    }
    return start;
  }

  private static RecordMetadata send(
      KafkaProducer<String, byte[]> producer, long order, String payload) throws Exception {
    return producer
        .send(
            new ProducerRecord<>(
                TOPIC, Long.toString(order), payload.getBytes(StandardCharsets.UTF_8)))
        .get(15, TimeUnit.SECONDS);
  }

  private static KafkaProducer<String, byte[]> producer(boolean transactional) {
    var properties = new Properties();
    properties.put("bootstrap.servers", KAFKA.getBootstrapServers());
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    if (transactional) properties.put("transactional.id", "f02-aborted-producer");
    return new KafkaProducer<>(properties);
  }

  private static AdminClient admin() {
    return AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()));
  }

  private static long committed(int partition) throws Exception {
    try (var admin = admin()) {
      var offsets =
          admin
              .listConsumerGroupOffsets(TOPIC)
              .partitionsToOffsetAndMetadata()
              .get(10, TimeUnit.SECONDS);
      var offset = offsets.get(new TopicPartition(TOPIC, partition));
      return offset == null ? 0 : offset.offset();
    }
  }
}
