package com.maxcapital.challenge;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.yaml.snakeyaml.Yaml;
import tools.jackson.databind.json.JsonMapper;

@Testcontainers
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = "app.instance-id=integration-test")
class RuntimeInfrastructureIT {

  // Leer Compose evita que la prueba use versiones diferentes al entorno entregado.
  private static final Map<String, Object> COMPOSE = readCompose();
  private static final List<String> TOPICS =
      List.of("execution-reports", "settlements", "execution-reports-rejected");
  private static final String PROBE_TOPIC = "f01-transaction-" + UUID.randomUUID();

  @Container
  static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer(
              DockerImageName.parse(imageFor("postgres")).asCompatibleSubstituteFor("postgres"))
          .withDatabaseName("orders")
          .withUsername("orders")
          .withPassword("orders");

  @Container
  static final KafkaContainer KAFKA =
      new KafkaContainer(
              DockerImageName.parse(imageFor("kafka")).asCompatibleSubstituteFor("apache/kafka"))
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
          .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");

  @Value("${local.server.port}")
  int port;

  @Autowired JdbcTemplate jdbc;

  @DynamicPropertySource
  static void connections(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
  }

  @BeforeAll
  static void createTopics() throws Exception {
    try (var admin = admin()) {
      var topics = new ArrayList<NewTopic>();
      for (var topic : TOPICS) {
        topics.add(
            new NewTopic(topic, 2, (short) 1)
                .configs(Map.of("retention.ms", "-1", "retention.bytes", "-1")));
      }
      topics.add(new NewTopic(PROBE_TOPIC, 2, (short) 1));
      admin.createTopics(topics).all().get(20, TimeUnit.SECONDS);
    }
  }

  @Test
  void exposesHealthyProbesAndPublicIdentityWithoutConnectionDetails() throws Exception {
    var json = new JsonMapper();
    for (var probe : List.of("liveness", "readiness")) {
      var response = get("/actuator/health/" + probe);
      assertThat(response.statusCode()).isEqualTo(200);
      var body = json.readTree(response.body());
      assertThat(body.path("status").asString()).isEqualTo("UP");
      assertThat(body.has("components")).isFalse();
      assertThat(response.body()).doesNotContain("jdbc:", "password", "bootstrap.servers");
    }
    var response = get("/actuator/info");
    assertThat(response.statusCode()).isEqualTo(200);
    var app = json.readTree(response.body()).path("app");
    assertThat(app.path("name").asString()).isEqualTo("max-capital-challenge");
    assertThat(app.path("version").asString()).isEqualTo("0.1.0-SNAPSHOT");
    assertThat(app.path("instance-id").asString()).isEqualTo("integration-test");
  }

  @Test
  void connectsToPostgresAndInitializesTheFlywaySchema() {
    assertThat(jdbc.queryForObject("select 1", Integer.class)).isEqualTo(1);
    assertThat(
            jdbc.queryForObject(
                "select exists(select 1 from information_schema.schemata where schema_name = 'app')",
                Boolean.class))
        .isTrue();
    assertThat(
            jdbc.queryForObject(
                "select exists(select 1 from information_schema.tables"
                    + " where table_schema = 'app' and table_name = 'flyway_schema_history')",
                Boolean.class))
        .isTrue();
  }

  @Test
  void keepsTwoPartitionTopicsAndOnlyExposesCommittedMessages() throws Exception {
    try (var admin = admin()) {
      var descriptions = admin.describeTopics(TOPICS).allTopicNames().get(20, TimeUnit.SECONDS);
      for (var topic : TOPICS) {
        assertThat(descriptions.get(topic).partitions()).hasSize(2);
        var resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        var config =
            admin.describeConfigs(List.of(resource)).all().get(20, TimeUnit.SECONDS).get(resource);
        assertThat(config.get("retention.ms").value()).isEqualTo("-1");
        assertThat(config.get("retention.bytes").value()).isEqualTo("-1");
      }
    }

    var producerProperties = new Properties();
    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    producerProperties.put(
        ProducerConfig.TRANSACTIONAL_ID_CONFIG, "f01-probe-" + UUID.randomUUID());
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    try (var producer = new KafkaProducer<String, String>(producerProperties)) {
      producer.initTransactions();
      producer.beginTransaction();
      producer.send(new ProducerRecord<>(PROBE_TOPIC, 0, "probe", "committed")).get();
      producer.commitTransaction();
      producer.beginTransaction();
      producer.send(new ProducerRecord<>(PROBE_TOPIC, 0, "probe", "aborted")).get();
      producer.abortTransaction();
    }

    var consumerProperties = new Properties();
    consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    consumerProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProperties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    try (var consumer = new KafkaConsumer<String, String>(consumerProperties)) {
      var partition = new TopicPartition(PROBE_TOPIC, 0);
      consumer.assign(List.of(partition));
      consumer.seekToBeginning(List.of(partition));
      long end = consumer.endOffsets(List.of(partition)).get(partition);
      var values = new ArrayList<String>();
      long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
      while (consumer.position(partition) < end && System.nanoTime() < deadline) {
        consumer.poll(Duration.ofMillis(250)).forEach(record -> values.add(record.value()));
      }
      assertThat(consumer.position(partition)).isEqualTo(end);
      assertThat(values).containsExactly("committed");
    }
  }

  private HttpResponse<String> get(String path) throws Exception {
    try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
      var request =
          HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
              .timeout(Duration.ofSeconds(10))
              .GET()
              .build();
      return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
  }

  private static AdminClient admin() {
    return AdminClient.create(
        Map.of(
            "bootstrap.servers", KAFKA.getBootstrapServers(),
            "request.timeout.ms", "10000",
            "default.api.timeout.ms", "20000"));
  }

  private static String imageFor(String service) {
    var services = (Map<?, ?>) COMPOSE.get("services");
    return ((Map<?, ?>) services.get(service)).get("image").toString();
  }

  private static Map<String, Object> readCompose() {
    try {
      return new Yaml().load(Files.readString(Path.of("compose.yaml")));
    } catch (IOException exception) {
      throw new UncheckedIOException(exception);
    }
  }
}
