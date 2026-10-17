package com.maxcapital.challenge.adapter.out.kafka;

import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

@Component("kafkaHealthIndicator")
public class KafkaHealthIndicator implements HealthIndicator {

  private final AdminClient admin;

  public KafkaHealthIndicator(KafkaAdmin kafkaAdmin) {
    var properties = new HashMap<String, Object>(kafkaAdmin.getConfigurationProperties());
    properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 2000);
    properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 3000);
    this.admin = AdminClient.create(properties);
  }

  @Override
  public Health health() {
    try {
      var nodes = admin.describeCluster().nodes().get(3, TimeUnit.SECONDS);
      return nodes.isEmpty() ? Health.down().build() : Health.up().build();
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      return Health.down().build();
    } catch (ExecutionException | TimeoutException exception) {
      return Health.down().build();
    }
  }

  @PreDestroy
  void close() {
    admin.close(Duration.ofSeconds(1));
  }
}
