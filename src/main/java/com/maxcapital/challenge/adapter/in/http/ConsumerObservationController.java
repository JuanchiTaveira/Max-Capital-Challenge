package com.maxcapital.challenge.adapter.in.http;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Profile("!evaluation-console")
@ConditionalOnProperty(name = "app.demo.observation-enabled", havingValue = "true")
public class ConsumerObservationController {
  private final KafkaListenerEndpointRegistry registry;
  private final String instance;

  public ConsumerObservationController(
      KafkaListenerEndpointRegistry registry, @Value("${app.instance-id}") String instance) {
    this.registry = registry;
    this.instance = instance;
  }

  @GetMapping("/internal/demo/consumer")
  public Map<String, Object> observe() {
    var container = registry.getListenerContainer("execution-reports");
    var result = new LinkedHashMap<String, Object>();
    result.put("instanceId", instance);
    result.put("observedAt", Instant.now().toString());
    result.put("running", container != null && container.isRunning());
    result.put(
        "clientId",
        container == null
            ? ""
            : String.join(",", container.metrics().keySet().stream().sorted().toList()));
    var partitions = container == null ? null : container.getAssignedPartitions();
    result.put(
        "assignments",
        partitions == null
            ? List.of()
            : partitions.stream()
                .map(p -> Map.of("topic", p.topic(), "partition", Integer.toString(p.partition())))
                .toList());
    return result;
  }
}
