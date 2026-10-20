package com.maxcapital.challenge.demo;

import java.util.Map;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;

@RestController
@Profile("evaluation-console")
@RequestMapping("/demo/api")
public class ConsolePublishController {
  private final ConsoleKafka kafka;
  private final ReportPreflight preflight;
  private final DemoGate gate;

  public ConsolePublishController(ConsoleKafka kafka, ReportPreflight preflight, DemoGate gate) {
    this.kafka = kafka;
    this.preflight = preflight;
    this.gate = gate;
  }

  public record Draft(String key, String payload) {}

  public record Position(String topic, int partition, String offset) {}

  @GetMapping("/messages")
  public Map<String, Object> messages(
      @RequestParam String topic,
      @RequestParam(defaultValue = "0") int partition,
      @RequestParam(defaultValue = "0") String cursor,
      @RequestParam(defaultValue = "50") int limit) {
    return kafka.read(topic, partition, cursor, limit);
  }

  @PostMapping(value = "/reports", consumes = "application/json")
  public ConsoleKafka.Record publish(@RequestBody Draft draft) {
    return gate.manual(
        () -> {
          preflight.validate(draft.key(), draft.payload());
          return kafka.publish(draft.key(), draft.payload());
        });
  }

  @PostMapping(value = "/messages/republication", consumes = "application/json")
  public ConsoleKafka.Record republish(@RequestBody Position source) {
    return gate.manual(
        () -> {
          if (!"execution-reports".equals(source.topic()))
            throw new IllegalArgumentException("Sólo se republican ER de entrada");
          var record = kafka.exact(source.topic(), source.partition(), source.offset());
          preflight.validate(record.key(), record.payload());
          return kafka.publish(record.key(), record.payload());
        });
  }
}
