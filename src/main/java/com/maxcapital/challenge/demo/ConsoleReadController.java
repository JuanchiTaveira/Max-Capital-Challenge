package com.maxcapital.challenge.demo;

import java.util.Map;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;

@RestController
@Profile("evaluation-console")
@RequestMapping("/demo/api")
public class ConsoleReadController {
  private final ConsoleQueries queries;
  private final InstanceAccess instances;

  public ConsoleReadController(ConsoleQueries queries, InstanceAccess instances) {
    this.queries = queries;
    this.instances = instances;
  }

  @GetMapping("/orders")
  public Map<String, Object> list(
      @RequestParam(required = false) String cursor, @RequestParam(defaultValue = "50") int limit) {
    return queries.list(cursor, limit);
  }

  @GetMapping("/orders/{id}")
  public Map<String, Object> order(@PathVariable String id) {
    return queries.order(id);
  }

  @GetMapping("/results")
  public Map<String, Object> result(
      @RequestParam String topic, @RequestParam int partition, @RequestParam String offset) {
    return queries.result(topic, partition, offset);
  }

  @GetMapping("/instances/{service}/orders/{id}")
  public Map<String, Object> instance(@PathVariable String service, @PathVariable String id) {
    return instances.order(service, id);
  }
}
