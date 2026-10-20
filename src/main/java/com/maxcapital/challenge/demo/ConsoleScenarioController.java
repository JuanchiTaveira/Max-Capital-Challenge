package com.maxcapital.challenge.demo;

import java.util.List;
import java.util.Map;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;

@RestController
@Profile("evaluation-console")
@RequestMapping("/demo/api")
public class ConsoleScenarioController {
  private final ScenarioCatalog catalog;
  private final ScenarioRunner runs;

  public ConsoleScenarioController(ScenarioCatalog catalog, ScenarioRunner runs) {
    this.catalog = catalog;
    this.runs = runs;
  }

  @GetMapping("/catalog")
  public List<ScenarioCatalog.Entry> catalog() {
    return catalog.entries();
  }

  public record Start(String scenario, String mode) {}

  @PostMapping(value = "/runs", consumes = "application/json")
  public Map<String, Object> start(@RequestBody Start request) {
    return runs.start(request.scenario(), request.mode());
  }

  @GetMapping("/runs/{id}")
  public Map<String, Object> get(@PathVariable String id) {
    return runs.get(id);
  }

  @PostMapping(value = "/runs/{id}/next", consumes = "application/json")
  public Map<String, Object> next(@PathVariable String id) {
    return runs.next(id);
  }

  @PostMapping(value = "/runs/{id}/cancel", consumes = "application/json")
  public Map<String, Object> cancel(@PathVariable String id) {
    return runs.cancel(id);
  }
}
