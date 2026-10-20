package com.maxcapital.challenge.demo;

import java.util.Map;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;

@RestController
@Profile("evaluation-console")
@RequestMapping("/demo/api")
public class ConsoleOperationsController {
  private final ConsoleSystem system;
  private final DockerAccess docker;
  private final DemoGate gate;

  public ConsoleOperationsController(ConsoleSystem system, DockerAccess docker, DemoGate gate) {
    this.system = system;
    this.docker = docker;
    this.gate = gate;
  }

  @GetMapping("/system")
  public Map<String, Object> system() {
    return system.observe();
  }

  @GetMapping("/instances/{service}/logs")
  public Map<String, Object> logs(@PathVariable String service) {
    return docker.logs(service);
  }

  @PostMapping(value = "/services/{service}/{action}", consumes = "application/json")
  public Map<String, Object> control(@PathVariable String service, @PathVariable String action) {
    return gate.manual(() -> docker.control(service, action));
  }
}
