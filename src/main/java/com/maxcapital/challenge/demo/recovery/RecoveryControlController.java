package com.maxcapital.challenge.demo.recovery;

import java.util.Map;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Profile("!evaluation-console")
@ConditionalOnProperty(name = "app.demo.recovery-enabled", havingValue = "true")
@RequestMapping("/internal/demo/recovery")
public class RecoveryControlController {
  private final RecoveryBarrier barrier;

  public RecoveryControlController(RecoveryBarrier barrier) {
    this.barrier = barrier;
  }

  @GetMapping
  public Map<String, Object> observe() {
    return barrier.observe();
  }

  @PostMapping("/arm")
  public Map<String, Object> arm(@RequestBody RecoveryBarrier.Arm request) {
    return barrier.arm(request);
  }

  @PostMapping("/control")
  public Map<String, Object> control(@RequestBody RecoveryBarrier.Control request) {
    return barrier.control(request);
  }

  @ExceptionHandler(IllegalArgumentException.class)
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  public Map<String, String> invalid(IllegalArgumentException exception) {
    return Map.of("error", exception.getMessage());
  }
}
