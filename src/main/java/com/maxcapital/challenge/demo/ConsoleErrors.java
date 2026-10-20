package com.maxcapital.challenge.demo;

import java.util.Map;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.DataAccessException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ResponseStatusException;

@RestControllerAdvice(basePackages = "com.maxcapital.challenge.demo")
@Profile("evaluation-console")
public class ConsoleErrors {
  @ExceptionHandler(IllegalArgumentException.class)
  ResponseEntity<?> invalid(IllegalArgumentException ex) {
    return ResponseEntity.badRequest()
        .body(Map.of("error", ex.getMessage() == null ? "Solicitud inválida" : ex.getMessage()));
  }

  @ExceptionHandler(ResponseStatusException.class)
  ResponseEntity<?> status(ResponseStatusException ex) {
    return ResponseEntity.status(ex.getStatusCode())
        .body(Map.of("error", ex.getReason() == null ? "Solicitud rechazada" : ex.getReason()));
  }

  @ExceptionHandler(DataAccessException.class)
  ResponseEntity<?> database() {
    return ResponseEntity.status(503)
        .body(Map.of("error", "PostgreSQL no disponible para esta consulta"));
  }

  @ExceptionHandler(IllegalStateException.class)
  ResponseEntity<?> unavailable(IllegalStateException ex) {
    return ResponseEntity.status(503)
        .body(
            Map.of("error", ex.getMessage() == null ? "Servicio no disponible" : ex.getMessage()));
  }

  @ExceptionHandler(org.apache.kafka.common.KafkaException.class)
  ResponseEntity<?> broker() {
    return ResponseEntity.status(503)
        .body(Map.of("error", "Kafka no disponible para esta consulta"));
  }
}
