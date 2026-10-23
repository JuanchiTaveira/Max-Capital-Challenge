package com.maxcapital.challenge.adapter.in.http;

import com.maxcapital.challenge.adapter.in.report.ExecutionReportParser;
import com.maxcapital.challenge.application.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.utils.Utils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@RestController
public class QuarantineController {
  private final QuarantineStore store;
  private final ExecutionReportParser parser;
  private final String bootstrap;
  private final JsonMapper json =
      JsonMapper.builder()
          .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
          .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
          .build();

  public QuarantineController(
      QuarantineStore store,
      ExecutionReportParser parser,
      @Value("${spring.kafka.bootstrap-servers}") String bootstrap) {
    this.store = store;
    this.parser = parser;
    this.bootstrap = bootstrap;
  }

  @GetMapping("/quarantine")
  public Map<String, Object> get(
      @RequestParam String topic, @RequestParam int partition, @RequestParam long offset) {
    try {
      var p = new InputPosition(topic, partition, offset);
      var e =
          store
              .find(p, false)
              .orElseThrow(() -> new NoSuchElementException("Posición sin cuarentena"));
      var result = new LinkedHashMap<String, Object>();
      result.put("source", position(p));
      result.put("state", e.state());
      result.put("causeCode", e.causeCode());
      result.put("originalKeyBase64", base64(e.originalKey()));
      result.put("originalValueBase64", base64(e.originalValue()));
      result.put("createdAt", e.createdAt());
      result.put("dlqNotified", e.notified());
      result.put("corrections", store.corrections(p).stream().map(this::view).toList());
      return result;
    } catch (NoSuchElementException missing) {
      throw new ResponseStatusException(HttpStatus.NOT_FOUND, missing.getMessage());
    } catch (IllegalArgumentException invalid) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, invalid.getMessage());
    }
  }

  @PostMapping(value = "/quarantine/corrections", consumes = "application/json")
  public ResponseEntity<Map<String, Object>> correct(@RequestBody String body) {
    try {
      var n = json.readTree(body);
      var source = n.path("source");
      if (!source.path("partition").isIntegralNumber()
          || !source.path("partition").canConvertToInt()
          || !n.path("effectiveReport").isObject())
        throw new IllegalArgumentException("source o effectiveReport inválidos");
      var p =
          new InputPosition(
              text(source, "topic"),
              source.path("partition").intValue(),
              Long.parseLong(source.path("offset").asString()));
      if (store.find(p, false).isEmpty())
        throw new NoSuchElementException("Posición sin cuarentena");
      var key = text(n, "effectiveKey");
      var parsed = parser.parse(key, json.writeValueAsBytes(n.path("effectiveReport")));
      var request =
          new QuarantineStore.Correction(
              text(n, "requestId"),
              p,
              key,
              parsed.payload(),
              text(n, "reason"),
              text(n, "declaredOperator"),
              "ACCEPTED",
              null,
              null);
      int expected = partition(p.topic(), key);
      return ResponseEntity.accepted().body(view(store.accept(request, expected)));
    } catch (CorrectionConflict | DataIntegrityViolationException conflict) {
      throw new ResponseStatusException(HttpStatus.CONFLICT, conflict.getMessage());
    } catch (NoSuchElementException missing) {
      throw new ResponseStatusException(HttpStatus.NOT_FOUND, missing.getMessage());
    } catch (IllegalArgumentException | tools.jackson.core.JacksonException invalid) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, invalid.getMessage());
    }
  }

  private int partition(String topic, String key) {
    try (var admin =
        AdminClient.create(
            Map.of(
                "bootstrap.servers",
                bootstrap,
                "default.api.timeout.ms",
                3000,
                "request.timeout.ms",
                3000))) {
      int count =
          admin
              .describeTopics(List.of(topic))
              .allTopicNames()
              .get(3, TimeUnit.SECONDS)
              .get(topic)
              .partitions()
              .size();
      return (Utils.murmur2(key.getBytes(StandardCharsets.UTF_8)) & 0x7fffffff) % count;
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new ResponseStatusException(
          HttpStatus.SERVICE_UNAVAILABLE, "Consulta Kafka interrumpida");
    } catch (Exception unavailable) {
      throw new ResponseStatusException(
          HttpStatus.SERVICE_UNAVAILABLE, "No se pudo comprobar la partición Kafka");
    }
  }

  private Map<String, Object> view(QuarantineStore.Correction c) {
    var value = new LinkedHashMap<String, Object>();
    value.put("requestId", c.requestId());
    value.put("source", position(c.source()));
    value.put("effectiveKey", c.effectiveKey());
    value.put("effectiveReport", c.effectiveReport());
    value.put("reason", c.reason());
    value.put("declaredOperator", c.declaredOperator());
    value.put("outcome", c.outcome());
    value.put("diagnosticCode", c.diagnosticCode());
    value.put("createdAt", c.createdAt());
    return value;
  }

  private static String text(JsonNode n, String field) {
    if (!n.path(field).isString() || n.path(field).asString().isBlank())
      throw new IllegalArgumentException(field + " debe ser texto no vacío");
    return n.path(field).asString();
  }

  private static Map<String, Object> position(InputPosition p) {
    return Map.of(
        "topic", p.topic(), "partition", p.partition(), "offset", Long.toString(p.offset()));
  }

  private static String base64(byte[] b) {
    return b == null ? null : Base64.getEncoder().encodeToString(b);
  }
}
