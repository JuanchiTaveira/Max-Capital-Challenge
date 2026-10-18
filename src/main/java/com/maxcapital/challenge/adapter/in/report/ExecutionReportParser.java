package com.maxcapital.challenge.adapter.in.report;

import com.maxcapital.challenge.domain.ExecutionReport;
import com.maxcapital.challenge.domain.OrderStatus;
import java.math.BigDecimal;
import org.springframework.stereotype.Component;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@Component
public class ExecutionReportParser {
  private final JsonMapper json =
      JsonMapper.builder()
          .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
          .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
          .build();

  public ParsedReport parse(String key, byte[] bytes) {
    if (bytes == null) {
      throw new IllegalArgumentException("ER nulo");
    }
    var tree = json.readTree(bytes);
    if (tree == null || !tree.isObject()) {
      throw new IllegalArgumentException("el ER debe ser un objeto JSON");
    }
    var report =
        new ExecutionReport(
            integer(tree, "fixId"),
            integer(tree, "numericOrderId"),
            OrderStatus.valueOf(string(tree, "status")),
            quantity(tree, "nominalAmounts"),
            quantity(tree, "leavesNominalAmount"),
            quantity(tree, "accumulativeNominalAmount"),
            string(tree, "transactionTime"));
    if (!Long.toString(report.numericOrderId()).equals(key)) {
      throw new IllegalArgumentException("key Kafka distinta de numericOrderId");
    }
    return new ParsedReport(report, json.writeValueAsString(tree));
  }

  private static long integer(JsonNode tree, String field) {
    var node = tree.path(field);
    if (!node.isIntegralNumber() || !node.canConvertToLong()) {
      throw new IllegalArgumentException(field + " debe ser entero de 64 bits");
    }
    return node.longValue();
  }

  private static String string(JsonNode tree, String field) {
    var node = tree.path(field);
    if (!node.isString() || node.asString().isBlank()) {
      throw new IllegalArgumentException(field + " debe ser texto no vacío");
    }
    return node.asString();
  }

  private static BigDecimal quantity(JsonNode tree, String field) {
    var node = tree.path(field);
    if (!node.isNumber()) {
      throw new IllegalArgumentException(field + " debe ser numérico");
    }
    return node.decimalValue();
  }

  public record ParsedReport(ExecutionReport report, String payload) {}
}
