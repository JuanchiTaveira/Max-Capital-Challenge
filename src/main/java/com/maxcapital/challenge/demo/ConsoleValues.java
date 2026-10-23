package com.maxcapital.challenge.demo;

import com.maxcapital.challenge.application.OrderQueries.OrderDetails;
import com.maxcapital.challenge.domain.OrderState;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

public final class ConsoleValues {
  static final JsonMapper JSON =
      JsonMapper.builder().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS).build();

  private ConsoleValues() {}

  static Map<String, Object> observed(String source) {
    var value = new LinkedHashMap<String, Object>();
    value.put("source", source);
    value.put("observedAt", Instant.now().toString());
    return value;
  }

  static long positive(String value) {
    try {
      long id = Long.parseLong(value);
      if (id <= 0) throw new NumberFormatException();
      return id;
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException("El identificador debe ser un entero positivo de 64 bits");
    }
  }

  static long offset(String value) {
    try {
      long offset = Long.parseLong(value);
      if (offset < 0) throw new NumberFormatException();
      return offset;
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException("El offset debe ser un entero no negativo de 64 bits");
    }
  }

  static Map<String, Object> state(OrderState state) {
    var value = new LinkedHashMap<String, Object>();
    value.put("numericOrderId", Long.toString(state.numericOrderId()));
    value.put("status", state.status().name());
    value.put("appliedExecutionCount", Long.toString(state.appliedExecutionCount()));
    value.put("nominalAmounts", state.nominalAmounts().toPlainString());
    value.put("accumulativeNominalAmount", state.accumulativeNominalAmount().toPlainString());
    value.put("leavesNominalAmount", state.leavesNominalAmount().toPlainString());
    return value;
  }

  static Map<String, Object> order(OrderDetails details) {
    var value = state(details.state());
    value.put(
        "ledger",
        details.ledger().stream()
            .map(entry -> ledgerEntry(Long.toString(entry.ledgerId()), entry.report()))
            .toList());
    return value;
  }

  static Map<String, Object> publicOrder(JsonNode node) {
    var result = new LinkedHashMap<String, Object>();
    for (var field :
        new String[] {
          "numericOrderId",
          "status",
          "appliedExecutionCount",
          "nominalAmounts",
          "accumulativeNominalAmount",
          "leavesNominalAmount"
        }) {
      result.put(field, node.path(field).asString());
    }
    var ledger = new java.util.ArrayList<Map<String, String>>();
    for (var entry : node.path("ledger")) {
      ledger.add(ledgerEntry(entry.path("ledgerId").asString(), entry.path("report").toString()));
    }
    result.put("ledger", ledger);
    return result;
  }

  private static Map<String, String> ledgerEntry(String id, String payload) {
    var result = new LinkedHashMap<String, String>();
    result.put("ledgerId", id);
    result.put("payload", payload);
    try {
      var report = JSON.readTree(payload);
      for (String field :
          new String[] {
            "fixId",
            "numericOrderId",
            "status",
            "nominalAmounts",
            "accumulativeNominalAmount",
            "leavesNominalAmount"
          }) {
        var value = report.get(field);
        if (value != null && !value.isNull())
          result.put(field, value.isValueNode() ? value.asString() : value.toString());
      }
    } catch (RuntimeException ignored) {
      // The original JSON remains available even if it cannot be summarized.
    }
    return result;
  }

  static Map<String, Object> sqlRow(Map<String, Object> row) {
    var result = new LinkedHashMap<String, Object>();
    row.forEach(
        (key, value) -> {
          var camel = new StringBuilder();
          boolean upper = false;
          for (char character : key.toCharArray()) {
            if (character == '_') upper = true;
            else {
              camel.append(upper ? Character.toUpperCase(character) : character);
              upper = false;
            }
          }
          result.put(
              camel.toString(),
              value instanceof BigDecimal decimal
                  ? decimal.toPlainString()
                  : value == null ? null : value.toString());
        });
    return result;
  }
}
