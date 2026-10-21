package com.maxcapital.challenge.adapter.out.kafka;

import com.maxcapital.challenge.application.Settlement;
import java.math.BigDecimal;
import tools.jackson.databind.json.JsonMapper;

public final class SettlementJson {
  private static final JsonMapper JSON = new JsonMapper();

  private SettlementJson() {}

  public static byte[] encode(Settlement settlement) {
    var node = JSON.createObjectNode();
    node.put("schemaVersion", 1);
    node.put("settlementId", "settlement:" + settlement.numericOrderId());
    node.put("numericOrderId", Long.toString(settlement.numericOrderId()));
    node.put("fixId", Long.toString(settlement.fixId()));
    node.put("ledgerId", Long.toString(settlement.ledgerId()));
    var source = node.putObject("source");
    source.put("topic", settlement.source().topic());
    source.put("partition", settlement.source().partition());
    source.put("offset", Long.toString(settlement.source().offset()));
    node.put("nominalAmounts", decimal(settlement.nominalAmounts()));
    node.put("accumulativeNominalAmount", decimal(settlement.accumulativeNominalAmount()));
    node.put("leavesNominalAmount", decimal(settlement.leavesNominalAmount()));
    return JSON.writeValueAsBytes(node);
  }

  private static String decimal(BigDecimal value) {
    return value.stripTrailingZeros().toPlainString();
  }
}
