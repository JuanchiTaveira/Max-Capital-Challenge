package com.maxcapital.challenge.adapter.out.kafka;

import com.maxcapital.challenge.application.Settlement;
import com.maxcapital.challenge.application.SettlementPublisher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Profile("!evaluation-console")
public class KafkaSettlementPublisher implements SettlementPublisher {
  private final KafkaTemplate<Object, Object> kafka;
  private final String topic;

  public KafkaSettlementPublisher(
      KafkaTemplate<Object, Object> kafka,
      @Value("${app.settlements-topic:settlements}") String topic) {
    this.kafka = kafka;
    this.topic = topic;
  }

  @Override
  public void publish(Settlement settlement) {
    kafka.send(
        topic, Long.toString(settlement.numericOrderId()), SettlementJson.encode(settlement));
  }
}
