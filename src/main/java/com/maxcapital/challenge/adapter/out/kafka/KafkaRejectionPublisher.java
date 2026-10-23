package com.maxcapital.challenge.adapter.out.kafka;

import com.maxcapital.challenge.application.*;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Component
@Profile("!evaluation-console")
public class KafkaRejectionPublisher implements RejectionPublisher {
  private final KafkaTemplate<Object, Object> kafka;
  private final QuarantineStore store;
  private final String topic;

  public KafkaRejectionPublisher(
      KafkaTemplate<Object, Object> kafka,
      QuarantineStore store,
      @Value("${app.rejected-topic:execution-reports-rejected}") String topic) {
    this.kafka = kafka;
    this.store = store;
    this.topic = topic;
  }

  @Override
  public void publish(QuarantineStore.Entry entry) {
    if (entry.notified()) return;
    var p = entry.source();
    String id = "rejection:" + p.topic() + ":" + p.partition() + ":" + p.offset();
    byte[] payload =
        JsonMapper.builder()
            .build()
            .writeValueAsBytes(
                Map.of(
                    "schemaVersion",
                    1,
                    "rejectionId",
                    id,
                    "source",
                    Map.of(
                        "topic",
                        p.topic(),
                        "partition",
                        p.partition(),
                        "offset",
                        Long.toString(p.offset())),
                    "causeCode",
                    entry.causeCode()));
    // Transacción independiente: el offset inválido sigue pendiente.
    kafka.executeInTransaction(
        tx -> {
          tx.send(topic, p.partition(), id, payload);
          return null;
        });
    // Una caída entre estos commits puede repetir el aviso, nunca el negocio.
    store.notified(p);
  }
}
