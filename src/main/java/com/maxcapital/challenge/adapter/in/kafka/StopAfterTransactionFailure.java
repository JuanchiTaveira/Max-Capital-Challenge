package com.maxcapital.challenge.adapter.in.kafka;

import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.AfterRollbackProcessor;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListenerContainer;

final class StopAfterTransactionFailure implements AfterRollbackProcessor<Object, Object> {
  private static final Logger LOG = LoggerFactory.getLogger(StopAfterTransactionFailure.class);

  @Override
  public void process(
      List<ConsumerRecord<Object, Object>> records,
      Consumer<Object, Object> consumer,
      MessageListenerContainer container,
      Exception exception,
      boolean recoverable,
      ContainerProperties.EOSMode mode) {
    var record = records.getFirst();
    LOG.error(
        "ER pendiente; transacción Kafka fallida o incierta: topic={} partition={} offset={} numericOrderId/key={} cause={}",
        record.topic(),
        record.partition(),
        record.offset(),
        record.key(),
        exception.toString());
    // No seek: reiniciar crea un consumidor que obtiene el progreso estable del grupo.
    container.stopAbnormally(() -> {});
  }
}
