package com.maxcapital.challenge.adapter.in.kafka;

import com.maxcapital.challenge.application.*;
import com.maxcapital.challenge.domain.InvalidReport;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.*;
import org.springframework.util.backoff.*;

final class StopAfterTransactionFailure extends DefaultAfterRollbackProcessor<Object, Object> {
  private static final Logger LOG = LoggerFactory.getLogger(StopAfterTransactionFailure.class);
  private final QuarantineStore quarantine;
  private final RejectionPublisher publisher;

  StopAfterTransactionFailure(QuarantineStore quarantine, RejectionPublisher publisher) {
    this(quarantine, publisher, retryBackOff());
  }

  StopAfterTransactionFailure(
      QuarantineStore quarantine, RejectionPublisher publisher, BackOff backOff) {
    super(
        (ConsumerAwareRecordRecoverer)
            (record, consumer, error) -> {
              LOG.error(
                  "Reintentos agotados; partición pausada: {}-{} offset={}",
                  record.topic(),
                  record.partition(),
                  record.offset());
              consumer.pause(Set.of(new TopicPartition(record.topic(), record.partition())));
            },
        backOff);
    this.quarantine = quarantine;
    this.publisher = publisher;
    setResetStateOnExceptionChange(false);
  }

  static BackOff retryBackOff() {
    var exponential = new ExponentialBackOff(500, 2);
    exponential.setMaxInterval(5000);
    exponential.setMaxAttempts(4);
    return () -> {
      var execution = exponential.start();
      return () -> {
        long base = execution.nextBackOff();
        return base == BackOffExecution.STOP
            ? base
            : Math.min(5000, base + ThreadLocalRandom.current().nextLong(201));
      };
    };
  }

  @Override
  public void process(
      List<ConsumerRecord<Object, Object>> records,
      Consumer<Object, Object> consumer,
      MessageListenerContainer container,
      Exception exception,
      boolean recoverable,
      ContainerProperties.EOSMode mode) {
    var r = records.getFirst();
    var p = new TopicPartition(r.topic(), r.partition());
    var position = new InputPosition(r.topic(), r.partition(), r.offset());
    boolean listenerFailure = cause(exception, ListenerExecutionFailedException.class) != null;
    try {
      var invalid = cause(exception, InvalidReport.class);
      if (listenerFailure && invalid != null) {
        var entry = quarantine.capture(position, bytes(r.key()), bytes(r.value()), invalid.code());
        var correction = cause(exception, CorrectionFailure.class);
        if (correction != null)
          quarantine.rejectCorrection(position, correction.requestId(), invalid.code());
        publisher.publish(entry);
        pause(consumer, container, p, r.offset());
        return;
      }
      var sql = cause(exception, SQLException.class);
      if (listenerFailure && sql != null && Set.of("40P01", "40001").contains(sql.getSQLState())) {
        super.process(records, consumer, container, exception, recoverable, mode);
        if (consumer.paused().contains(p)) pause(consumer, container, p, r.offset());
        return;
      }
    } catch (RuntimeException failure) {
      LOG.error("No se pudo conservar/notificar el ER pendiente: {}", position, failure);
    }
    LOG.error(
        "ER pendiente; transacción Kafka fallida o incierta: topic={} partition={} offset={} cause={}",
        r.topic(),
        r.partition(),
        r.offset(),
        exception.toString());
    container.stopAbnormally(() -> {});
  }

  private static void pause(
      Consumer<?, ?> consumer, MessageListenerContainer container, TopicPartition p, long offset) {
    container.pausePartition(p);
    // Spring registra la pausa antes del próximo poll (max.poll.records=1).
    if (consumer.paused().contains(p)) consumer.resume(Set.of(p));
    consumer.seek(p, offset);
  }

  static byte[] bytes(Object value) {
    return value == null
        ? null
        : value instanceof byte[] b ? b : value.toString().getBytes(StandardCharsets.UTF_8);
  }

  private static <T extends Throwable> T cause(Throwable e, Class<T> type) {
    for (var c = e; c != null; c = c.getCause()) if (type.isInstance(c)) return type.cast(c);
    return null;
  }
}
