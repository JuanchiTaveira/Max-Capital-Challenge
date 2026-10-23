package com.maxcapital.challenge.adapter.in.kafka;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.maxcapital.challenge.application.*;
import com.maxcapital.challenge.domain.InvalidReport;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.*;
import org.springframework.util.backoff.*;

class QuarantineRetryTest {
  @Test
  void exponentialJitterHasFourBoundedWaitsAndEachSessionStartsAgain() {
    var backoff = StopAfterTransactionFailure.retryBackOff();
    for (int session = 0; session < 10; session++) {
      var execution = backoff.start();
      for (long base : new long[] {500, 1000, 2000, 4000})
        assertThat(execution.nextBackOff()).isBetween(base, base + 200);
      assertThat(execution.nextBackOff()).isEqualTo(BackOffExecution.STOP);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void retriesKnownRollbackFiveTimesThenPausesWithoutCommitting() {
    var store = mock(QuarantineStore.class);
    var publisher = mock(RejectionPublisher.class);
    var processor = new StopAfterTransactionFailure(store, publisher, new FixedBackOff(0, 4));
    Consumer<Object, Object> consumer = mock(Consumer.class);
    var container = mock(MessageListenerContainer.class);
    var paused = new AtomicBoolean();
    var p = new TopicPartition("retry", 0);
    doAnswer(
            i -> {
              paused.set(true);
              return null;
            })
        .when(consumer)
        .pause(anyCollection());
    when(consumer.paused()).thenAnswer(i -> paused.get() ? Set.of(p) : Set.of());
    var r = new ConsumerRecord<Object, Object>("retry", 0, 7L, "1", new byte[] {1});
    for (int attempt = 1; attempt <= 5; attempt++) {
      processor.process(
          List.of(r),
          consumer,
          container,
          new ListenerExecutionFailedException("SQL", new SQLException("deadlock", "40P01")),
          true,
          ContainerProperties.EOSMode.V2);
      assertThat(paused.get()).isEqualTo(attempt == 5);
    }
    verify(container).pausePartition(p);
    verify(container, never()).stopAbnormally(any());
    verify(consumer, never()).commitSync();
    verifyNoInteractions(store, publisher);
  }

  @Test
  @SuppressWarnings("unchecked")
  void pausesPermanentOnlyAfterCaptureAndNotificationAndStopsOnUncertainty() {
    var store = mock(QuarantineStore.class);
    var publisher = mock(RejectionPublisher.class);
    var processor = new StopAfterTransactionFailure(store, publisher);
    Consumer<Object, Object> consumer = mock(Consumer.class);
    var container = mock(MessageListenerContainer.class);
    var p = new InputPosition("reports", 1, 9);
    var entry =
        new QuarantineStore.Entry(
            p, new byte[] {0}, new byte[] {-1}, "INVALID_FORMAT", "OPEN", false, null, "now");
    when(store.capture(eq(p), any(), any(), eq("INVALID_FORMAT"))).thenReturn(entry);
    var r =
        new ConsumerRecord<Object, Object>(
            p.topic(), p.partition(), p.offset(), entry.originalKey(), entry.originalValue());
    processor.process(
        List.of(r),
        consumer,
        container,
        new ListenerExecutionFailedException(
            "invalid", new InvalidReport("INVALID_FORMAT", "JSON")),
        true,
        ContainerProperties.EOSMode.V2);
    var ordered = inOrder(store, publisher, container, consumer);
    ordered.verify(store).capture(eq(p), any(), any(), eq("INVALID_FORMAT"));
    ordered.verify(publisher).publish(entry);
    ordered.verify(container).pausePartition(new TopicPartition("reports", 1));
    doThrow(new IllegalStateException("DLQ unavailable")).when(publisher).publish(entry);
    processor.process(
        List.of(r),
        consumer,
        container,
        new ListenerExecutionFailedException(
            "invalid", new InvalidReport("INVALID_FORMAT", "JSON")),
        true,
        ContainerProperties.EOSMode.V2);
    processor.process(
        List.of(r),
        consumer,
        container,
        new IllegalStateException("uncertain commit"),
        true,
        ContainerProperties.EOSMode.V2);
    verify(container, times(2)).stopAbnormally(any());
  }
}
