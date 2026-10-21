package com.maxcapital.challenge;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;
import org.springframework.kafka.core.ProducerPostProcessor;

// Instrumentación debajo de CloseSafeProducer: conserva la invalidación real de Spring.
final class TransactionProbe implements ProducerPostProcessor<Object, Object> {
  enum Mode {
    BEFORE_SEND,
    SEND_FAILURE,
    AFTER_SEND_ABORT,
    LOST_COMMIT_REPLY,
    PENDING_COMMIT
  }

  final String target;
  final Mode mode;
  final CountDownLatch entered = new CountDownLatch(1);
  final CountDownLatch release = new CountDownLatch(1);
  final AtomicBoolean once = new AtomicBoolean();
  final AtomicInteger sends = new AtomicInteger();
  final AtomicInteger closes = new AtomicInteger();
  final AtomicInteger failures = new AtomicInteger();

  TransactionProbe(long target, Mode mode) {
    this.target = Long.toString(target);
    this.mode = mode;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Producer<Object, Object> apply(Producer<Object, Object> delegate) {
    var targeted = new AtomicBoolean();
    return (Producer<Object, Object>)
        Proxy.newProxyInstance(
            Producer.class.getClassLoader(),
            new Class<?>[] {Producer.class},
            (proxy, method, args) -> {
              String name = method.getName();
              boolean send =
                  name.equals("send")
                      && args[0] instanceof ProducerRecord<?, ?> record
                      && target.equals(record.key());
              if (send) {
                targeted.set(true);
                sends.incrementAndGet();
                if (mode == Mode.SEND_FAILURE && once.compareAndSet(false, true)) {
                  failures.incrementAndGet();
                  throw new IllegalStateException("controlled send failure");
                }
                if (mode == Mode.BEFORE_SEND && once.compareAndSet(false, true)) block();
              }
              if (name.equals("commitTransaction")
                  && targeted.get()
                  && (mode == Mode.LOST_COMMIT_REPLY || mode == Mode.PENDING_COMMIT)
                  && once.compareAndSet(false, true)) {
                if (mode == Mode.LOST_COMMIT_REPLY) invoke(delegate, method, args);
                block();
                failures.incrementAndGet();
                throw new TimeoutException("controlled " + mode);
              }
              if (name.equals("close")) closes.incrementAndGet();
              Object result = invoke(delegate, method, args);
              if (send && mode == Mode.AFTER_SEND_ABORT && once.compareAndSet(false, true)) {
                ((Future<?>) result).get(20, TimeUnit.SECONDS);
                block();
                failures.incrementAndGet();
                throw new IllegalStateException("controlled abort after acknowledged send");
              }
              if (name.equals("commitTransaction") || name.equals("abortTransaction"))
                targeted.set(false);
              return result;
            });
  }

  private void block() throws InterruptedException {
    entered.countDown();
    if (!release.await(90, TimeUnit.SECONDS))
      throw new IllegalStateException("probe release timeout");
  }

  private static Object invoke(Object target, java.lang.reflect.Method method, Object[] args)
      throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }
}
