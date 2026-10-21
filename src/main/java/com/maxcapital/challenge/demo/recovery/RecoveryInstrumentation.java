package com.maxcapital.challenge.demo.recovery;

import com.maxcapital.challenge.application.InputPosition;
import com.maxcapital.challenge.application.ProcessingResult;
import com.maxcapital.challenge.application.ProcessingResults;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import tools.jackson.databind.json.JsonMapper;

@Configuration(proxyBeanMethods = false)
@Profile("!evaluation-console")
@ConditionalOnProperty(name = "app.demo.recovery-enabled", havingValue = "true")
public class RecoveryInstrumentation {
  @Bean
  static RecoveryBarrier recoveryBarrier(Environment environment) {
    return new RecoveryBarrier(environment.getRequiredProperty("app.instance-id"));
  }

  @Bean
  static BeanPostProcessor recoveryWiring(RecoveryBarrier barrier) {
    return new BeanPostProcessor() {
      @Override
      @SuppressWarnings("unchecked")
      public Object postProcessAfterInitialization(Object bean, String name) {
        if (bean instanceof ProcessingResults delegate) {
          return new ProcessingResults() {
            @Override
            public Optional<StoredResult> find(InputPosition position, String payload) {
              return delegate.find(position, payload);
            }

            @Override
            public void insert(ProcessingResult result) {
              delegate.insert(result);
              TransactionSynchronizationManager.registerSynchronization(
                  new TransactionSynchronization() {
                    @Override
                    public void beforeCommit(boolean readOnly) {
                      barrier.reach(
                          RecoveryBarrier.Boundary.BEFORE_SQL_COMMIT,
                          result.position(),
                          result.numericOrderId(),
                          result.fixId());
                    }

                    @Override
                    public void afterCommit() {
                      barrier.reach(
                          RecoveryBarrier.Boundary.AFTER_SQL_COMMIT,
                          result.position(),
                          result.numericOrderId(),
                          result.fixId());
                    }
                  });
            }
          };
        }
        if (bean instanceof DefaultKafkaProducerFactory<?, ?> factory) {
          ((DefaultKafkaProducerFactory<Object, Object>) factory)
              .addPostProcessor(
                  delegate ->
                      (Producer<Object, Object>)
                          Proxy.newProxyInstance(
                              Producer.class.getClassLoader(),
                              new Class<?>[] {Producer.class},
                              (proxy, method, args) -> {
                                Object value;
                                try {
                                  value = method.invoke(delegate, args);
                                } catch (InvocationTargetException ex) {
                                  throw ex.getCause();
                                }
                                if (method.getName().equals("send")
                                    && args[0] instanceof ProducerRecord<?, ?> record
                                    && record.value() instanceof byte[] bytes) {
                                  var json = JsonMapper.builder().build().readTree(bytes);
                                  if (json.path("schemaVersion").asInt() == 1
                                      && json.has("source")) {
                                    ((Future<?>) value).get(20, TimeUnit.SECONDS);
                                    var source = json.path("source");
                                    barrier.reach(
                                        RecoveryBarrier.Boundary.AFTER_SEND_ACK_BEFORE_KAFKA_COMMIT,
                                        new InputPosition(
                                            source.path("topic").asString(),
                                            source.path("partition").asInt(),
                                            Long.parseLong(source.path("offset").asString())),
                                        Long.parseLong(json.path("numericOrderId").asString()),
                                        Long.parseLong(json.path("fixId").asString()));
                                  }
                                }
                                return value;
                              }));
        }
        if (bean instanceof ConcurrentKafkaListenerContainerFactory<?, ?> factory) {
          var typed = (ConcurrentKafkaListenerContainerFactory<Object, Object>) factory;
          typed.setContainerCustomizer(
              created -> {
                var delegate = created.getAfterRollbackProcessor();
                if (delegate != null)
                  created.setAfterRollbackProcessor(
                      (records, consumer, container, exception, recoverable, mode) -> {
                        if (!records.isEmpty()) {
                          var first = records.getFirst();
                          barrier.failed(
                              new InputPosition(first.topic(), first.partition(), first.offset()),
                              exception);
                        }
                        delegate.process(
                            records, consumer, container, exception, recoverable, mode);
                      });
              });
        }
        return bean;
      }
    };
  }
}
