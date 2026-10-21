package com.maxcapital.challenge;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;

// Entry point exclusivo de pruebas. Ningún flag ni endpoint se agrega al producto.
public final class SettlementProcess {
  private SettlementProcess() {}

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    var barrier = Path.of(option(args, "--test.barrier="));
    var probe =
        new TransactionProbe(
            Long.parseLong(option(args, "--test.order=")), TransactionProbe.Mode.BEFORE_SEND);
    try (var app =
        new SpringApplicationBuilder(MaxCapitalChallengeApplication.class)
            .web(WebApplicationType.NONE)
            .initializers(
                ctx ->
                    ctx.getBeanFactory()
                        .addBeanPostProcessor(
                            new BeanPostProcessor() {
                              @Override
                              public Object postProcessAfterInitialization(
                                  Object bean, String name) {
                                if (bean instanceof DefaultKafkaProducerFactory<?, ?> factory)
                                  ((DefaultKafkaProducerFactory<Object, Object>) factory)
                                      .addPostProcessor(probe);
                                return bean;
                              }
                            }))
            .run(args)) {
      app.getBean(KafkaListenerEndpointRegistry.class)
          .getListenerContainer("execution-reports")
          .start();
      if (!probe.entered.await(60, TimeUnit.SECONDS))
        throw new IllegalStateException("child did not reach SQL barrier");
      Files.writeString(barrier, Long.toString(ProcessHandle.current().pid()));
      new CountDownLatch(1).await();
    }
  }

  private static String option(String[] args, String prefix) {
    return Arrays.stream(args)
        .filter(a -> a.startsWith(prefix))
        .findFirst()
        .orElseThrow()
        .substring(prefix.length());
  }
}
