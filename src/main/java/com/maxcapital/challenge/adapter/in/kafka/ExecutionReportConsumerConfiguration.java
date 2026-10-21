package com.maxcapital.challenge.adapter.in.kafka;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.transaction.KafkaTransactionManager;

@Configuration
@Profile("!evaluation-console")
public class ExecutionReportConsumerConfiguration {
  @Bean
  ConcurrentKafkaListenerContainerFactory<Object, Object> executionReportContainerFactory(
      ConsumerFactory<Object, Object> consumerFactory,
      KafkaTransactionManager<Object, Object> transactionManager,
      @Value("${spring.kafka.listener.auto-startup:true}") boolean autoStartup) {
    var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
    factory.setConsumerFactory(consumerFactory);
    factory.setConcurrency(1);
    factory.setAutoStartup(autoStartup);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
    factory.getContainerProperties().setKafkaAwareTransactionManager(transactionManager);
    factory.getContainerProperties().setStopImmediate(true);
    factory.getContainerProperties().setStopContainerWhenFenced(true);
    factory.setAfterRollbackProcessor(new StopAfterTransactionFailure());
    return factory;
  }
}
