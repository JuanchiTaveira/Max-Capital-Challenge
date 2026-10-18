package com.maxcapital.challenge.adapter.in.kafka;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.CommonContainerStoppingErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;

@Configuration
public class ExecutionReportConsumerConfiguration {
  @Bean
  ConcurrentKafkaListenerContainerFactory<Object, Object> executionReportContainerFactory(
      ConsumerFactory<Object, Object> consumerFactory,
      @Value("${spring.kafka.listener.auto-startup:true}") boolean autoStartup) {
    var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
    factory.setConsumerFactory(consumerFactory);
    factory.setConcurrency(1);
    factory.setAutoStartup(autoStartup);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    factory.getContainerProperties().setSyncCommits(true);
    factory.setCommonErrorHandler(new CommonContainerStoppingErrorHandler());
    return factory;
  }
}
