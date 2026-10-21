package com.maxcapital.challenge.adapter.in.kafka;

import com.maxcapital.challenge.adapter.in.report.ExecutionReportParser;
import com.maxcapital.challenge.application.HandleExecutionReport;
import com.maxcapital.challenge.application.InputPosition;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Profile("!evaluation-console")
public class ExecutionReportListener {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutionReportListener.class);
  private final ExecutionReportParser parser;
  private final HandleExecutionReport handler;

  public ExecutionReportListener(ExecutionReportParser parser, HandleExecutionReport handler) {
    this.parser = parser;
    this.handler = handler;
  }

  @KafkaListener(
      id = "execution-reports",
      idIsGroup = false,
      topics = "${app.execution-reports-topic:execution-reports}",
      containerFactory = "executionReportContainerFactory")
  public void consume(ConsumerRecord<String, byte[]> record) {
    try {
      var parsed = parser.parse(record.key(), record.value());
      var result =
          handler.handle(
              new InputPosition(record.topic(), record.partition(), record.offset()),
              parsed.report(),
              parsed.payload());
      LOG.debug(
          "ER resuelto en SQL, pendiente de commit Kafka: position={} outcome={} ledgerId={}",
          result.position(),
          result.outcome(),
          result.ledgerId());
    } catch (RuntimeException exception) {
      LOG.error(
          "ER pendiente: topic={} partition={} offset={} numericOrderId/key={} cause={}",
          record.topic(),
          record.partition(),
          record.offset(),
          record.key(),
          exception.toString());
      throw exception;
    }
  }
}
