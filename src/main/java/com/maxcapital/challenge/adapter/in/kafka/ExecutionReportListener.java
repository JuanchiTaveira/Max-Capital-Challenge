package com.maxcapital.challenge.adapter.in.kafka;

import com.maxcapital.challenge.adapter.in.report.ExecutionReportParser;
import com.maxcapital.challenge.application.InputPosition;
import com.maxcapital.challenge.application.ProcessExecutionReport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class ExecutionReportListener {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutionReportListener.class);
  private final ExecutionReportParser parser;
  private final ProcessExecutionReport processor;

  public ExecutionReportListener(ExecutionReportParser parser, ProcessExecutionReport processor) {
    this.parser = parser;
    this.processor = processor;
  }

  @KafkaListener(
      id = "execution-reports",
      idIsGroup = false,
      topics = "${app.execution-reports-topic:execution-reports}",
      containerFactory = "executionReportContainerFactory")
  public void consume(ConsumerRecord<String, byte[]> record, Acknowledgment acknowledgment) {
    try {
      var parsed = parser.parse(record.key(), record.value());
      var result =
          processor.process(
              new InputPosition(record.topic(), record.partition(), record.offset()),
              parsed.report(),
              parsed.payload());
      LOG.debug(
          "ER resuelto: position={} outcome={} ledgerId={}",
          result.position(),
          result.outcome(),
          result.ledgerId());
      acknowledgment.acknowledge();
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
