package com.maxcapital.challenge.adapter.in.kafka;

import com.maxcapital.challenge.adapter.in.report.ExecutionReportParser;
import com.maxcapital.challenge.application.HandleExecutionReport;
import com.maxcapital.challenge.application.InputPosition;
import com.maxcapital.challenge.application.QuarantineStore;
import com.maxcapital.challenge.application.RejectionPublisher;
import com.maxcapital.challenge.domain.InvalidReport;
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
  private final QuarantineStore quarantine;
  private final RejectionPublisher rejections;

  public ExecutionReportListener(
      ExecutionReportParser parser,
      HandleExecutionReport handler,
      QuarantineStore quarantine,
      RejectionPublisher rejections) {
    this.parser = parser;
    this.handler = handler;
    this.quarantine = quarantine;
    this.rejections = rejections;
  }

  @KafkaListener(
      id = "execution-reports",
      idIsGroup = false,
      topics = "${app.execution-reports-topic:execution-reports}",
      containerFactory = "executionReportContainerFactory")
  public void consume(ConsumerRecord<?, byte[]> record) {
    try {
      var position = new InputPosition(record.topic(), record.partition(), record.offset());
      var entry = quarantine.find(position, false);
      String key;
      byte[] value = record.value();
      String correctionId = null;
      if (entry.isPresent()) {
        rejections.publish(entry.get());
        if (entry.get().state().equals("OPEN"))
          throw new InvalidReport(entry.get().causeCode(), "ER en cuarentena");
        var correction = entry.get().correction();
        key = correction.effectiveKey();
        value = correction.effectiveReport().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        correctionId = correction.requestId();
      } else {
        try {
          var raw = StopAfterTransactionFailure.bytes(record.key());
          key =
              raw == null
                  ? null
                  : java.nio.charset.StandardCharsets.UTF_8
                      .newDecoder()
                      .decode(java.nio.ByteBuffer.wrap(raw))
                      .toString();
        } catch (java.nio.charset.CharacterCodingException invalid) {
          throw new InvalidReport("INVALID_KEY", "key Kafka no es UTF-8");
        }
      }
      var parsed = parser.parse(key, value);
      var result = handler.handle(position, parsed.report(), parsed.payload(), correctionId);
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
          record.key() instanceof byte[] raw
              ? new String(raw, java.nio.charset.StandardCharsets.UTF_8)
              : record.key(),
          exception.toString());
      throw exception;
    }
  }
}
