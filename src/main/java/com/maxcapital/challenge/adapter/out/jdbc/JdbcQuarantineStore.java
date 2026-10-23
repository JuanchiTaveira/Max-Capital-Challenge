package com.maxcapital.challenge.adapter.out.jdbc;

import com.maxcapital.challenge.application.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class JdbcQuarantineStore implements QuarantineStore {
  private final JdbcTemplate jdbc;

  public JdbcQuarantineStore(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  @Override
  public Optional<Entry> find(InputPosition p, boolean lock) {
    var rows =
        jdbc.query(
            "SELECT * FROM app.quarantined_reports WHERE topic=? AND partition_id=? AND record_offset=?"
                + (lock ? " FOR UPDATE" : ""),
            (r, i) ->
                new Entry(
                    p,
                    r.getBytes("original_key"),
                    r.getBytes("original_value"),
                    r.getString("cause_code"),
                    r.getString("state"),
                    r.getObject("dlq_notified_at") != null,
                    r.getString("correction_id") == null
                        ? null
                        : correction(r.getString("correction_id")).orElseThrow(),
                    r.getString("created_at")),
            p.topic(),
            p.partition(),
            p.offset());
    return rows.stream().findFirst();
  }

  @Override
  @Transactional("orderTransactionManager")
  public Entry capture(InputPosition p, byte[] key, byte[] value, String code) {
    jdbc.update(
        "INSERT INTO app.quarantined_reports(topic,partition_id,record_offset,original_key,original_value,cause_code) VALUES (?,?,?,?,?,?) ON CONFLICT DO NOTHING",
        p.topic(),
        p.partition(),
        p.offset(),
        key,
        value,
        code);
    var entry = find(p, true).orElseThrow();
    if (!Arrays.equals(key, entry.originalKey()) || !Arrays.equals(value, entry.originalValue()))
      throw new CorrectionConflict("Contenido distinto para la posición capturada");
    return entry;
  }

  @Override
  public void notified(InputPosition p) {
    jdbc.update(
        "UPDATE app.quarantined_reports SET dlq_notified_at=coalesce(dlq_notified_at,clock_timestamp()) WHERE topic=? AND partition_id=? AND record_offset=?",
        p.topic(),
        p.partition(),
        p.offset());
  }

  @Override
  @Transactional("orderTransactionManager")
  public Correction accept(Correction c, int expectedPartition) {
    if (c.requestId() == null
        || c.requestId().isBlank()
        || c.requestId().length() > 200
        || c.reason() == null
        || c.reason().isBlank()
        || c.declaredOperator() == null
        || c.declaredOperator().isBlank())
      throw new IllegalArgumentException("requestId, motivo y responsable son obligatorios");
    var p = c.source();
    var entry =
        find(p, true).orElseThrow(() -> new NoSuchElementException("Posición sin cuarentena"));
    var previous = correction(c.requestId());
    if (previous.isPresent()) {
      var old = previous.get();
      boolean sameJson =
          jdbc.queryForObject(
              "SELECT effective_report=CAST(? AS jsonb) FROM app.report_corrections WHERE request_id=?",
              Boolean.class,
              c.effectiveReport(),
              c.requestId());
      if (old.source().equals(p)
          && old.effectiveKey().equals(c.effectiveKey())
          && sameJson
          && old.reason().equals(c.reason())
          && old.declaredOperator().equals(c.declaredOperator())) return old;
      throw new CorrectionConflict("requestId reutilizado con otro contenido");
    }
    if (!entry.state().equals("OPEN")
        || jdbc.queryForObject(
            "SELECT EXISTS(SELECT 1 FROM app.processing_results WHERE topic=? AND partition_id=? AND record_offset=?)",
            Boolean.class,
            p.topic(),
            p.partition(),
            p.offset()))
      throw new CorrectionConflict("La posición ya está resuelta o tiene una corrección aceptada");
    String key =
        entry.originalKey() == null ? "" : new String(entry.originalKey(), StandardCharsets.UTF_8);
    if (key.matches("[1-9][0-9]*") && !key.equals(c.effectiveKey()))
      throw new CorrectionConflict("La identidad de la key original no puede cambiar");
    if (expectedPartition != p.partition())
      throw new CorrectionConflict("La corrección no corresponde a la partición original");
    jdbc.update(
        "INSERT INTO app.report_corrections(request_id,topic,partition_id,record_offset,effective_key,effective_report,reason,declared_operator) VALUES (?,?,?,?,?,CAST(? AS jsonb),?,?)",
        c.requestId(),
        p.topic(),
        p.partition(),
        p.offset(),
        c.effectiveKey(),
        c.effectiveReport(),
        c.reason(),
        c.declaredOperator());
    jdbc.update(
        "UPDATE app.quarantined_reports SET state='READY',correction_id=? WHERE topic=? AND partition_id=? AND record_offset=?",
        c.requestId(),
        p.topic(),
        p.partition(),
        p.offset());
    return correction(c.requestId()).orElseThrow();
  }

  @Override
  public void applied(InputPosition p, String outcome) {
    jdbc.update(
        "UPDATE app.report_corrections SET outcome=? WHERE request_id=(SELECT correction_id FROM app.quarantined_reports WHERE topic=? AND partition_id=? AND record_offset=?) AND outcome='ACCEPTED'",
        outcome,
        p.topic(),
        p.partition(),
        p.offset());
    jdbc.update(
        "UPDATE app.quarantined_reports SET state='RESOLVED' WHERE topic=? AND partition_id=? AND record_offset=? AND state='READY'",
        p.topic(),
        p.partition(),
        p.offset());
  }

  @Override
  @Transactional("orderTransactionManager")
  public void rejectCorrection(InputPosition p, String requestId, String code) {
    var entry = find(p, true);
    if (entry.isEmpty()
        || !entry.get().state().equals("READY")
        || !entry.get().correction().requestId().equals(requestId)) return;
    jdbc.update(
        "UPDATE app.report_corrections SET outcome='REJECTED',diagnostic_code=? WHERE request_id=?",
        code,
        entry.get().correction().requestId());
    jdbc.update(
        "UPDATE app.quarantined_reports SET state='OPEN' WHERE topic=? AND partition_id=? AND record_offset=?",
        p.topic(),
        p.partition(),
        p.offset());
  }

  @Override
  public List<Correction> corrections(InputPosition p) {
    return jdbc.query(
        "SELECT * FROM app.report_corrections WHERE topic=? AND partition_id=? AND record_offset=? ORDER BY created_at,request_id",
        (r, i) -> map(r),
        p.topic(),
        p.partition(),
        p.offset());
  }

  private Optional<Correction> correction(String id) {
    return jdbc
        .query("SELECT * FROM app.report_corrections WHERE request_id=?", (r, i) -> map(r), id)
        .stream()
        .findFirst();
  }

  private static Correction map(java.sql.ResultSet r) throws java.sql.SQLException {
    return new Correction(
        r.getString("request_id"),
        new InputPosition(
            r.getString("topic"), r.getInt("partition_id"), r.getLong("record_offset")),
        r.getString("effective_key"),
        r.getString("effective_report"),
        r.getString("reason"),
        r.getString("declared_operator"),
        r.getString("outcome"),
        r.getString("diagnostic_code"),
        r.getString("created_at"));
  }
}
