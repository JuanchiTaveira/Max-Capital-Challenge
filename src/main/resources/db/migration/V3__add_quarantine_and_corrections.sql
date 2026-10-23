CREATE TABLE app.quarantined_reports (
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
    record_offset BIGINT NOT NULL CHECK (record_offset >= 0),
    original_key BYTEA,
    original_value BYTEA,
    cause_code TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    dlq_notified_at TIMESTAMPTZ,
    state TEXT NOT NULL DEFAULT 'OPEN' CHECK (state IN ('OPEN','READY','RESOLVED')),
    correction_id TEXT,
    PRIMARY KEY (topic, partition_id, record_offset)
);

CREATE TABLE app.report_corrections (
    request_id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    record_offset BIGINT NOT NULL,
    effective_key TEXT NOT NULL,
    effective_report JSONB NOT NULL,
    reason TEXT NOT NULL CHECK (btrim(reason) <> ''),
    declared_operator TEXT NOT NULL CHECK (btrim(declared_operator) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    outcome TEXT NOT NULL DEFAULT 'ACCEPTED' CHECK (outcome IN ('ACCEPTED','APPLIED','DUPLICATE','REJECTED')),
    diagnostic_code TEXT,
    FOREIGN KEY (topic, partition_id, record_offset)
        REFERENCES app.quarantined_reports(topic, partition_id, record_offset)
);

CREATE UNIQUE INDEX one_accepted_correction ON app.report_corrections(topic, partition_id, record_offset)
    WHERE outcome = 'ACCEPTED';
ALTER TABLE app.quarantined_reports ADD FOREIGN KEY (correction_id)
    REFERENCES app.report_corrections(request_id);
