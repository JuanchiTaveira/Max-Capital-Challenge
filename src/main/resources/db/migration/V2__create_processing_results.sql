ALTER TABLE app.execution_report_ledger
    ADD CONSTRAINT execution_report_ledger_reference
    UNIQUE (ledger_id, numeric_order_id, fix_id);

CREATE TABLE app.processing_results (
    topic TEXT NOT NULL CHECK (btrim(topic) <> ''),
    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
    record_offset BIGINT NOT NULL CHECK (record_offset >= 0),
    outcome VARCHAR(16) NOT NULL CHECK (outcome IN ('APPLIED', 'DUPLICATE')),
    numeric_order_id BIGINT NOT NULL,
    fix_id BIGINT NOT NULL,
    ledger_id BIGINT NOT NULL,
    status VARCHAR(32) NOT NULL CHECK (status IN ('NEW', 'PARTIALLY_FILLED', 'FILLED', 'CANCELLED')),
    nominal_amounts NUMERIC(20,8) NOT NULL CHECK (nominal_amounts > 0),
    leaves_nominal_amount NUMERIC(20,8) NOT NULL CHECK (leaves_nominal_amount >= 0),
    accumulative_nominal_amount NUMERIC(20,8) NOT NULL
        CHECK (accumulative_nominal_amount >= 0 AND accumulative_nominal_amount <= nominal_amounts),
    applied_execution_count BIGINT NOT NULL CHECK (applied_execution_count >= 0),
    PRIMARY KEY (topic, partition_id, record_offset),
    FOREIGN KEY (ledger_id, numeric_order_id, fix_id)
        REFERENCES app.execution_report_ledger (ledger_id, numeric_order_id, fix_id)
);
