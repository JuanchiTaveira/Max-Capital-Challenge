CREATE TABLE app.orders (
    numeric_order_id BIGINT PRIMARY KEY CHECK (numeric_order_id > 0),
    status VARCHAR(32) NOT NULL CHECK (status IN ('NEW', 'PARTIALLY_FILLED', 'FILLED', 'CANCELLED')),
    nominal_amounts NUMERIC(20,8) NOT NULL CHECK (nominal_amounts > 0),
    leaves_nominal_amount NUMERIC(20,8) NOT NULL CHECK (leaves_nominal_amount >= 0),
    accumulative_nominal_amount NUMERIC(20,8) NOT NULL
        CHECK (accumulative_nominal_amount >= 0 AND accumulative_nominal_amount <= nominal_amounts),
    applied_execution_count BIGINT NOT NULL CHECK (applied_execution_count >= 0)
);

CREATE TABLE app.execution_report_ledger (
    ledger_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    numeric_order_id BIGINT NOT NULL REFERENCES app.orders(numeric_order_id),
    fix_id BIGINT NOT NULL CHECK (fix_id > 0),
    report JSONB NOT NULL CHECK (jsonb_typeof(report) = 'object'),
    UNIQUE (numeric_order_id, fix_id)
);

CREATE INDEX execution_report_ledger_order_insertion
    ON app.execution_report_ledger(numeric_order_id, ledger_id);
