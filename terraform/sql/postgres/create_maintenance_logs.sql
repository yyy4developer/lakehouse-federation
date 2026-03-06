CREATE TABLE IF NOT EXISTS maintenance_logs (
    log_id       SERIAL PRIMARY KEY,
    machine_id   INTEGER NOT NULL,
    log_date     DATE NOT NULL,
    action       VARCHAR(100) NOT NULL,
    technician   VARCHAR(80) NOT NULL,
    duration_min INTEGER NOT NULL,
    notes        TEXT
);
