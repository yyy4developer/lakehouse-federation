CREATE TABLE IF NOT EXISTS equipment_specs (
    spec_id         INT         NOT NULL,
    machine_id      INT         NOT NULL,
    manufacturer    VARCHAR(50) NOT NULL,
    model_number    VARCHAR(30) NOT NULL,
    max_rpm         DECIMAL(8,1),
    weight_kg       DECIMAL(8,1),
    power_kw        DECIMAL(8,1),
    install_year    INT,
    PRIMARY KEY (spec_id)
);
