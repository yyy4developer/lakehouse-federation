IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'energy_consumption')
CREATE TABLE energy_consumption (
    record_id   INT PRIMARY KEY,
    machine_id  INT NOT NULL,
    measure_date DATE NOT NULL,
    kwh_consumed DECIMAL(8,2) NOT NULL,
    peak_demand_kw DECIMAL(6,2) NOT NULL,
    cost_usd    DECIMAL(8,2) NOT NULL
);
