-- Iceberg tables don't support VARCHAR(N), use STRING
CREATE OR REPLACE ICEBERG TABLE operational_metrics (
    metric_id        INT,
    machine_id       INT,
    metric_date      DATE,
    oee_score        FLOAT,
    availability_pct FLOAT,
    performance_pct  FLOAT,
    quality_pct      FLOAT
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = '{external_volume}'
BASE_LOCATION = 'operational_metrics/'
