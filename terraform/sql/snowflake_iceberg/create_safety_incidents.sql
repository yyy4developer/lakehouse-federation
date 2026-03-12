CREATE OR REPLACE ICEBERG TABLE safety_incidents (
    incident_id       INT,
    machine_id        INT,
    incident_date     DATE,
    severity          STRING,
    description       STRING,
    corrective_action STRING,
    resolved          BOOLEAN
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = '{external_volume}'
BASE_LOCATION = 'safety_incidents/'
