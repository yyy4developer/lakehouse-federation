"""Amazon Redshift - Query Federation source."""

from ..config import SourceDef

source = SourceDef(
    key="redshift",
    label="Amazon Redshift (Query Federation)",
    fed_type="query",
    cloud_req=None,
    tables=["sensor_readings", "production_events", "quality_inspections"],
    sections=["1.2 Query Federation: Amazon Redshift"],
    test_queries=[
        ("sensor_readings", "{query_prefix}_redshift.{db_prefix}.sensor_readings", 100),
        ("production_events", "{query_prefix}_redshift.{db_prefix}.production_events", 30),
        ("quality_inspections", "{query_prefix}_redshift.{db_prefix}.quality_inspections", 40),
    ],
)
