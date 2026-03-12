"""Google BigQuery - Query Federation source."""

from ..config import SourceDef

source = SourceDef(
    key="bigquery",
    label="Google BigQuery (Query Federation)",
    fed_type="query",
    cloud_req=None,
    tables=["downtime_records", "cost_allocation"],
    sections=["1.5 Query Federation: Google BigQuery"],
    test_queries=[
        ("downtime_records", "{query_prefix}_bigquery.{db_prefix}_factory.downtime_records", 35),
        ("cost_allocation", "{query_prefix}_bigquery.{db_prefix}_factory.cost_allocation", 30),
    ],
)
