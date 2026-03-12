"""PostgreSQL - Query Federation source."""

from ..config import SourceDef

source = SourceDef(
    key="postgres",
    label="PostgreSQL (Query Federation)",
    fed_type="query",
    cloud_req=None,
    tables=["machines", "maintenance_logs", "work_orders"],
    sections=["1.3 Query Federation: PostgreSQL"],
    test_queries=[
        ("maintenance_logs", "{query_prefix}_postgres.{db_prefix}.maintenance_logs", 30),
        ("work_orders", "{query_prefix}_postgres.{db_prefix}.work_orders", 25),
    ],
)
