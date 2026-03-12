"""Snowflake - Query Federation source."""

from ..config import SourceDef

source = SourceDef(
    key="snowflake",
    label="Snowflake (Query Federation)",
    fed_type="query",
    cloud_req=None,
    tables=["equipment_specs", "spare_parts_inventory"],
    sections=["1.7 Query Federation: Snowflake"],
    test_queries=[
        ("equipment_specs", "{query_prefix}_snowflake.{db_prefix}.equipment_specs", 10),
        ("spare_parts_inventory", "{query_prefix}_snowflake.{db_prefix}.spare_parts_inventory", 30),
    ],
)
