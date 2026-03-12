"""Snowflake Iceberg - Catalog Federation source.

Databricks reads Snowflake's Iceberg metadata via CONNECTION_SNOWFLAKE,
then accesses data directly from S3 (no Snowflake compute needed).
"""

from ..config import SourceDef

source = SourceDef(
    key="snowflake_iceberg",
    label="Snowflake Iceberg (Catalog Federation)",
    fed_type="catalog",
    cloud_req="aws",
    tables=["operational_metrics", "safety_incidents"],
    sections=["1.8 Catalog Federation: Snowflake Iceberg"],
    test_queries=[
        ("operational_metrics", "{catalog_prefix}_snowflake_iceberg.{db_prefix}.operational_metrics", 50),
        ("safety_incidents", "{catalog_prefix}_snowflake_iceberg.{db_prefix}.safety_incidents", 20),
    ],
)
