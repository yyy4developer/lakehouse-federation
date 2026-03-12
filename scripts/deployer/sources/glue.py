"""AWS Glue - Catalog Federation source."""

from ..config import SourceDef

source = SourceDef(
    key="glue",
    label="AWS Glue (Catalog Federation)",
    fed_type="catalog",
    cloud_req="aws",
    tables=["sensors", "machines", "quality_inspections"],
    sections=["1.1 Catalog Federation: AWS Glue"],
    test_queries=[
        ("sensors", "{catalog_prefix}_glue.{db_prefix}_factory_master.sensors", 20),
        ("machines", "{catalog_prefix}_glue.{db_prefix}_factory_master.machines", 10),
        ("quality_inspections", "{catalog_prefix}_glue.{db_prefix}_factory_master.quality_inspections", 50),
    ],
)
