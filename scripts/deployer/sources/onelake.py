"""Microsoft OneLake / Fabric - Catalog Federation source."""

from ..config import SourceDef

source = SourceDef(
    key="onelake",
    label="OneLake / Fabric (Catalog Federation)",
    fed_type="catalog",
    cloud_req="azure",
    tables=["production_plans", "inventory_levels"],
    sections=["1.6 Catalog Federation: Microsoft OneLake"],
    test_queries=[
        ("production_plans", "{catalog_prefix}_onelake.default.production_plans", 20),
        ("inventory_levels", "{catalog_prefix}_onelake.default.inventory_levels", 30),
    ],
)
