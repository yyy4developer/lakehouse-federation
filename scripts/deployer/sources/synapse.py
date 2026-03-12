"""Azure Synapse - Query Federation source."""

from ..config import SourceDef

source = SourceDef(
    key="synapse",
    label="Azure Synapse (Query Federation)",
    fed_type="query",
    cloud_req=None,
    tables=["shift_schedules", "energy_consumption"],
    sections=["1.4 Query Federation: Azure Synapse"],
    test_queries=[
        ("shift_schedules", "{query_prefix}_synapse.{db_prefix}.shift_schedules", 40),
        ("energy_consumption", "{query_prefix}_synapse.{db_prefix}.energy_consumption", 50),
    ],
)
