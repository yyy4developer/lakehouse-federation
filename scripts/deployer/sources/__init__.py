"""Per-resource federation source definitions.

Each source is defined in its own module file. To add a new source:
1. Create a new file (e.g., mysql.py) with a SourceDef instance
2. Import and register it in this __init__.py
3. Add corresponding Terraform files in terraform/

The SourceDef dataclass holds everything needed for deploy, test, notebook, and result generation.
"""

from __future__ import annotations

from ..config import SourceDef

# Import all source definitions
from .bigquery import source as bigquery
from .glue import source as glue
from .onelake import source as onelake
from .postgres import source as postgres
from .redshift import source as redshift
from .snowflake import source as snowflake
from .snowflake_iceberg import source as snowflake_iceberg
from .synapse import source as synapse

# Registry: add new sources here
ALL_SOURCES: dict[str, SourceDef] = {
    "glue": glue,
    "redshift": redshift,
    "postgres": postgres,
    "synapse": synapse,
    "bigquery": bigquery,
    "onelake": onelake,
    "snowflake": snowflake,
    "snowflake_iceberg": snowflake_iceberg,
}
