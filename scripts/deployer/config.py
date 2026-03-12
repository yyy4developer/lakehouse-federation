"""Constants, source definitions, and shared utilities.

Source definitions are loaded from deployer/sources/ directory.
Each resource has its own file — edit or add resources there.
"""

from __future__ import annotations

import random
import string
from dataclasses import dataclass, field
from pathlib import Path

from rich.console import Console

console = Console()

PROJECT_ROOT = Path(__file__).parent.parent.parent


def _read_version() -> str:
    try:
        for line in (PROJECT_ROOT / "pyproject.toml").read_text().splitlines():
            if line.startswith("version"):
                return line.split("=", 1)[1].strip().strip('"')
    except OSError:
        pass
    return "0.0.0"


VERSION = _read_version()
TERRAFORM_DIR = PROJECT_ROOT / "terraform"
NOTEBOOK_TEMPLATE = PROJECT_ROOT / "notebooks" / "federation_demo_template.sql"
NOTEBOOK_OUTPUT = PROJECT_ROOT / "notebooks" / "federation_demo.sql"
DEPLOY_STATE_FILE = PROJECT_ROOT / ".deploy_state.json"


@dataclass
class SourceDef:
    """Federation source definition.

    To add a new source, create a file in deployer/sources/ and register it
    in deployer/sources/__init__.py.
    """
    key: str
    label: str
    fed_type: str  # "catalog" or "query"
    cloud_req: str | None
    tables: list[str] = field(default_factory=list)
    sections: list[str] = field(default_factory=list)
    test_queries: list[tuple[str, str, int]] = field(default_factory=list)


def _load_sources() -> dict[str, SourceDef]:
    """Lazy-load sources from deployer/sources/ to avoid circular imports."""
    from .sources import ALL_SOURCES
    return ALL_SOURCES


# Lazy proxy — populated on first access
class _SourcesProxy(dict):
    _loaded = False

    def _ensure_loaded(self):
        if not self._loaded:
            self.update(_load_sources())
            self._loaded = True

    def __getitem__(self, key):
        self._ensure_loaded()
        return super().__getitem__(key)

    def __contains__(self, key):
        self._ensure_loaded()
        return super().__contains__(key)

    def __iter__(self):
        self._ensure_loaded()
        return super().__iter__()

    def __len__(self):
        self._ensure_loaded()
        return super().__len__()

    def items(self):
        self._ensure_loaded()
        return super().items()

    def values(self):
        self._ensure_loaded()
        return super().values()

    def keys(self):
        self._ensure_loaded()
        return super().keys()

    def get(self, key, default=None):
        self._ensure_loaded()
        return super().get(key, default)


SOURCES: dict[str, SourceDef] = _SourcesProxy()

# Chapter 2 cross-source JOIN section markers
CH2_SOURCE_MARKERS = {
    "postgres": "PostgreSQL: 保守履歴の統合",
    "synapse": "Synapse: シフト・エネルギーの統合",
    "bigquery": "BigQuery: 稼働停止・コスト分析",
}


def random_suffix(length: int = 4) -> str:
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=length))
    if not any(c.isdigit() for c in suffix):
        suffix = suffix[:length - 1] + random.choice(string.digits)
    return suffix
