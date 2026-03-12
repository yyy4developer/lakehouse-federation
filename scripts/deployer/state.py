"""Deploy state persistence."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

from .config import DEPLOY_STATE_FILE, console


@dataclass
class DeployState:
    workspace_url: str
    sources: list[str]
    query_prefix: str
    catalog_prefix: str
    analysis_catalog: str
    deployed_at: str = ""
    aws_profile: str = ""

    def save(self) -> None:
        data = {k: v for k, v in asdict(self).items() if v}
        if not data.get("deployed_at"):
            data["deployed_at"] = datetime.now(timezone.utc).isoformat()
        aws_profile = os.environ.get("AWS_PROFILE")
        if aws_profile:
            data["aws_profile"] = aws_profile
        DEPLOY_STATE_FILE.write_text(json.dumps(data, indent=2) + "\n")
        console.print(f"[green]✓[/green] Saved deploy state to {DEPLOY_STATE_FILE.name}")

    @classmethod
    def load(cls) -> DeployState | None:
        if not DEPLOY_STATE_FILE.exists():
            return None
        try:
            data = json.loads(DEPLOY_STATE_FILE.read_text())
            return cls(
                workspace_url=data["workspace_url"],
                sources=data.get("sources", []),
                query_prefix=data.get("query_prefix", "lhf_query"),
                catalog_prefix=data.get("catalog_prefix", "lhf_catalog"),
                analysis_catalog=data.get("analysis_catalog", "main"),
                deployed_at=data.get("deployed_at", ""),
                aws_profile=data.get("aws_profile", ""),
            )
        except (json.JSONDecodeError, OSError, KeyError):
            return None

    @classmethod
    def remove(cls) -> None:
        if DEPLOY_STATE_FILE.exists():
            DEPLOY_STATE_FILE.unlink()
            console.print("[green]✓[/green] Removed deploy state file\n")
