"""Databricks API client: authentication, SQL execution, warehouse management."""

from __future__ import annotations

import json
import subprocess
import sys
import time

from .config import console


class DatabricksClient:
    def __init__(self, workspace_url: str, terraform=None) -> None:
        self.workspace_url = workspace_url
        self._terraform = terraform
        self._token: str | None = None
        self._warehouse_id: str | None = None

    def setup_auth(self) -> None:
        console.print("\n[bold]Databricks OAuth 認証...[/bold]")
        console.print(f"  Workspace: {self.workspace_url}")

        result = subprocess.run(
            ["databricks", "auth", "token", "--host", self.workspace_url],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            console.print("  [green]✓[/green] OAuth 認証済み")
            return

        console.print("  [yellow]OAuth ログインが必要です。ブラウザが開きます。[/yellow]")
        result = subprocess.run(
            ["databricks", "auth", "login", "--host", self.workspace_url],
            timeout=120,
        )
        if result.returncode != 0:
            console.print("  [red]✗ OAuth 認証に失敗しました[/red]")
            sys.exit(1)
        console.print("  [green]✓[/green] OAuth 認証完了")

    @property
    def token(self) -> str | None:
        if self._token is None:
            self._token = self._fetch_token()
        return self._token

    def _fetch_token(self) -> str | None:
        try:
            result = subprocess.run(
                ["databricks", "auth", "token", "--host", self.workspace_url],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0:
                return json.loads(result.stdout).get("access_token")
        except Exception:
            pass
        return None

    @property
    def warehouse_id(self) -> str | None:
        if self._warehouse_id is None:
            self._warehouse_id = self._find_warehouse()
        return self._warehouse_id

    def _find_warehouse(self) -> str | None:
        try:
            result = subprocess.run(
                ["curl", "-s", f"{self.workspace_url}/api/2.0/sql/warehouses",
                 "-H", f"Authorization: Bearer {self.token}"],
                capture_output=True, text=True, timeout=15,
            )
            warehouses = json.loads(result.stdout).get("warehouses", [])

            for w in warehouses:
                if w.get("state") == "RUNNING":
                    return w["id"]

            for w in warehouses:
                if w.get("state") == "STOPPED":
                    return self._start_warehouse(w["id"], w.get("name", w["id"]))

            if warehouses:
                return warehouses[0]["id"]
        except Exception:
            pass
        return None

    def _start_warehouse(self, wh_id: str, name: str) -> str:
        console.print(f"  Starting stopped warehouse {name}...")
        subprocess.run(
            ["curl", "-s", "-X", "POST",
             f"{self.workspace_url}/api/2.0/sql/warehouses/{wh_id}/start",
             "-H", f"Authorization: Bearer {self.token}"],
            capture_output=True, text=True, timeout=15,
        )
        for _ in range(24):
            time.sleep(5)
            check = subprocess.run(
                ["curl", "-s",
                 f"{self.workspace_url}/api/2.0/sql/warehouses/{wh_id}",
                 "-H", f"Authorization: Bearer {self.token}"],
                capture_output=True, text=True, timeout=15,
            )
            state = json.loads(check.stdout).get("state", "")
            if state == "RUNNING":
                console.print(f"  [green]✓[/green] Warehouse started.")
                return wh_id
            if state in ("DELETED", "DELETING"):
                break
        console.print(f"  [yellow]! Warehouse did not start in time.[/yellow]")
        return wh_id

    def execute_sql(self, sql: str) -> dict:
        result = subprocess.run(
            ["curl", "-s", "-X", "POST",
             f"{self.workspace_url}/api/2.0/sql/statements",
             "-H", f"Authorization: Bearer {self.token}",
             "-H", "Content-Type: application/json",
             "-d", json.dumps({
                 "statement": sql,
                 "warehouse_id": self.warehouse_id,
                 "wait_timeout": "50s",
                 "on_wait_timeout": "CANCEL",
             })],
            capture_output=True, text=True, timeout=90,
        )
        return json.loads(result.stdout)

    def get_notebook_path(self, cloud: str = "") -> str:
        try:
            result = subprocess.run(
                ["curl", "-s", f"{self.workspace_url}/api/2.0/preview/scim/v2/Me",
                 "-H", f"Authorization: Bearer {self.token}"],
                capture_output=True, text=True, timeout=15,
            )
            user_name = json.loads(result.stdout).get("userName", "unknown")
        except Exception:
            user_name = "unknown"

        if not cloud and self._terraform:
            cloud = self._terraform.read_tfvar("cloud")
        suffix = cloud if cloud in ("aws", "azure") else "azure"
        return f"/Users/{user_name}/.bundle/lakehouse_federation_demo/files/notebooks/federation_demo_{suffix}"

    def detect_workspace_default_storage(self, azure_subscription_id: str) -> str:
        """Detect cross-tenant workspace default storage URL."""
        try:
            import urllib.request
            req = urllib.request.Request(
                f"{self.workspace_url.rstrip('/')}/api/2.1/unity-catalog/storage-credentials",
                headers={"Authorization": f"Bearer {self.token}"},
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())

            for cred in data.get("storage_credentials", []):
                ami = cred.get("azure_managed_identity", {})
                ac_id = ami.get("access_connector_id", "")
                if ac_id and azure_subscription_id and azure_subscription_id not in ac_id:
                    pf = cred.get("path_filters", {}).get("allowlist", {}).get("path_prefixes", [])
                    if pf:
                        console.print(
                            f"\n[yellow]⚠[/yellow] ワークスペースが異なるAzureテナントにあります。"
                            f"\n  ワークスペースのデフォルトストレージを使用します。"
                        )
                        return pf[0].rstrip("/")
        except Exception:
            pass
        return ""
