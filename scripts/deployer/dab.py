"""Databricks Asset Bundle (DAB) deploy and destroy."""

from __future__ import annotations

import os
import subprocess
import time

from .config import PROJECT_ROOT, console


class DABManager:
    def deploy(self, workspace_url: str, databricks_client=None) -> None:
        console.print("[bold]DAB deployment...[/bold]\n")
        env = os.environ.copy()
        env["DATABRICKS_HOST"] = workspace_url

        for attempt in range(1, 4):
            result = subprocess.run(
                ["databricks", "bundle", "deploy", "--target", "dev"],
                cwd=PROJECT_ROOT, env=env, capture_output=True, text=True,
            )

            if result.returncode != 0:
                console.print(f"  [yellow]Attempt {attempt}/3 failed: {result.stderr.strip()}[/yellow]")
                if attempt < 3:
                    time.sleep(5)
                continue

            if databricks_client and databricks_client.token:
                try:
                    nb_path = databricks_client.get_notebook_path()
                    check = subprocess.run(
                        ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
                         "-G", f"{workspace_url}/api/2.0/workspace/get-status",
                         "-H", f"Authorization: Bearer {databricks_client.token}",
                         "--data-urlencode", f"path={nb_path}"],
                        capture_output=True, text=True, timeout=15,
                    )
                    if check.stdout.strip() == "200":
                        console.print("[green]✓ DAB deploy complete (notebook verified)[/green]\n")
                        return
                    console.print(f"  [yellow]Attempt {attempt}/3: deploy reported success but notebook not found[/yellow]")
                    if attempt < 3:
                        time.sleep(5)
                    continue
                except Exception:
                    pass

            console.print("[green]✓ DAB deploy complete[/green]\n")
            return

        console.print("[yellow]! DAB deploy failed after 3 attempts (non-critical)[/yellow]\n")

    @staticmethod
    def destroy(workspace_url: str) -> None:
        console.print("[bold]DAB destroy...[/bold]\n")
        env = os.environ.copy()
        env["DATABRICKS_HOST"] = workspace_url
        result = subprocess.run(
            ["databricks", "bundle", "destroy", "--target", "dev", "--auto-approve"],
            cwd=PROJECT_ROOT, env=env,
        )
        if result.returncode == 0:
            console.print("[green]✓ DAB destroy complete[/green]\n")
        else:
            console.print("[yellow]! DAB destroy failed (non-critical)[/yellow]\n")
