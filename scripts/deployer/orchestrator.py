"""Main orchestrator: deploy, redeploy, destroy flows."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from urllib.parse import urlparse

import questionary
from rich.panel import Panel
from rich.table import Table

from .auth import AuthManager, CredentialCollector
from .cleanup import NotebookCleanup, SnowflakeCleanup
from .config import SOURCES, VERSION, console, random_suffix
from .connectivity import ConnectivityTester
from .dab import DABManager
from .databricks import DatabricksClient
from .notebook import NotebookGenerator
from .result import ResultGenerator
from .state import DeployState
from .terraform import TerraformManager


class Deployer:
    def __init__(self) -> None:
        self.terraform = TerraformManager()
        self.auth = AuthManager()
        self.cred_collector = CredentialCollector()
        self.notebook_gen = NotebookGenerator()
        self.dab = DABManager()
        self.connectivity = ConnectivityTester()
        self.result_gen = ResultGenerator()
        self.snowflake_cleanup = SnowflakeCleanup()
        self.notebook_cleanup = NotebookCleanup()

    # ---- Interactive deploy ----

    def deploy(self) -> None:
        self._print_banner()

        cloud = self._select_cloud()
        workspace_url = self._get_workspace_url()
        sources = self._select_sources(cloud)
        query_prefix, catalog_prefix, analysis_catalog, resource_suffix = self._get_catalog_prefix()
        aws_region, azure_region = self._select_regions(cloud, sources)

        auto_creds = self.auth.check_cloud_auth(cloud, sources)
        dbx = DatabricksClient(workspace_url, terraform=self.terraform)
        dbx.setup_auth()
        creds = self.cred_collector.collect(cloud, sources, auto_creds)

        # Confirm
        console.print("\n[bold]Configuration Summary:[/bold]")
        console.print(f"  Cloud: {cloud}")
        console.print(f"  Workspace: {workspace_url}")
        console.print(f"  Sources: {', '.join(sources)}")
        console.print(f"  Prefixes: {query_prefix} / {catalog_prefix}")
        console.print(f"  Analysis catalog: {analysis_catalog}")
        console.print(f"  AWS region: {aws_region}")
        if any(s in sources for s in ("synapse", "postgres", "onelake")) or cloud == "azure":
            console.print(f"  Azure region: {azure_region}")

        if not questionary.confirm("\nデプロイを開始しますか?", default=True).ask():
            console.print("[yellow]Cancelled.[/yellow]")
            sys.exit(0)

        # Detect cross-tenant storage for Azure
        ws_storage = ""
        if cloud == "azure":
            ws_storage = dbx.detect_workspace_default_storage(creds.get("azure_subscription_id", ""))

        self.terraform.generate_tfvars(
            cloud, workspace_url, sources, query_prefix, catalog_prefix, analysis_catalog,
            creds, aws_region=aws_region, azure_region=azure_region,
            workspace_default_storage_url=ws_storage,
            resource_suffix=resource_suffix,
        )
        self.notebook_gen.generate(sources, query_prefix, catalog_prefix, analysis_catalog=analysis_catalog)
        DeployState(workspace_url, sources, query_prefix, catalog_prefix, analysis_catalog).save()
        self.terraform.init_plan_apply()
        self.dab.deploy(workspace_url, databricks_client=dbx)
        self._run_notebook_job(dbx, query_prefix, catalog_prefix)
        self._print_summary(cloud, workspace_url, sources, query_prefix, catalog_prefix, dbx)

    # ---- Non-interactive redeploy ----

    def redeploy(self) -> None:
        self._print_banner()

        cloud = self.terraform.read_tfvar("cloud")
        workspace_url = self.terraform.read_tfvar("databricks_host")
        if not workspace_url:
            console.print("[red]No terraform.tfvars found. Run interactive deploy first.[/red]")
            sys.exit(1)

        query_prefix = self.terraform.read_tfvar("catalog_prefix_query") or "lhf_query"
        catalog_prefix = self.terraform.read_tfvar("catalog_prefix_catalog") or "lhf_catalog"
        sources = self.terraform.read_sources()

        state = DeployState.load()
        analysis_catalog = (state.analysis_catalog if state else "") or self.terraform.read_tfvar("analysis_catalog") or "main"

        console.print(f"[bold]Redeploy (non-interactive)[/bold]")
        console.print(f"  Cloud: {cloud}")
        console.print(f"  Workspace: {workspace_url}")
        console.print(f"  Sources: {', '.join(sources)}")
        console.print(f"  Prefixes: {query_prefix} / {catalog_prefix}")
        console.print(f"  Analysis catalog: {analysis_catalog}\n")

        if state and state.aws_profile and not os.environ.get("AWS_PROFILE"):
            os.environ["AWS_PROFILE"] = state.aws_profile
            console.print(f"  AWS Profile: {state.aws_profile}")

        self.auth.ensure_aws_auth(sources)
        self.auth.ensure_azure_auth(sources)
        dbx = DatabricksClient(workspace_url, terraform=self.terraform)
        dbx.setup_auth()

        self.notebook_gen.generate(sources, query_prefix, catalog_prefix, analysis_catalog=analysis_catalog)
        DeployState(workspace_url, sources, query_prefix, catalog_prefix, analysis_catalog).save()
        self.terraform.init_plan_apply()
        self.dab.deploy(workspace_url, databricks_client=dbx)
        self.connectivity.run(dbx, sources, query_prefix, catalog_prefix, terraform=self.terraform)
        self._run_notebook_job(dbx, query_prefix, catalog_prefix)
        self.result_gen.generate(cloud, workspace_url, sources, query_prefix, catalog_prefix,
                                 terraform=self.terraform, databricks_client=dbx)
        console.print("\n[bold green]Redeploy complete.[/bold green]")

    # ---- Destroy ----

    def destroy(self) -> None:
        console.print(Panel.fit(
            "[bold red]Lakehouse Federation Demo - Destroy[/bold red]\n"
            "[dim]Removing all deployed resources[/dim]",
            border_style="red",
        ))

        state = DeployState.load()
        if not state:
            console.print("[yellow]No deploy state found (.deploy_state.json).[/yellow]")
            console.print("[dim]Attempting destroy with terraform state only...[/dim]\n")
            subprocess.run(["terraform", "destroy", "-auto-approve"], cwd=str(self.terraform._tfvars_path.parent))
            return

        workspace_url = state.workspace_url
        sources = state.sources
        db_prefix = state.query_prefix.removesuffix("_query")

        if state.aws_profile and not os.environ.get("AWS_PROFILE"):
            os.environ["AWS_PROFILE"] = state.aws_profile
            console.print(f"  AWS Profile: {state.aws_profile}")

        console.print(f"  Workspace: {workspace_url}")
        console.print(f"  Analysis catalog: {state.analysis_catalog}")
        console.print(f"  Sources: {', '.join(sources)}")
        console.print(f"  Deployed at: {state.deployed_at}\n")

        self.auth.ensure_aws_auth(sources)
        self.auth.ensure_azure_auth(sources)

        if sys.stdin.isatty():
            if not questionary.confirm("全リソースを削除しますか?", default=False).ask():
                console.print("[yellow]Cancelled.[/yellow]")
                return
        else:
            console.print("[dim]Non-interactive mode: proceeding with destroy[/dim]")

        # 1. Databricks auth
        dbx = DatabricksClient(workspace_url, terraform=self.terraform)
        dbx.setup_auth()

        # 2. Cleanup notebook objects
        self.notebook_cleanup.cleanup(dbx, state.analysis_catalog, db_prefix)

        # 3. Snowflake cleanup
        if "snowflake_iceberg" in sources or "snowflake" in sources:
            self.snowflake_cleanup.cleanup(self.terraform)

        # 4. Terraform destroy
        self.terraform.destroy()

        # 5. DAB destroy
        self.dab.destroy(workspace_url)

        # 6. Remove state
        DeployState.remove()

        console.print("[bold green]Destroy complete.[/bold green]")

    # ---- Notebook job execution ----

    def _run_notebook_job(self, dbx: DatabricksClient, query_prefix: str, catalog_prefix: str) -> None:
        console.print("[bold]Running demo notebook...[/bold]\n")

        if not dbx.token:
            console.print("[yellow]! Could not get token. Skipping notebook run.[/yellow]\n")
            return
        if not dbx.warehouse_id:
            console.print("[yellow]! No SQL warehouse found. Skipping notebook run.[/yellow]\n")
            return

        notebook_path = dbx.get_notebook_path()
        job_payload = {
            "run_name": "Federation Demo - Validation Run",
            "tasks": [{
                "task_key": "run_demo",
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "source": "WORKSPACE",
                    "warehouse_id": dbx.warehouse_id,
                },
            }],
        }

        try:
            result = subprocess.run(
                ["curl", "-s", "-X", "POST",
                 f"{dbx.workspace_url}/api/2.1/jobs/runs/submit",
                 "-H", f"Authorization: Bearer {dbx.token}",
                 "-H", "Content-Type: application/json",
                 "-d", json.dumps(job_payload)],
                capture_output=True, text=True, timeout=30,
            )
            resp = json.loads(result.stdout)
            run_id = resp.get("run_id")

            if not run_id:
                console.print(f"[yellow]! Job submit failed: {resp}[/yellow]\n")
                return

            console.print(f"  Run ID: {run_id}")
            console.print(f"  URL: {dbx.workspace_url}/#job/run/{run_id}\n")

            for _ in range(60):
                time.sleep(10)
                poll = subprocess.run(
                    ["curl", "-s",
                     f"{dbx.workspace_url}/api/2.1/jobs/runs/get?run_id={run_id}",
                     "-H", f"Authorization: Bearer {dbx.token}"],
                    capture_output=True, text=True, timeout=15,
                )
                run_info = json.loads(poll.stdout)
                run_state = run_info.get("state", {})
                life_cycle = run_state.get("life_cycle_state", "")
                result_state = run_state.get("result_state", "")

                if life_cycle == "TERMINATED":
                    if result_state == "SUCCESS":
                        console.print("[green]✓ Notebook run completed successfully[/green]\n")
                    else:
                        msg = run_state.get("state_message", "")
                        console.print(f"[yellow]! Notebook run finished: {result_state} - {msg}[/yellow]\n")
                    return
                elif life_cycle in ("INTERNAL_ERROR", "SKIPPED"):
                    console.print(f"[red]✗ Notebook run failed: {life_cycle}[/red]\n")
                    return

            console.print("[yellow]! Notebook run timed out (10 min). Check manually.[/yellow]\n")
        except Exception as e:
            console.print(f"[yellow]! Notebook job error: {e}[/yellow]\n")

    # ---- Summary ----

    def _print_summary(
        self, cloud: str, workspace_url: str, sources: list[str],
        query_prefix: str, catalog_prefix: str, dbx: DatabricksClient,
    ) -> None:
        console.print("\n")
        console.print(Panel("[bold green]Deploy Complete![/bold green]", border_style="green"))

        table = Table(title="Deployed Resources")
        table.add_column("Source", style="cyan")
        table.add_column("Type")
        table.add_column("Status", style="green")
        for s in sources:
            src_def = SOURCES[s]
            table.add_row(src_def.label, src_def.fed_type, "✓ deployed")
        console.print(table)

        nb_path = dbx.get_notebook_path() if dbx.token else "/unknown"
        console.print(f"\n[bold]Databricks Workspace:[/bold] {workspace_url}")
        console.print(f"[bold]Demo Notebook:[/bold] {workspace_url}/#workspace{nb_path}")

        self.connectivity.run(dbx, sources, query_prefix, catalog_prefix, terraform=self.terraform)

        result_path = self.result_gen.generate(
            cloud, workspace_url, sources, query_prefix, catalog_prefix,
            terraform=self.terraform, databricks_client=dbx,
        )
        console.print(f"\n[bold]Deploy Result:[/bold] {result_path}")
        self.terraform.print_outputs()

    # ---- UI helpers ----

    @staticmethod
    def _print_banner() -> None:
        console.print(Panel.fit(
            "[bold cyan]Lakehouse Federation Demo[/bold cyan]\n"
            f"[dim]Multi-cloud federation deployment tool  v{VERSION}[/dim]",
            border_style="cyan",
        ))

    @staticmethod
    def _select_cloud() -> str:
        return questionary.select(
            "Databricks workspace のクラウドを選択:",
            choices=[
                questionary.Choice("AWS", value="aws"),
                questionary.Choice("Azure", value="azure"),
            ],
        ).ask()

    @staticmethod
    def _get_workspace_url() -> str:
        url = questionary.text(
            "Databricks workspace URL を入力 (例: https://fevm-xxx.cloud.databricks.com):",
        ).ask()

        if not url:
            console.print("\n[yellow]Workspace がない場合は FEVM で作成できます:[/yellow]")
            console.print("  Claude Code で [bold]/databricks-fe-vm-workspace-deployment[/bold] を実行")
            console.print("  AWS: aws_sandbox_serverless テンプレート")
            console.print("  Azure: azure_sandbox_classic テンプレート\n")
            url = questionary.text(
                "Workspace URL を入力 (必須):",
                validate=lambda x: len(x) > 0 or "URL は必須です",
            ).ask()

        parsed = urlparse(url.strip())
        clean = f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
        if clean != url.strip().rstrip("/"):
            console.print(f"  [dim]URL をクリーンアップ: {clean}[/dim]")
        return clean

    @staticmethod
    def _select_sources(cloud: str) -> list[str]:
        choices = []
        for key, src_def in SOURCES.items():
            if src_def.cloud_req and src_def.cloud_req != cloud:
                continue
            choices.append(questionary.Choice(src_def.label, value=key))

        return questionary.checkbox(
            "有効にする Federation ソースを選択:",
            choices=choices,
            validate=lambda x: len(x) > 0 or "少なくとも1つ選択してください",
        ).ask()

    @staticmethod
    def _get_catalog_prefix() -> tuple[str, str, str, str]:
        """Returns (query_prefix, catalog_prefix, analysis_catalog, resource_suffix)."""
        suffix = random_suffix()
        default_prefix = f"lhf_{suffix}"
        console.print(f"[dim]  自動生成 prefix: {default_prefix} (変更可)[/dim]")
        prefix = questionary.text("カタログ名の共通 prefix:", default=default_prefix).ask()

        # Extract suffix from prefix for Terraform resource naming
        # e.g. "lhf_a1b2_yao" → "a1b2", "lhf_a1b2" → "a1b2"
        parts = prefix.split("_")
        resource_suffix = parts[1] if len(parts) >= 2 else suffix

        query_prefix = f"{prefix}_query"
        catalog_prefix = f"{prefix}_catalog"
        analysis_catalog = f"{prefix}_union_dbx"

        console.print(f"  Query Federation:   {query_prefix}_*")
        console.print(f"  Catalog Federation: {catalog_prefix}_*")
        console.print(f"  分析結果カタログ:   {analysis_catalog}")
        console.print(f"  リソース suffix:    {resource_suffix}")

        return query_prefix, catalog_prefix, analysis_catalog, resource_suffix

    @staticmethod
    def _select_regions(cloud: str, sources: list[str]) -> tuple[str, str]:
        default_aws = "us-west-2"
        default_azure = "westus2"

        aws_region = questionary.text(f"AWS リージョン (空白 Enter = {default_aws}):", default="").ask()
        aws_region = aws_region.strip() or default_aws

        need_azure = any(s in sources for s in ("synapse", "postgres", "onelake")) or cloud == "azure"
        if need_azure:
            azure_region = questionary.text(f"Azure リージョン (空白 Enter = {default_azure}):", default="").ask()
            azure_region = azure_region.strip() or default_azure
        else:
            azure_region = default_azure

        return aws_region, azure_region
