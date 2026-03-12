"""Connectivity tests against deployed federation sources."""

from __future__ import annotations

from rich.table import Table

from .config import SOURCES, console


class ConnectivityTester:
    def run(
        self,
        databricks_client,
        sources: list[str],
        query_prefix: str,
        catalog_prefix: str,
        terraform=None,
    ) -> bool:
        # Derive db_prefix
        outputs = terraform.get_outputs() if terraform else {}
        db_names = outputs.get("database_names", {})
        sample_db = next(iter(db_names.values()), "lhf_demo_factory")
        db_prefix = sample_db.rsplit("_factory", 1)[0]

        console.print("\n")
        from rich.panel import Panel
        console.print(Panel("[bold cyan]Connectivity Test[/bold cyan]", border_style="cyan"))

        if not databricks_client.token:
            console.print("[red]Could not get Databricks token. Skipping tests.[/red]")
            return False
        if not databricks_client.warehouse_id:
            console.print("[red]No SQL warehouse found. Skipping tests.[/red]")
            return False

        console.print(f"  Using warehouse: {databricks_client.warehouse_id}\n")

        table = Table(title="Federation Source Tests")
        table.add_column("Source", style="cyan")
        table.add_column("Table")
        table.add_column("Expected", justify="right")
        table.add_column("Actual", justify="right")
        table.add_column("Status")

        all_passed = True

        for src in sources:
            src_def = SOURCES.get(src)
            if not src_def:
                continue
            for tbl_name, fqn_template, expected in src_def.test_queries:
                fqn = fqn_template.format(
                    query_prefix=query_prefix,
                    catalog_prefix=catalog_prefix,
                    db_prefix=db_prefix,
                )
                sql = f"SELECT count(*) AS cnt FROM {fqn}"

                try:
                    resp = databricks_client.execute_sql(sql)
                    state = resp.get("status", {}).get("state", "UNKNOWN")

                    if state == "SUCCEEDED":
                        data = resp.get("result", {}).get("data_array", [])
                        actual = int(data[0][0]) if data else 0
                        if actual == expected:
                            table.add_row(src, tbl_name, str(expected), str(actual), "[green]PASS[/green]")
                        else:
                            table.add_row(src, tbl_name, str(expected), str(actual), "[yellow]MISMATCH[/yellow]")
                            all_passed = False
                    else:
                        error_msg = resp.get("status", {}).get("error", {}).get("message", state)
                        table.add_row(src, tbl_name, str(expected), error_msg[:30], "[red]FAIL[/red]")
                        all_passed = False
                except Exception as e:
                    table.add_row(src, tbl_name, str(expected), str(e)[:30], "[red]ERROR[/red]")
                    all_passed = False

        console.print(table)

        if all_passed:
            console.print("\n[bold green]All connectivity tests passed![/bold green]")
        else:
            console.print("\n[bold red]Some tests failed. Check the table above.[/bold red]")

        return all_passed
