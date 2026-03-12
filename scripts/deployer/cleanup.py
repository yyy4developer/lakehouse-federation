"""Cleanup operations for Snowflake and Databricks notebook objects."""

from __future__ import annotations

from .config import console


class SnowflakeCleanup:
    def cleanup(self, terraform) -> None:
        console.print("\n[bold]Snowflake cleanup...[/bold]")
        sf_url = terraform.read_tfvar("snowflake_account_url") or ""
        sf_user = terraform.read_tfvar("snowflake_user") or ""
        sf_password = terraform.read_tfvar("snowflake_password") or ""
        sf_warehouse = terraform.read_tfvar("snowflake_warehouse") or ""

        if not all([sf_url, sf_user, sf_password]):
            console.print("[yellow]  Snowflake credentials not found in tfvars, skipping[/yellow]")
            return

        try:
            import snowflake.connector
            account = sf_url.replace("https://", "").replace(".snowflakecomputing.com", "").strip("/")
            conn = snowflake.connector.connect(
                account=account, user=sf_user, password=sf_password, warehouse=sf_warehouse,
            )
            cur = conn.cursor()

            # Drop all Iceberg tables in account
            cur.execute("SHOW ICEBERG TABLES IN ACCOUNT")
            for row in cur.fetchall():
                fqn = f"{row[2]}.{row[3]}.{row[1]}"
                console.print(f"  Dropping Iceberg table {fqn}...")
                cur.execute(f"DROP ICEBERG TABLE IF EXISTS {fqn}")

            # Drop external volume and storage integration
            cur.execute("DROP EXTERNAL VOLUME IF EXISTS LHF_ICEBERG_VOLUME")
            cur.execute("DROP STORAGE INTEGRATION IF EXISTS LHF_S3_INTEGRATION")

            # Drop databases matching LHF_*_FACTORY or LHF_*_ICEBERG pattern
            cur.execute("SHOW DATABASES")
            for row in cur.fetchall():
                db_name = row[1]
                if db_name.startswith("LHF_") and (db_name.endswith("_FACTORY") or db_name.endswith("_ICEBERG")):
                    console.print(f"  Dropping database {db_name}...")
                    cur.execute(f"DROP DATABASE IF EXISTS {db_name}")

            conn.close()
            console.print("[green]✓ Snowflake cleanup complete[/green]\n")
        except Exception as e:
            console.print(f"[yellow]  Snowflake cleanup failed: {e}[/yellow]\n")


class NotebookCleanup:
    def cleanup(self, databricks_client, analysis_catalog: str, db_prefix: str) -> None:
        console.print("\n[bold]Cleaning up notebook-created objects...[/bold]")

        if not databricks_client.token:
            console.print("[yellow]! Could not get token. Skipping cleanup.[/yellow]")
            return
        if not databricks_client.warehouse_id:
            console.print("[yellow]! No SQL warehouse found. Skipping cleanup.[/yellow]")
            return

        cleanup_sqls = [
            (f"DROP TABLE IF EXISTS {analysis_catalog}.{db_prefix}.factory_operations_union", "factory_operations_union"),
            (f"DROP TABLE IF EXISTS {analysis_catalog}.{db_prefix}.machine_health_summary", "machine_health_summary"),
            (f"DROP SCHEMA IF EXISTS {analysis_catalog}.{db_prefix} CASCADE", f"{db_prefix} schema"),
            (f"DROP CATALOG IF EXISTS {analysis_catalog} CASCADE", f"{analysis_catalog} catalog"),
        ]

        for sql, label in cleanup_sqls:
            try:
                resp = databricks_client.execute_sql(sql)
                status = resp.get("status", {}).get("state", "")
                if status == "SUCCEEDED":
                    console.print(f"  [green]✓[/green] Dropped {label}")
                else:
                    err = resp.get("status", {}).get("error", {}).get("message", "unknown")
                    console.print(f"  [yellow]![/yellow] {label}: {err}")
            except Exception as e:
                console.print(f"  [yellow]![/yellow] {label}: {e}")
