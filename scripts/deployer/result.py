"""Deploy result markdown generation."""

from __future__ import annotations

from datetime import datetime, timezone

from .config import PROJECT_ROOT, SOURCES, console


class ResultGenerator:
    def generate(
        self,
        cloud: str,
        workspace_url: str,
        sources: list[str],
        query_prefix: str,
        catalog_prefix: str,
        terraform=None,
        databricks_client=None,
    ):
        outputs = terraform.get_outputs() if terraform else {}
        catalogs = outputs.get("databricks_catalogs", {})
        db_names = outputs.get("database_names", {})
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        aws_region = terraform.read_tfvar("aws_region") or "us-west-2" if terraform else "us-west-2"
        gcp_project_id = terraform.read_tfvar("gcp_project_id") if terraform else ""
        project_prefix = terraform.read_tfvar("project_prefix") or "lhf-demo" if terraform else "lhf-demo"

        sample_db = next(iter(db_names.values()), "lhf_demo_factory")
        db_prefix = sample_db.rsplit("_factory", 1)[0]

        nb_path = "/unknown"
        if databricks_client and databricks_client.token:
            nb_path = databricks_client.get_notebook_path()

        lines = [
            "# Lakehouse Federation Demo - Deploy Result",
            "",
            f"**Deployed at**: {now}",
            f"**Cloud**: {cloud}",
            f"**Workspace**: {workspace_url}",
            "",
            "---",
            "",
            "## Access Links",
            "",
            "### Databricks",
            "",
            "| Resource | URL |",
            "|----------|-----|",
            f"| Workspace | {workspace_url} |",
            f"| Demo Notebook | {workspace_url}/#workspace{nb_path} |",
            f"| Catalog Explorer | {workspace_url}/explore/data |",
        ]

        for src in sources:
            cat_name = catalogs.get(src)
            if cat_name:
                lines.append(f"| {SOURCES[src].label} Catalog | {workspace_url}/explore/data/{cat_name} |")

        # External source consoles
        lines += ["", "### External Source Consoles", ""]
        lines += ["| Source | Console / Query Editor |", "|--------|----------------------|"]
        self._add_console_links(lines, sources, outputs, db_names, aws_region, gcp_project_id, project_prefix, cloud, terraform)

        # Connection endpoints
        lines += ["", "### Connection Endpoints (CLI / JDBC)", ""]
        lines += ["| Source | Endpoint |", "|--------|----------|"]
        if "redshift" in sources and outputs.get("redshift_endpoint"):
            lines.append(f"| Redshift | `{outputs['redshift_endpoint']}:5439` |")
        if "postgres" in sources and outputs.get("postgres_endpoint"):
            lines.append(f"| PostgreSQL | `{outputs['postgres_endpoint']}:5432` |")
        if "synapse" in sources and outputs.get("synapse_endpoint"):
            lines.append(f"| Synapse | `{outputs['synapse_endpoint']}:1433` |")

        # Resource tree
        lines += ["", "---", "", "## Deployed Resource Tree", "", "```", "Unity Catalog"]
        self._add_catalog_federation_tree(lines, sources, catalogs, db_names, catalog_prefix, db_prefix)
        self._add_query_federation_tree(lines, sources, catalogs, db_names, query_prefix, db_prefix)
        lines += ["```", "", "---", "", "## Databricks Catalogs", ""]
        lines += ["| Source | Catalog Name | Database/Schema | Tables |", "|--------|-------------|-----------------|--------|"]

        for src in sources:
            src_def = SOURCES.get(src)
            if not src_def:
                continue
            cat = catalogs.get(src, "N/A")
            db = db_names.get(src, "N/A")
            tables = ", ".join(src_def.tables)
            lines.append(f"| {src_def.label} | `{cat}` | `{db}` | {tables} |")

        lines += [""]

        result_path = PROJECT_ROOT / "deploy_result.md"
        result_path.write_text("\n".join(lines) + "\n")
        console.print(f"[green]✓[/green] Generated {result_path}")
        return result_path

    @staticmethod
    def _add_console_links(lines, sources, outputs, db_names, aws_region, gcp_project_id, project_prefix, cloud, terraform):
        if "glue" in sources:
            glue_db = db_names.get("glue", "")
            lines.append(f"| AWS Glue | https://{aws_region}.console.aws.amazon.com/glue/home?region={aws_region}#/v2/data-catalog/databases/view/{glue_db} |")
            lines.append(f"| S3 (Glue Data) | https://s3.console.aws.amazon.com/s3/buckets/{outputs.get('s3_bucket_name', '')}?region={aws_region} |")
        if "redshift" in sources:
            lines.append(f"| Redshift Query Editor | https://{aws_region}.console.aws.amazon.com/sqlworkbench/home?region={aws_region}#/client |")
        if "postgres" in sources:
            if cloud == "aws":
                lines.append(f"| RDS (PostgreSQL) | https://{aws_region}.console.aws.amazon.com/rds/home?region={aws_region}#database:id={project_prefix}-postgres |")
            else:
                name_prefix = outputs.get("name_prefix", "")
                lines.append(f"| Azure PostgreSQL | https://portal.azure.com/#browse/Microsoft.DBforPostgreSQL%2FflexibleServers (search: {name_prefix}-postgres) |")
        if "synapse" in sources:
            synapse_ep = outputs.get("synapse_endpoint", "")
            synapse_ws_name = synapse_ep.replace("-ondemand.sql.azuresynapse.net", "") if synapse_ep else ""
            lines.append(f"| Azure Synapse Studio | https://web.azuresynapse.net?workspace={synapse_ws_name} |")
        if ("snowflake" in sources or "snowflake_iceberg" in sources) and terraform:
            sf_url = terraform.read_tfvar("snowflake_account_url")
            if sf_url:
                lines.append(f"| Snowflake | {sf_url} |")
        if "bigquery" in sources and gcp_project_id:
            bq_dataset = db_names.get("bigquery", "")
            lines.append(f"| BigQuery Console | https://console.cloud.google.com/bigquery?project={gcp_project_id}&d={bq_dataset}&p={gcp_project_id}&page=dataset |")

    @staticmethod
    def _add_catalog_federation_tree(lines, sources, catalogs, db_names, catalog_prefix, db_prefix):
        for src in ["glue", "onelake", "snowflake_iceberg"]:
            if src not in sources:
                continue
            src_def = SOURCES[src]
            cat_name = catalogs.get(src, f"{catalog_prefix}_{src}")
            db = db_prefix if src == "snowflake_iceberg" else db_names.get(src, "default")
            lines.append(f"├── {cat_name}  (Catalog Federation: {src_def.label})")
            lines.append(f"│   └── {db}")
            for i, t in enumerate(src_def.tables):
                connector = "├" if i < len(src_def.tables) - 1 else "└"
                lines.append(f"│       {connector}── {t}")

    @staticmethod
    def _add_query_federation_tree(lines, sources, catalogs, db_names, query_prefix, db_prefix):
        for src in ["redshift", "postgres", "synapse", "bigquery", "snowflake"]:
            if src not in sources:
                continue
            src_def = SOURCES[src]
            cat_name = catalogs.get(src, f"{query_prefix}_{src}")
            schema = db_names.get(src, "unknown") if src == "bigquery" else db_prefix
            lines.append(f"├── {cat_name}  (Query Federation: {src_def.label})")
            lines.append(f"│   └── {schema}")
            for i, t in enumerate(src_def.tables):
                connector = "├" if i < len(src_def.tables) - 1 else "└"
                lines.append(f"│       {connector}── {t}")
