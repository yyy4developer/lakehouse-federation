"""Notebook generation from template."""

from __future__ import annotations

from .config import (
    CH2_SOURCE_MARKERS,
    NOTEBOOK_OUTPUT,
    NOTEBOOK_TEMPLATE,
    SOURCES,
    console,
)


class NotebookGenerator:
    def generate(
        self,
        sources: list[str],
        query_prefix: str = "lhf_query",
        catalog_prefix: str = "lhf_catalog",
        analysis_catalog: str = "main",
    ) -> None:
        db_prefix = query_prefix.removesuffix("_query")
        template = NOTEBOOK_TEMPLATE.read_text()

        # Inject actual values into DECLARE defaults
        for old, new in {
            "query_prefix STRING DEFAULT 'lhf_query'": f"query_prefix STRING DEFAULT '{query_prefix}'",
            "catalog_prefix STRING DEFAULT 'lhf_catalog'": f"catalog_prefix STRING DEFAULT '{catalog_prefix}'",
            "db_prefix STRING DEFAULT 'lhf_demo'": f"db_prefix STRING DEFAULT '{db_prefix}'",
            "analysis_catalog STRING DEFAULT 'main'": f"analysis_catalog STRING DEFAULT '{analysis_catalog}'",
        }.items():
            template = template.replace(old, new)

        commands = template.split("-- COMMAND ----------")
        output_commands = []
        current_source = None
        skip_source = False

        for cmd in commands:
            new_source = None
            for src_key, src_def in SOURCES.items():
                if any(p in cmd for p in src_def.sections):
                    new_source = src_key
                    break

            if new_source:
                current_source = new_source
                skip_source = new_source not in sources
                if skip_source:
                    continue
                output_commands.append(cmd)
            elif current_source and skip_source:
                if any(m in cmd for m in ["# 第2章:", "# 第3章:", "# 第4章:"]):
                    current_source = None
                    skip_source = False
                    output_commands.append(cmd)
            else:
                if self._should_skip_command(cmd, sources):
                    continue
                output_commands.append(cmd)

        job_content = "-- COMMAND ----------".join(output_commands)
        job_content = self._handle_glue_substitutions(job_content, sources)

        NOTEBOOK_OUTPUT.write_text(job_content)

        console.print(f"[green]✓[/green] Generated notebook with {len(sources)} source(s): {', '.join(sources)}")

    @staticmethod
    def _should_skip_command(cmd: str, sources: list[str]) -> bool:
        # Skip source-specific cross-source JOINs in chapter 2
        if any(marker in cmd and src not in sources for src, marker in CH2_SOURCE_MARKERS.items()):
            return True

        # Skip machine_health_summary if dependencies missing
        if "machine_health_summary" in cmd and ("CREATE OR REPLACE TABLE" in cmd or "ORDER BY sensor_critical_count" in cmd):
            if "redshift" not in sources:
                return True
            if "glue" not in sources and "postgres" not in sources:
                return True

        # Skip factory_operations_union if not enough sources
        if "factory_operations_union" in cmd:
            if not {"redshift", "postgres", "synapse", "bigquery"}.issubset(set(sources)):
                return True

        # Remove cross-source JOIN header if no extra sources
        if "追加ソースのクロスソース" in cmd:
            if not any(s in sources for s in ["postgres", "synapse", "bigquery"]):
                return True

        return False

    @staticmethod
    def _handle_glue_substitutions(content: str, sources: list[str]) -> str:
        if "glue" in sources:
            return content

        if "postgres" in sources:
            content = content.replace(
                "catalog_prefix || '_glue.' || db_prefix || '_factory_master.machines",
                "query_prefix || '_postgres.' || db_prefix || '.machines",
            )

        if "redshift" in sources:
            content = content.replace(
                "  SELECT machine_id, result, defect_count FROM ' || catalog_prefix || '_glue.' || db_prefix || '_factory_master.quality_inspections\n  UNION ALL\n",
                "",
            )

        content = content.replace(
            "catalog_prefix || '_glue.' || db_prefix || '_factory_master.quality_inspections",
            "query_prefix || '_redshift.' || db_prefix || '.quality_inspections" if "redshift" in sources else "",
        )
        return content

