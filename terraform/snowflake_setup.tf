# =============================================================================
# Snowflake Query Federation - Database & Table Setup
# Creates database, schema, tables, and seed data via Python script
# =============================================================================

resource "null_resource" "snowflake_init" {
  count = var.enable_snowflake ? 1 : 0

  triggers = {
    db_name = local.snowflake_db_name
    schema  = local.snowflake_schema
  }

  provisioner "local-exec" {
    command = <<-EOT
      cd ${path.module}
      uv run --project ${abspath(path.module)}/.. python scripts/snowflake_setup.py \
        --account-url "${var.snowflake_account_url}" \
        --user "${var.snowflake_user}" \
        --password "${var.snowflake_password}" \
        --warehouse "${var.snowflake_warehouse}" \
        --database "${local.snowflake_db_name}" \
        --schema "${local.snowflake_schema}"
    EOT
  }
}
