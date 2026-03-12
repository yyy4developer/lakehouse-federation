# =============================================================================
# AWS Lake Formation Permissions
# Opt out of Lake Formation for Glue tables so they're accessible via IAM
# =============================================================================

# Set default permissions BEFORE any tables are created
# This ensures Glue ETL-created tables automatically get IAM_ALLOWED_PRINCIPALS
resource "aws_lakeformation_data_lake_settings" "opt_out" {
  count = var.enable_glue ? 1 : 0

  create_database_default_permissions {
    principal   = "IAM_ALLOWED_PRINCIPALS"
    permissions = ["ALL"]
  }
  create_table_default_permissions {
    principal   = "IAM_ALLOWED_PRINCIPALS"
    permissions = ["ALL"]
  }
}

resource "aws_lakeformation_permissions" "iam_database" {
  count = var.enable_glue ? 1 : 0

  principal   = "IAM_ALLOWED_PRINCIPALS"
  permissions = ["ALL"]

  database {
    name = aws_glue_catalog_database.factory_master[0].name
  }

  depends_on = [aws_lakeformation_data_lake_settings.opt_out]
}
