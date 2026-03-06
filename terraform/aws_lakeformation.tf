# =============================================================================
# AWS Lake Formation Permissions
# Grants IAM_ALLOWED_PRINCIPALS on databases to opt out of Lake Formation
# =============================================================================

resource "aws_lakeformation_permissions" "iam_database" {
  count = var.enable_glue ? 1 : 0

  principal   = "IAM_ALLOWED_PRINCIPALS"
  permissions = ["ALL"]

  database {
    name = aws_glue_catalog_database.factory_master[0].name
  }
}

# Opt out of Lake Formation for this database entirely
# This ensures all tables (including those created by Glue ETL) are accessible via IAM
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
