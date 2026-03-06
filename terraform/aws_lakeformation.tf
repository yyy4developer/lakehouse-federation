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
