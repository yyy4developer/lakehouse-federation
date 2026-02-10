# =============================================================================
# Databricks External Location
# Governs access to the S3 path where Glue table data is stored.
# Required for Glue HMS Federation - the foreign catalog needs
# authorized paths covered by external locations.
#
# URL is set to the bucket root to cover:
#   - factory_master/ (Parquet, Delta, Iceberg data)
#   - glue_factory_metadata/ (foreign catalog storage_root)
# =============================================================================

resource "databricks_external_location" "glue_data" {
  name            = "${var.project_prefix}-glue-data"
  url             = "s3://${aws_s3_bucket.glue_data.id}"
  credential_name = databricks_storage_credential.glue_storage.name
  skip_validation = true # IAM role propagation may take a few seconds

  comment = "External location for Glue factory data - covers all formats (Parquet, Delta, Iceberg) and metadata"

  depends_on = [aws_iam_role.databricks_storage, aws_iam_role_policy.s3_read_access]
}
