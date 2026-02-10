# =============================================================================
# Databricks Foreign Catalogs
# Mirror external data sources in Unity Catalog
# =============================================================================

# -----------------------------------------------------------------------------
# Foreign Catalog: AWS Glue (factory master data)
# Mirrors the Glue database as a Unity Catalog catalog.
# Authorized paths restrict which S3 locations can be accessed.
# storage_root provides a location for foreign catalog metadata.
# -----------------------------------------------------------------------------

resource "databricks_catalog" "glue" {
  name            = "glue_factory"
  connection_name = databricks_connection.glue.name

  options = {
    authorized_paths = "s3://${aws_s3_bucket.glue_data.id}"
  }

  storage_root = "s3://${aws_s3_bucket.glue_data.id}/glue_factory_metadata"

  comment = "外部カタログ: AWS Glue 工場マスタデータ（sensors [Parquet], machines [Delta], quality_inspections [Iceberg]）"

  depends_on = [databricks_external_location.glue_data]
}

# -----------------------------------------------------------------------------
# Foreign Catalog: Redshift Serverless (factory transaction data)
# Mirrors the Redshift database as a Unity Catalog catalog.
# -----------------------------------------------------------------------------

resource "databricks_catalog" "redshift" {
  name            = "redshift_factory"
  connection_name = databricks_connection.redshift.name

  options = {
    database = "factory_db"
  }

  comment = "外部カタログ: Redshift 工場トランザクションデータ（sensor_readings, production_events, quality_inspections）"
}
