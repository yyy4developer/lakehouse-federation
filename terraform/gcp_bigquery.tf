# =============================================================================
# Google BigQuery Dataset & Tables
# =============================================================================

resource "google_bigquery_dataset" "factory" {
  count = var.enable_bigquery ? 1 : 0

  dataset_id = "factory_analytics"
  location   = "US"

  labels = {
    project = "lakehouse-federation-demo"
  }
}

resource "google_bigquery_table" "downtime_records" {
  count = var.enable_bigquery ? 1 : 0

  dataset_id          = google_bigquery_dataset.factory[0].dataset_id
  table_id            = "downtime_records"
  description         = "機械稼働停止記録 - 停止理由、カテゴリ、時間を管理"
  deletion_protection = false

  schema = jsonencode([
    { name = "record_id",  type = "INT64",     mode = "REQUIRED", description = "記録ID" },
    { name = "machine_id", type = "INT64",     mode = "REQUIRED", description = "機械ID (1-10)" },
    { name = "start_time", type = "TIMESTAMP", mode = "REQUIRED", description = "停止開始時刻" },
    { name = "end_time",   type = "TIMESTAMP", mode = "REQUIRED", description = "停止終了時刻" },
    { name = "reason",     type = "STRING",    mode = "REQUIRED", description = "停止理由" },
    { name = "category",   type = "STRING",    mode = "REQUIRED", description = "カテゴリ（計画停止/故障/保守/その他）" },
  ])
}

resource "google_bigquery_table" "cost_allocation" {
  count = var.enable_bigquery ? 1 : 0

  dataset_id          = google_bigquery_dataset.factory[0].dataset_id
  table_id            = "cost_allocation"
  description         = "機械別コスト配分 - カテゴリ別の費用を管理"
  deletion_protection = false

  schema = jsonencode([
    { name = "allocation_id",  type = "INT64",   mode = "REQUIRED", description = "配分ID" },
    { name = "machine_id",     type = "INT64",   mode = "REQUIRED", description = "機械ID (1-10)" },
    { name = "cost_category",  type = "STRING",  mode = "REQUIRED", description = "コストカテゴリ（電力/保守/消耗品/人件費）" },
    { name = "amount_usd",     type = "FLOAT64", mode = "REQUIRED", description = "金額（USD）" },
    { name = "fiscal_quarter", type = "STRING",  mode = "REQUIRED", description = "会計四半期（例: 2025-Q1）" },
  ])
}

# Insert data via bq CLI
resource "null_resource" "bigquery_init" {
  count = var.enable_bigquery ? 1 : 0

  triggers = {
    dataset_id = google_bigquery_dataset.factory[0].dataset_id
  }

  provisioner "local-exec" {
    command = <<-EOT
      echo "Inserting BigQuery data..."
      bq query --project_id="${var.gcp_project_id}" --use_legacy_sql=false \
        < ${path.module}/sql/bigquery/insert_downtime_records.sql
      bq query --project_id="${var.gcp_project_id}" --use_legacy_sql=false \
        < ${path.module}/sql/bigquery/insert_cost_allocation.sql
      echo "BigQuery initialization complete."
    EOT
  }

  depends_on = [
    google_bigquery_table.downtime_records,
    google_bigquery_table.cost_allocation,
  ]
}
