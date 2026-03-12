# =============================================================================
# Snowflake Iceberg Catalog Federation (via AWS Glue CATALOG_SYNC)
# Requires: enable_glue = true (reuses S3 bucket + Glue catalog)
# =============================================================================

# Glue database for Snowflake Iceberg tables (separate from Glue ETL database)
resource "aws_glue_catalog_database" "snowflake_iceberg" {
  count       = var.enable_snowflake_iceberg ? 1 : 0
  name        = local.snowflake_iceberg_glue_db
  description = "Snowflake Iceberg tables (CATALOG_SYNC via Glue)"
}

# IAM Role for Snowflake to access S3 + Glue
# Trust policy is a placeholder — updated by the setup script with Snowflake's IAM user ARN
resource "aws_iam_role" "snowflake_access" {
  count = var.enable_snowflake_iceberg ? 1 : 0
  name  = "${local.name_prefix}-snowflake-access"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::${local.aws_account_id}:root" }
      Action    = "sts:AssumeRole"
    }]
  })

  lifecycle { ignore_changes = [assume_role_policy] }
}

resource "aws_iam_role_policy" "snowflake_s3_glue" {
  count = var.enable_snowflake_iceberg ? 1 : 0
  name  = "${local.name_prefix}-snowflake-s3-glue"
  role  = aws_iam_role.snowflake_access[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
          "s3:ListBucket", "s3:GetBucketLocation",
        ]
        Resource = [
          aws_s3_bucket.glue_data[0].arn,
          "${aws_s3_bucket.glue_data[0].arn}/*",
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase", "glue:GetDatabases",
          "glue:GetTable", "glue:GetTables",
          "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
          "glue:GetTableVersions",
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:${local.aws_account_id}:catalog",
          "arn:aws:glue:${var.aws_region}:${local.aws_account_id}:database/${local.snowflake_iceberg_glue_db}",
          "arn:aws:glue:${var.aws_region}:${local.aws_account_id}:table/${local.snowflake_iceberg_glue_db}/*",
        ]
      },
    ]
  })
}

# Lake Formation permissions for Snowflake Iceberg Glue database
resource "aws_lakeformation_permissions" "snowflake_iceberg_db" {
  count = var.enable_snowflake_iceberg ? 1 : 0

  principal   = "IAM_ALLOWED_PRINCIPALS"
  permissions = ["ALL"]

  database {
    name = aws_glue_catalog_database.snowflake_iceberg[0].name
  }
}

# Run Snowflake Iceberg setup script
resource "null_resource" "snowflake_iceberg_setup" {
  count = var.enable_snowflake_iceberg ? 1 : 0

  triggers = {
    glue_db  = local.snowflake_iceberg_glue_db
    role_arn = aws_iam_role.snowflake_access[0].arn
  }

  provisioner "local-exec" {
    command = <<-EOT
      echo "Waiting 15s for IAM role propagation..."
      sleep 15
      cd ${path.module}
      uv run --project ${abspath(path.module)}/.. python scripts/snowflake_iceberg_setup.py \
        --account-url "${var.snowflake_account_url}" \
        --user "${var.snowflake_user}" \
        --password "${var.snowflake_password}" \
        --warehouse "${var.snowflake_warehouse}" \
        --database "${local.snowflake_db_name}" \
        --schema "${local.snowflake_schema}" \
        --s3-bucket "${aws_s3_bucket.glue_data[0].id}" \
        --aws-region "${var.aws_region}" \
        --aws-account-id "${local.aws_account_id}" \
        --glue-database "${local.snowflake_iceberg_glue_db}" \
        --iam-role-arn "${aws_iam_role.snowflake_access[0].arn}" \
        --iam-role-name "${aws_iam_role.snowflake_access[0].name}"
    EOT
  }

  depends_on = [
    aws_glue_catalog_database.snowflake_iceberg,
    aws_iam_role_policy.snowflake_s3_glue,
    aws_lakeformation_permissions.snowflake_iceberg_db,
  ]
}
