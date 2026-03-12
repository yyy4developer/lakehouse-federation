# =============================================================================
# Snowflake Iceberg Catalog Federation
# Snowflake-managed Iceberg tables -> S3 (data) -> Databricks reads via
# CONNECTION_SNOWFLAKE (metadata) + S3 external location (data)
# Requires: enable_glue = true (reuses S3 bucket + external location)
# =============================================================================

# IAM Role for Snowflake to access S3
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

resource "aws_iam_role_policy" "snowflake_s3" {
  count = var.enable_snowflake_iceberg ? 1 : 0
  name  = "${local.name_prefix}-snowflake-s3"
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
          "${aws_s3_bucket.glue_data[0].arn}/snowflake_iceberg/*",
        ]
      },
    ]
  })
}

# Run Snowflake Iceberg setup script
resource "null_resource" "snowflake_iceberg_setup" {
  count = var.enable_snowflake_iceberg ? 1 : 0

  triggers = {
    role_arn = aws_iam_role.snowflake_access[0].arn
    bucket   = aws_s3_bucket.glue_data[0].id
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
        --database "${local.snowflake_iceberg_db_name}" \
        --schema "${local.snowflake_schema}" \
        --s3-bucket "${aws_s3_bucket.glue_data[0].id}" \
        --aws-region "${var.aws_region}" \
        --iam-role-arn "${aws_iam_role.snowflake_access[0].arn}" \
        --iam-role-name "${aws_iam_role.snowflake_access[0].name}"
    EOT
  }

  depends_on = [
    aws_iam_role_policy.snowflake_s3,
  ]
}
