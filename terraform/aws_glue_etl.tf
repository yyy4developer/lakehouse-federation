# =============================================================================
# Glue ETL Job for Data Generation
# Generates factory data in Parquet, Delta, and Iceberg formats on S3
# =============================================================================

# -----------------------------------------------------------------------------
# IAM Role for Glue ETL Job
# -----------------------------------------------------------------------------

resource "aws_iam_role" "glue_etl" {
  name = "${var.project_prefix}-glue-etl"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "glue_etl" {
  name = "${var.project_prefix}-glue-etl-policy"
  role = aws_iam_role.glue_etl.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3Access"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = [
          aws_s3_bucket.glue_data.arn,
          "${aws_s3_bucket.glue_data.arn}/*",
        ]
      },
      {
        Sid    = "GlueCatalogAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:CreateDatabase",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
          "glue:DeletePartition",
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:${local.aws_account_id}:catalog",
          "arn:aws:glue:${var.aws_region}:${local.aws_account_id}:database/*",
          "arn:aws:glue:${var.aws_region}:${local.aws_account_id}:table/*/*",
        ]
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "arn:aws:logs:${var.aws_region}:${local.aws_account_id}:*"
      },
      {
        Sid    = "LakeFormationAccess"
        Effect = "Allow"
        Action = [
          "lakeformation:GetDataAccess",
        ]
        Resource = "*"
      }
    ]
  })
}

# Attach AWS managed Glue service role policy
resource "aws_iam_role_policy_attachment" "glue_etl_service" {
  role       = aws_iam_role.glue_etl.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# -----------------------------------------------------------------------------
# Upload PySpark script to S3
# -----------------------------------------------------------------------------

resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.glue_data.id
  key    = "scripts/generate_data.py"
  source = "${path.module}/scripts/generate_data.py"
  etag   = filemd5("${path.module}/scripts/generate_data.py")
}

# -----------------------------------------------------------------------------
# Glue ETL Job Definition
# -----------------------------------------------------------------------------

resource "aws_glue_job" "data_generator" {
  name     = "${var.project_prefix}-data-generator"
  role_arn = aws_iam_role.glue_etl.arn

  command {
    script_location = "s3://${aws_s3_bucket.glue_data.id}/scripts/generate_data.py"
    python_version  = "3"
  }

  default_arguments = {
    "--datalake-formats"                 = "delta,iceberg"
    "--enable-glue-datacatalog"          = ""
    "--S3_BUCKET"                        = aws_s3_bucket.glue_data.id
    "--GLUE_DATABASE"                    = aws_glue_catalog_database.factory_master.name
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://${aws_s3_bucket.glue_data.id}/tmp/"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 10 # minutes

  depends_on = [
    aws_s3_object.glue_script,
    aws_glue_catalog_database.factory_master,
    aws_lakeformation_permissions.iam_database,
  ]
}

# -----------------------------------------------------------------------------
# Trigger Glue Job execution and wait for completion
# -----------------------------------------------------------------------------

resource "null_resource" "run_glue_job" {
  triggers = {
    script_hash = filemd5("${path.module}/scripts/generate_data.py")
    job_name    = aws_glue_job.data_generator.name
  }

  provisioner "local-exec" {
    command = <<-EOT
      echo "Starting Glue job: ${aws_glue_job.data_generator.name}"
      RUN_ID=$(aws glue start-job-run --job-name "${aws_glue_job.data_generator.name}" --region "${var.aws_region}" --query 'JobRunId' --output text)
      echo "Job run ID: $RUN_ID"

      # Poll for completion
      while true; do
        STATUS=$(aws glue get-job-run --job-name "${aws_glue_job.data_generator.name}" --run-id "$RUN_ID" --region "${var.aws_region}" --query 'JobRun.JobRunState' --output text)
        echo "Status: $STATUS"
        if [ "$STATUS" = "SUCCEEDED" ]; then
          echo "Glue job completed successfully."
          break
        elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "STOPPED" ] || [ "$STATUS" = "ERROR" ] || [ "$STATUS" = "TIMEOUT" ]; then
          echo "Glue job failed with status: $STATUS"
          aws glue get-job-run --job-name "${aws_glue_job.data_generator.name}" --run-id "$RUN_ID" --region "${var.aws_region}" --query 'JobRun.ErrorMessage' --output text
          exit 1
        fi
        sleep 15
      done
    EOT
  }

  depends_on = [aws_glue_job.data_generator, aws_s3_object.glue_script]
}
