# =============================================================================
# Databricks Connections for Lakehouse Federation
# =============================================================================

# AWS Glue (Catalog Federation)
resource "databricks_connection" "glue" {
  count           = var.enable_glue ? 1 : 0
  name            = "${local.name_prefix}-glue-conn"
  connection_type = "GLUE"

  options = {
    aws_region     = var.aws_region
    aws_account_id = local.aws_account_id
    credential     = databricks_credential.glue_service[0].name
  }

  comment = "Connection to AWS Glue catalog"
}

# Amazon Redshift (Query Federation)
resource "databricks_connection" "redshift" {
  count           = var.enable_redshift ? 1 : 0
  name            = "${local.name_prefix}-redshift-conn"
  connection_type = "REDSHIFT"

  options = {
    host     = aws_redshiftserverless_workgroup.demo[0].endpoint[0].address
    port     = "5439"
    user     = "admin"
    password = var.redshift_admin_password
  }

  comment = "Connection to Redshift Serverless"
}

# PostgreSQL (Query Federation)
resource "databricks_connection" "postgres" {
  count           = var.enable_postgres ? 1 : 0
  name            = "${local.name_prefix}-postgres-conn"
  connection_type = "POSTGRESQL"

  options = {
    host = var.cloud == "aws" ? (
      aws_db_instance.postgres[0].address
    ) : (
      azurerm_postgresql_flexible_server.postgres[0].fqdn
    )
    port     = "5432"
    user     = "pgadmin"
    password = var.postgres_admin_password
  }

  comment = "Connection to PostgreSQL"
}

# Azure Synapse (Query Federation)
resource "databricks_connection" "synapse" {
  count           = var.enable_synapse ? 1 : 0
  name            = "${local.name_prefix}-synapse-conn"
  connection_type = "SQLDW"

  options = {
    host                    = "${azurerm_synapse_workspace.demo[0].name}-ondemand.sql.azuresynapse.net"
    port                    = "1433"
    user                    = "sqladmin"
    password                = var.synapse_admin_password
    trustServerCertificate  = "true"
  }

  comment = "Connection to Azure Synapse Analytics"
}

# Google BigQuery (Query Federation)
resource "databricks_connection" "bigquery" {
  count           = var.enable_bigquery && var.gcp_credentials_json != "" ? 1 : 0
  name            = "${local.name_prefix}-bigquery-conn"
  connection_type = "BIGQUERY"

  options = {
    GoogleServiceAccountKeyJson = var.gcp_credentials_json
    projectId                   = var.gcp_project_id
  }

  comment = "Connection to Google BigQuery"
}

# Snowflake (Query Federation)
resource "databricks_connection" "snowflake" {
  count           = var.enable_snowflake ? 1 : 0
  name            = "${local.name_prefix}-snowflake-conn"
  connection_type = "SNOWFLAKE"

  options = {
    host        = replace(replace(var.snowflake_account_url, "https://", ""), "http://", "")
    user        = var.snowflake_user
    password    = var.snowflake_password
    sfWarehouse = var.snowflake_warehouse
  }

  comment = "Connection to Snowflake"
}

# OneLake / Microsoft Fabric (Catalog Federation)
resource "databricks_connection" "onelake" {
  count           = var.enable_onelake ? 1 : 0
  name            = "${local.name_prefix}-onelake-conn"
  connection_type = "ONELAKE"

  options = {
    credential = databricks_storage_credential.catalog[0].name
    workspace  = var.fabric_workspace_id
  }

  comment = "Connection to Microsoft OneLake (Fabric)"

  depends_on = [databricks_storage_credential.catalog]
}
