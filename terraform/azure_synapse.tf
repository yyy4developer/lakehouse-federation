# =============================================================================
# Azure Synapse Analytics (Serverless SQL pool)
# =============================================================================

resource "azurerm_storage_account" "synapse" {
  count = var.enable_synapse ? 1 : 0

  name                     = replace("${var.project_prefix}synapse", "-", "")
  resource_group_name      = azurerm_resource_group.demo[0].name
  location                 = azurerm_resource_group.demo[0].location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true # ADLS Gen2

  tags = {
    Project = "lakehouse-federation-demo"
    owner   = "yunyi.yao@databricks.com"
  }
}

resource "azurerm_storage_data_lake_gen2_filesystem" "synapse" {
  count = var.enable_synapse ? 1 : 0

  name               = "synapse"
  storage_account_id = azurerm_storage_account.synapse[0].id
}

resource "azurerm_synapse_workspace" "demo" {
  count = var.enable_synapse ? 1 : 0

  name                                 = "${var.project_prefix}-synapse"
  resource_group_name                  = azurerm_resource_group.demo[0].name
  location                             = azurerm_resource_group.demo[0].location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.synapse[0].id
  sql_administrator_login              = "sqladmin"
  sql_administrator_login_password     = var.synapse_admin_password

  identity {
    type = "SystemAssigned"
  }

  tags = {
    Project = "lakehouse-federation-demo"
    owner   = "yunyi.yao@databricks.com"
  }
}

resource "azurerm_synapse_firewall_rule" "allow_all" {
  count = var.enable_synapse ? 1 : 0

  name                 = "AllowAll"
  synapse_workspace_id = azurerm_synapse_workspace.demo[0].id
  start_ip_address     = "0.0.0.0"
  end_ip_address       = "255.255.255.255"
}

# Initialize Synapse with tables and data via sqlcmd
resource "null_resource" "synapse_init" {
  count = var.enable_synapse ? 1 : 0

  triggers = {
    workspace_id = azurerm_synapse_workspace.demo[0].id
  }

  provisioner "local-exec" {
    command = <<-EOT
      SYNAPSE_HOST="${azurerm_synapse_workspace.demo[0].name}.sql.azuresynapse.net"

      echo "Creating database..."
      sqlcmd -S "$SYNAPSE_HOST" -U sqladmin -P '${var.synapse_admin_password}' \
        -Q "IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'factory_analytics') CREATE DATABASE factory_analytics"

      echo "Creating tables and inserting data..."
      sqlcmd -S "$SYNAPSE_HOST" -d factory_analytics -U sqladmin -P '${var.synapse_admin_password}' \
        -i ${path.module}/sql/synapse/create_shift_schedules.sql
      sqlcmd -S "$SYNAPSE_HOST" -d factory_analytics -U sqladmin -P '${var.synapse_admin_password}' \
        -i ${path.module}/sql/synapse/insert_shift_schedules.sql
      sqlcmd -S "$SYNAPSE_HOST" -d factory_analytics -U sqladmin -P '${var.synapse_admin_password}' \
        -i ${path.module}/sql/synapse/create_energy_consumption.sql
      sqlcmd -S "$SYNAPSE_HOST" -d factory_analytics -U sqladmin -P '${var.synapse_admin_password}' \
        -i ${path.module}/sql/synapse/insert_energy_consumption.sql

      echo "Synapse initialization complete."
    EOT
  }

  depends_on = [azurerm_synapse_firewall_rule.allow_all]
}
