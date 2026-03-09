# =============================================================================
# Azure storage for Databricks managed catalog (union catalog)
# Skipped when use_workspace_default_storage = true (workspace in different tenant)
# =============================================================================

locals {
  need_catalog_storage = var.cloud == "azure" && !var.use_workspace_default_storage
}

resource "azurerm_storage_account" "catalog" {
  count = local.need_catalog_storage ? 1 : 0

  name                     = "${local.name_prefix_compact}cat"
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

resource "azurerm_storage_container" "catalog" {
  count = local.need_catalog_storage ? 1 : 0

  name                  = "unity-catalog"
  storage_account_id    = azurerm_storage_account.catalog[0].id
  container_access_type = "private"
}

resource "azurerm_databricks_access_connector" "catalog" {
  count = local.need_catalog_storage ? 1 : 0

  name                = "${local.name_prefix}-access-connector"
  resource_group_name = azurerm_resource_group.demo[0].name
  location            = azurerm_resource_group.demo[0].location

  identity {
    type = "SystemAssigned"
  }

  tags = {
    Project = "lakehouse-federation-demo"
    owner   = "yunyi.yao@databricks.com"
  }
}

resource "azurerm_role_assignment" "catalog_storage" {
  count = local.need_catalog_storage ? 1 : 0

  scope                = azurerm_storage_account.catalog[0].id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.catalog[0].identity[0].principal_id
}

# Grant current user Contributor on access connector (required by Databricks to register storage credential)
resource "azurerm_role_assignment" "access_connector_contributor" {
  count = local.need_catalog_storage ? 1 : 0

  scope                = azurerm_databricks_access_connector.catalog[0].id
  role_definition_name = "Contributor"
  principal_id         = data.azurerm_client_config.current[0].object_id
}

data "azurerm_client_config" "current" {
  count = var.cloud == "azure" ? 1 : 0
}

# Wait for Azure RBAC propagation (role assignments can take up to 60s)
resource "time_sleep" "role_propagation" {
  count = local.need_catalog_storage ? 1 : 0

  depends_on      = [azurerm_role_assignment.catalog_storage, azurerm_role_assignment.access_connector_contributor]
  create_duration = "90s"
}

resource "databricks_storage_credential" "catalog" {
  count = local.need_catalog_storage ? 1 : 0
  name  = "${local.name_prefix}-catalog-storage-cred"

  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.catalog[0].id
  }

  comment = "Storage credential for managed catalog storage"

  depends_on = [time_sleep.role_propagation]
}

resource "databricks_external_location" "catalog" {
  count = local.need_catalog_storage ? 1 : 0
  name  = "${local.name_prefix}-catalog-location"
  url   = "abfss://${azurerm_storage_container.catalog[0].name}@${azurerm_storage_account.catalog[0].name}.dfs.core.windows.net/"

  credential_name = databricks_storage_credential.catalog[0].name
  comment         = "External location for managed catalog storage"

  depends_on = [databricks_storage_credential.catalog]
}
