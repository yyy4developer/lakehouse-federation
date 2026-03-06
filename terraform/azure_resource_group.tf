# =============================================================================
# Azure Resource Group (shared by Synapse, OneLake, Azure PostgreSQL)
# =============================================================================

resource "azurerm_resource_group" "demo" {
  count = (var.enable_synapse || var.enable_onelake || (var.enable_postgres && var.cloud == "azure")) ? 1 : 0

  name     = var.azure_resource_group_name
  location = var.azure_region

  tags = {
    Project   = "lakehouse-federation-demo"
    ManagedBy = "terraform"
    owner     = "yunyi.yao@databricks.com"
  }
}
