# =============================================================================
# Azure Database for PostgreSQL Flexible Server
# =============================================================================

resource "azurerm_postgresql_flexible_server" "postgres" {
  count = (var.enable_postgres && var.cloud == "azure") ? 1 : 0

  name                = "${var.project_prefix}-postgres"
  resource_group_name = azurerm_resource_group.demo[0].name
  location            = azurerm_resource_group.demo[0].location

  version                      = "16"
  sku_name                     = "B_Standard_B1ms"
  storage_mb                   = 32768
  administrator_login          = "pgadmin"
  administrator_password       = var.postgres_admin_password
  zone                         = "1"
  public_network_access_enabled = true

  tags = {
    Project = "lakehouse-federation-demo"
    owner   = "yunyi.yao@databricks.com"
  }
}

resource "azurerm_postgresql_flexible_server_database" "factory" {
  count = (var.enable_postgres && var.cloud == "azure") ? 1 : 0

  name      = local.postgres_db_name
  server_id = azurerm_postgresql_flexible_server.postgres[0].id
  charset   = "UTF8"
  collation = "en_US.utf8"
}

resource "azurerm_postgresql_flexible_server_firewall_rule" "allow_all" {
  count = (var.enable_postgres && var.cloud == "azure") ? 1 : 0

  name             = "allow-all-demo"
  server_id        = azurerm_postgresql_flexible_server.postgres[0].id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "255.255.255.255"
}

resource "null_resource" "azure_postgres_init" {
  count = (var.enable_postgres && var.cloud == "azure") ? 1 : 0

  triggers = {
    server_id = azurerm_postgresql_flexible_server.postgres[0].id
  }

  provisioner "local-exec" {
    command = <<-EOT
      export PGPASSWORD='${var.postgres_admin_password}'
      PGHOST='${azurerm_postgresql_flexible_server.postgres[0].fqdn}'

      echo "Creating tables..."
      psql -h "$PGHOST" -U pgadmin -d ${local.postgres_db_name} -f ${path.module}/sql/postgres/create_maintenance_logs.sql
      psql -h "$PGHOST" -U pgadmin -d ${local.postgres_db_name} -f ${path.module}/sql/postgres/create_work_orders.sql

      echo "Inserting data..."
      psql -h "$PGHOST" -U pgadmin -d ${local.postgres_db_name} -f ${path.module}/sql/postgres/insert_maintenance_logs.sql
      psql -h "$PGHOST" -U pgadmin -d ${local.postgres_db_name} -f ${path.module}/sql/postgres/insert_work_orders.sql

      echo "Adding comments..."
      psql -h "$PGHOST" -U pgadmin -d ${local.postgres_db_name} -f ${path.module}/sql/postgres/comments.sql

      echo "Azure PostgreSQL initialization complete."
    EOT
  }

  depends_on = [
    azurerm_postgresql_flexible_server_database.factory,
    azurerm_postgresql_flexible_server_firewall_rule.allow_all,
  ]
}
