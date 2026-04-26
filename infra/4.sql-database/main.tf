data "azurerm_resource_group" "main" {
  name = local.config.resource_group_name
}

resource "azurerm_mssql_server" "main" {
  name                         = local.config.sql_server_name
  resource_group_name          = data.azurerm_resource_group.main.name
  location                     = data.azurerm_resource_group.main.location
  version                      = "12.0"
  administrator_login          = local.config.sql_admin_login
  administrator_login_password = local.config.sql_admin_password
  minimum_tls_version          = "1.2"
  tags                         = local.config.tags
}

resource "azurerm_mssql_database" "main" {
  name      = local.config.sql_database_name
  server_id = azurerm_mssql_server.main.id
  sku_name  = "Basic"
  tags      = local.config.tags
}

resource "azurerm_mssql_firewall_rule" "allow_azure_services" {
  name             = "AllowAzureServices"
  server_id        = azurerm_mssql_server.main.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}
