data "azurerm_resource_group" "main" {
  name = local.config.resource_group_name
}

data "azurerm_storage_account" "data_lake" {
  name                = local.config.data_lake_storage_account_name
  resource_group_name = data.azurerm_resource_group.main.name
}

data "azurerm_linux_function_app" "function_app" {
  name                = local.config.ingestion_function_app_name
  resource_group_name = data.azurerm_resource_group.main.name
}
