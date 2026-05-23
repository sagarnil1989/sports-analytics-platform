data "azurerm_resource_group" "main" {
  name = local.config.resource_group_name
}

data "azurerm_data_factory" "main" {
  name                = local.adf_name
  resource_group_name = data.azurerm_resource_group.main.name
}

data "azurerm_key_vault" "main" {
  name                = local.config.key_vault_name
  resource_group_name = data.azurerm_resource_group.main.name
}

data "azurerm_databricks_workspace" "main" {
  name                = "dbw-${local.config.project}"
  resource_group_name = data.azurerm_resource_group.main.name
}
