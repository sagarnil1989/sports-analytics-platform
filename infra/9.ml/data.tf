data "azurerm_resource_group" "main" {
  name = local.config.resource_group_name
}

data "azurerm_data_factory" "main" {
  name                = local.adf_name
  resource_group_name = data.azurerm_resource_group.main.name
}
