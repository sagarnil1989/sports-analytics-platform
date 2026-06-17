data "azurerm_resource_group" "main" {
  name = local.config.resource_group_name
}

data "azurerm_data_factory" "main" {
  name                = local.adf_name
  resource_group_name = data.azurerm_resource_group.main.name
}

data "azurerm_user_assigned_identity" "batch_pool" {
  name                = "id-batch-${local.config.project}"
  resource_group_name = data.azurerm_resource_group.main.name
}
