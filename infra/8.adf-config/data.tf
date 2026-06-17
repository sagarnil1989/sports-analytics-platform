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

data "azurerm_storage_account" "data_lake" {
  name                = local.config.data_lake_storage_account_name
  resource_group_name = data.azurerm_resource_group.main.name
}

data "azurerm_batch_account" "main" {
  name                = "batch${local.config.project}"
  resource_group_name = data.azurerm_resource_group.main.name
}

data "azurerm_user_assigned_identity" "batch_pool" {
  name                = "id-batch-${local.config.project}"
  resource_group_name = data.azurerm_resource_group.main.name
}
