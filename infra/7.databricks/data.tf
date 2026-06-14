data "azurerm_resource_group" "main" {
  name = local.config.resource_group_name
}

data "azurerm_storage_account" "data_lake" {
  name                = local.config.data_lake_storage_account_name
  resource_group_name = data.azurerm_resource_group.main.name
}

data "azurerm_key_vault" "main" {
  name                = local.config.key_vault_name
  resource_group_name = data.azurerm_resource_group.main.name
}

data "azurerm_key_vault_secret" "bet365_api_token" {
  name         = "BET365-API-TOKEN"
  key_vault_id = data.azurerm_key_vault.main.id
}

