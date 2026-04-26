resource "azurerm_resource_group" "main" {
  name     = local.config.resource_group_name
  location = local.config.location
  tags     = local.config.tags
}

resource "azurerm_key_vault" "main" {
  name                       = local.config.key_vault_name
  resource_group_name        = azurerm_resource_group.main.name
  location                   = azurerm_resource_group.main.location
  tenant_id                  = local.config.tenant_id
  sku_name                   = "standard"
  soft_delete_retention_days = 7
  purge_protection_enabled   = false
  tags                       = local.config.tags
}
data "azurerm_client_config" "current" {}
resource "azurerm_key_vault_access_policy" "deployer_full" {
  key_vault_id = azurerm_key_vault.main.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = data.azurerm_client_config.current.object_id

  secret_permissions = [
    "Get",
    "List",
    "Set",
    "Delete",
    "Recover",
    "Backup",
    "Restore",
    "Purge"
  ]

  key_permissions = [
    "Get",
    "List",
    "Create",
    "Update",
    "Delete",
    "Recover",
    "Backup",
    "Restore",
    "Purge",
    "Encrypt",
    "Decrypt",
    "Sign",
    "Verify",
    "WrapKey",
    "UnwrapKey"
  ]

  certificate_permissions = [
    "Get",
    "List",
    "Create",
    "Update",
    "Delete",
    "Recover",
    "Backup",
    "Restore",
    "Purge",
    "Import"
  ]
}

resource "azurerm_log_analytics_workspace" "main" {
  name                = local.config.log_analytics_workspace_name
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = "PerGB2018"
  retention_in_days   = 30
  tags                = local.config.tags
}

resource "azurerm_application_insights" "main" {
  name                = local.config.application_insights_name
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  workspace_id        = azurerm_log_analytics_workspace.main.id
  application_type    = "web"
  tags                = local.config.tags
}

