data "azurerm_key_vault_secret" "bet365_api_token" {
  name         = "BET365-API-TOKEN"
  key_vault_id = data.azurerm_key_vault.main.id
}

data "azurerm_key_vault_secret" "cricwebsite_db_password" {
  name         = "POSTGRES-ADMIN-PASSWORD"
  key_vault_id = data.azurerm_key_vault.cricwebsite.id
}