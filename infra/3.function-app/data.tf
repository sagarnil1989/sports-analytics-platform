data "azurerm_key_vault_secret" "bet365_api_token" {
  name         = "BET365-API-TOKEN"
  key_vault_id = data.azurerm_key_vault.main.id
}