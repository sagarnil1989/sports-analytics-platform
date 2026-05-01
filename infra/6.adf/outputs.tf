output "adf_name" {
  value = azurerm_data_factory.main.name
}

output "adf_id" {
  value = azurerm_data_factory.main.id
}

output "adf_managed_identity_principal_id" {
  value       = azurerm_data_factory.main.identity[0].principal_id
  description = "Object ID of the ADF system-assigned managed identity — needed to grant Key Vault access manually."
}
