output "display_function_app_name" {
  value = azurerm_linux_function_app.display.name
}

output "display_function_app_hostname" {
  value = azurerm_linux_function_app.display.default_hostname
}

output "display_storage_account_name" {
  value = azurerm_storage_account.display_storage.name
}
