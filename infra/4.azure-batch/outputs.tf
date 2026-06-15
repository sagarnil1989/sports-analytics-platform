output "batch_account_name" {
  value = azurerm_batch_account.main.name
}

output "batch_account_endpoint" {
  value = azurerm_batch_account.main.account_endpoint
}

output "pool_name" {
  value = azurerm_batch_pool.main.name
}

output "scripts_container_name" {
  value = azurerm_storage_container.scripts.name
}
