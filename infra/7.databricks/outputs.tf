output "workspace_url" {
  value = local.workspace_url
}

output "vnet_id" {
  value = azurerm_virtual_network.databricks.id
}

output "databricks_notebook_bronze_to_silver" {
  value = databricks_notebook.bronze_to_silver.path
}

output "databricks_notebook_silver_to_gold" {
  value = databricks_notebook.silver_to_gold.path
}
