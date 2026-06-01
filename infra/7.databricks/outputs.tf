output "workspace_url" {
  value = local.workspace_url
}

output "vnet_id" {
  value = azurerm_virtual_network.databricks.id
}

output "databricks_notebook_silver_build_ended_match" {
  value = databricks_notebook.silver_build_ended_match.path
}

output "databricks_notebook_silver_backfill" {
  value = databricks_notebook.silver_backfill.path
}

output "databricks_notebook_gold_build_ended_match" {
  value = databricks_notebook.gold_build_ended_match.path
}

output "databricks_notebook_gold_backfill" {
  value = databricks_notebook.gold_backfill.path
}
