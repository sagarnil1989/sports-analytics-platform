output "marquez_api_url" {
  value       = local.marquez_api_url
  description = "OpenLineage endpoint — add this to env/local.config.json as marquez_api_url, then re-apply infra/6.adf."
}

output "marquez_ui_url" {
  value       = local.marquez_ui_url
  description = "Marquez Web UI — open in browser to view job/dataset lineage."
}

output "marquez_db_name" {
  value = azurerm_postgresql_flexible_server_database.marquez.name
}
