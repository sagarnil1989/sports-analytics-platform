locals {
  config        = jsondecode(file("../../env/local.config.json"))
  workspace_url = "https://${azurerm_databricks_workspace.main.workspace_url}"
  adf_name      = "adf-${local.config.project}"
  src_path      = "${path.module}/../../src/functions/cricket_ingestion"
  dbfs_src_path = "/FileStore/cricket-pipeline/src"
}
