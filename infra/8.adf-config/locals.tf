locals {
  config     = jsondecode(file("../../env/local.config.json"))
  adf_name   = "adf-${local.config.project}"
  kv_uri     = "https://${local.config.key_vault_name}.vault.azure.net/"
  batch_pool = "cricket-pipeline"
}
