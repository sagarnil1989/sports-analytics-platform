locals {
  config          = jsondecode(file("../../env/local.config.json"))
  batch_account   = "batch${local.config.project}"
  pool_name       = "cricket-pipeline"
  scripts_container = "batch-scripts"
  src_path        = "${path.module}/../../src/functions/cricket_ingestion"
  lib_path        = "${path.module}/../7.databricks/lib"
}
