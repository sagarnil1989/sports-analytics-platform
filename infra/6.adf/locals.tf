locals {
  config   = jsondecode(file("../../env/local.config.json"))
  adf_name = "adf-${local.config.project}"
}
