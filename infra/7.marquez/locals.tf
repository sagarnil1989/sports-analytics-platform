locals {
  config = jsondecode(file("../../env/local.config.json"))

  marquez_name     = "marquez-${local.config.project}"
  marquez_dns_fqdn = "${local.marquez_name}.${local.config.location}.azurecontainer.io"
  marquez_api_url  = "http://${local.marquez_dns_fqdn}:5000"
  marquez_ui_url   = "http://${local.marquez_dns_fqdn}:3000"
}
