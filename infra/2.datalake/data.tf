data "azurerm_resource_group" "rg" {
    name = local.config.resource_group_name
}