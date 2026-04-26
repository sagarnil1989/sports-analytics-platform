data "azurerm_resource_group" "main" {
  name = local.config.resource_group_name
}

resource "azurerm_static_web_app" "main" {
  name                = local.config.static_web_app_name
  resource_group_name = data.azurerm_resource_group.main.name
  location            = data.azurerm_resource_group.main.location
  sku_tier            = "Free"
  sku_size            = "Free"
  tags                = local.config.tags
}
