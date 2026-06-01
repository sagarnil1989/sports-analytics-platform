# ---------------------------------------------------------------------------
# VNet injection for Databricks — eliminates the Azure-managed NAT gateway
#
# Without VNet injection, Azure creates a managed VNet + NAT gateway (~€30/mo).
# With VNet injection, we own the VNet. no_public_ip=false means cluster nodes
# keep public IPs for outbound traffic, so no NAT gateway is needed at all.
#
# Subnets must be delegated to Microsoft.Databricks/workspaces.
# NSGs must exist and be associated — Databricks auto-adds its own rules.
# ---------------------------------------------------------------------------

resource "azurerm_virtual_network" "databricks" {
  name                = "vnet-databricks-${local.config.project}"
  resource_group_name = data.azurerm_resource_group.main.name
  location            = data.azurerm_resource_group.main.location
  address_space       = ["10.139.0.0/16"]
  tags                = local.config.tags
}

# ---------------------------------------------------------------------------
# Public subnet — Spark driver + workers (outbound via node public IPs)
# ---------------------------------------------------------------------------

resource "azurerm_subnet" "databricks_public" {
  name                 = "snet-databricks-public"
  resource_group_name  = data.azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.databricks.name
  address_prefixes     = ["10.139.0.0/24"]

  delegation {
    name = "databricks-del"
    service_delegation {
      name = "Microsoft.Databricks/workspaces"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action",
      ]
    }
  }
}

# ---------------------------------------------------------------------------
# Private subnet — internal cluster communication only
# ---------------------------------------------------------------------------

resource "azurerm_subnet" "databricks_private" {
  name                 = "snet-databricks-private"
  resource_group_name  = data.azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.databricks.name
  address_prefixes     = ["10.139.1.0/24"]

  delegation {
    name = "databricks-del"
    service_delegation {
      name = "Microsoft.Databricks/workspaces"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action",
      ]
    }
  }
}

# ---------------------------------------------------------------------------
# NSGs — public and private subnets
#
# Created empty — Databricks auto-injects all required rules when the workspace
# provisions (rules prefixed "Microsoft.Databricks-workspaces_UseOnly_*").
# lifecycle.ignore_changes prevents Terraform from fighting with Databricks
# over those auto-injected rules on every subsequent plan.
# ---------------------------------------------------------------------------

resource "azurerm_network_security_group" "databricks_public" {
  name                = "nsg-databricks-public-${local.config.project}"
  resource_group_name = data.azurerm_resource_group.main.name
  location            = data.azurerm_resource_group.main.location
  tags                = local.config.tags

  lifecycle {
    ignore_changes = [security_rule]
  }
}

resource "azurerm_network_security_group" "databricks_private" {
  name                = "nsg-databricks-private-${local.config.project}"
  resource_group_name = data.azurerm_resource_group.main.name
  location            = data.azurerm_resource_group.main.location
  tags                = local.config.tags

  lifecycle {
    ignore_changes = [security_rule]
  }
}

# ---------------------------------------------------------------------------
# NSG associations — must exist before workspace is created
# ---------------------------------------------------------------------------

resource "azurerm_subnet_network_security_group_association" "databricks_public" {
  subnet_id                 = azurerm_subnet.databricks_public.id
  network_security_group_id = azurerm_network_security_group.databricks_public.id
}

resource "azurerm_subnet_network_security_group_association" "databricks_private" {
  subnet_id                 = azurerm_subnet.databricks_private.id
  network_security_group_id = azurerm_network_security_group.databricks_private.id
}
