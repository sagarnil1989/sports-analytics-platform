# ---------------------------------------------------------------------------
# Azure Data Factory workspace
# ---------------------------------------------------------------------------

resource "azurerm_data_factory" "main" {
  name                = local.adf_name
  resource_group_name = data.azurerm_resource_group.main.name
  location            = data.azurerm_resource_group.main.location

  identity {
    type = "SystemAssigned"
  }

  tags = local.config.tags
}

# Grant ADF managed identity Storage Blob Data Contributor on the data lake
# so pipelines can read bronze and write silver-batch without a key.
resource "azurerm_role_assignment" "adf_data_lake_contributor" {
  scope                = data.azurerm_storage_account.data_lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.main.identity[0].principal_id
}

# ---------------------------------------------------------------------------
# Linked service — ADLS Gen2 (data lake)
# Using managed identity so no secrets are stored in ADF.
# ---------------------------------------------------------------------------

resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "data_lake" {
  name              = "ls_adls_datalake"
  data_factory_id   = azurerm_data_factory.main.id
  url               = "https://${data.azurerm_storage_account.data_lake.name}.dfs.core.windows.net"
  use_managed_identity = true
}
