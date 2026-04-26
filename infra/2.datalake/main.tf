
resource "azurerm_storage_account" "data_lake" {
  name                     = local.config.data_lake_storage_account_name
  resource_group_name      = data.azurerm_resource_group.rg.name
  location                 = local.config.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true
  min_tls_version          = "TLS1_2"

  blob_properties {
    delete_retention_policy {
      days = 7
    }
    container_delete_retention_policy {
      days = 7
    }
  }

  tags = local.config.tags
}

resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_id    = azurerm_storage_account.data_lake.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver" {
  name                  = "silver"
  storage_account_id    = azurerm_storage_account.data_lake.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  name                  = "gold"
  storage_account_id    = azurerm_storage_account.data_lake.id
  container_access_type = "private"
}

resource "azurerm_storage_account" "function_storage" {
  name                     = local.config.function_storage_account_name
  resource_group_name      = data.azurerm_resource_group.rg.name
  location                 = local.config.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  min_tls_version          = "TLS1_2"
  tags                     = local.config.tags
}

resource "azurerm_storage_queue" "parser_queue" {
  name                 = "parser-queue"
  storage_account_id   = azurerm_storage_account.function_storage.id
}
