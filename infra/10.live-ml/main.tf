# ---------------------------------------------------------------------------
# Live ML Function App — isolated inference pipeline for live matches
#
# ISOLATION RULES (see docs/live-ml-predictions.md):
#   - READ-ONLY on silver container (enforced at Azure RBAC level)
#   - READ-WRITE on gold container (scoped by code to live_predictions.json only)
#   - No access to ingestion control files beyond read
#   - Completely separate from func-ramanuj-ingestion and func-ramanuj-display
#   - Shares the same consumption App Service Plan (no added cost)
# ---------------------------------------------------------------------------

data "azurerm_resource_group" "main" {
  name = local.config.resource_group_name
}

data "azurerm_storage_account" "data_lake" {
  name                = local.config.data_lake_storage_account_name
  resource_group_name = data.azurerm_resource_group.main.name
}

data "azurerm_service_plan" "main" {
  name                = local.config.function_app_plan_name
  resource_group_name = data.azurerm_resource_group.main.name
}

data "azurerm_application_insights" "main" {
  name                = local.config.application_insights_name
  resource_group_name = data.azurerm_resource_group.main.name
}

data "azurerm_key_vault" "main" {
  name                = local.config.key_vault_name
  resource_group_name = data.azurerm_resource_group.main.name
}

# Dedicated storage account for the Function App's own runtime state.
# Does NOT touch the data lake storage.
resource "azurerm_storage_account" "live_ml_storage" {
  name                     = local.config.live_ml_storage_account_name
  resource_group_name      = data.azurerm_resource_group.main.name
  location                 = data.azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  tags                     = local.config.tags
}

resource "azurerm_linux_function_app" "live_ml" {
  name                       = local.config.live_ml_function_app_name
  resource_group_name        = data.azurerm_resource_group.main.name
  location                   = data.azurerm_resource_group.main.location
  service_plan_id            = data.azurerm_service_plan.main.id
  storage_account_name       = azurerm_storage_account.live_ml_storage.name
  storage_account_access_key = azurerm_storage_account.live_ml_storage.primary_access_key

  identity {
    type = "SystemAssigned"
  }

  site_config {
    application_stack {
      python_version = "3.11"
    }
  }

  app_settings = {
    FUNCTIONS_WORKER_RUNTIME              = "python"
    APPINSIGHTS_INSTRUMENTATIONKEY        = data.azurerm_application_insights.main.instrumentation_key
    APPLICATIONINSIGHTS_CONNECTION_STRING = data.azurerm_application_insights.main.connection_string

    AzureWebJobsStorage = azurerm_storage_account.live_ml_storage.primary_connection_string

    DATA_LAKE_BLOB_ENDPOINT = data.azurerm_storage_account.data_lake.primary_blob_endpoint

    AZURE_FUNCTIONS_WORKER_PROCESS_TERMINATE_TIMEOUT = local.config.azure_functions_worker_process_terminate_timeout
    AzureFunctionsJobHost__functionTimeout           = local.config.azure_functions_job_host__function_timeout
  }

  tags = local.config.tags
}

# ── RBAC: silver — READ ONLY ──────────────────────────────────────────────────
# Enforces at Azure level that live-ml can never write to silver data.
# This is the critical isolation guarantee — a bug in live-ml cannot corrupt
# the silver layer that ingestion, bronze-to-silver, and ML training all depend on.
resource "azurerm_role_assignment" "live_ml_silver_reader" {
  scope                = "${data.azurerm_storage_account.data_lake.id}/blobServices/default/containers/silver"
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = azurerm_linux_function_app.live_ml.identity[0].principal_id
}

# ── RBAC: bronze — READ ONLY (needs active_inplay_fi control file) ────────────
resource "azurerm_role_assignment" "live_ml_bronze_reader" {
  scope                = "${data.azurerm_storage_account.data_lake.id}/blobServices/default/containers/bronze"
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = azurerm_linux_function_app.live_ml.identity[0].principal_id
}

# ── RBAC: gold — CONTRIBUTOR (writes live_predictions.json per event_id) ──────
# Note: Azure Storage RBAC is container-level; path-level scoping requires ABAC.
# The code enforces writing only to event_id=*/live_predictions.json.
# Gold models (live_models/) are also read from here.
resource "azurerm_role_assignment" "live_ml_gold_contributor" {
  scope                = "${data.azurerm_storage_account.data_lake.id}/blobServices/default/containers/gold"
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_linux_function_app.live_ml.identity[0].principal_id
}
