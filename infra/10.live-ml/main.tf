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

# ---------------------------------------------------------------------------
# Azure Communication Services — email + SMS notifications
# ---------------------------------------------------------------------------

resource "azurerm_communication_service" "main" {
  name                = "acs-${local.config.project}"
  resource_group_name = data.azurerm_resource_group.main.name
  data_location       = "Europe"

  tags = local.config.tags
}

resource "azurerm_email_communication_service" "main" {
  name                = "acs-email-${local.config.project}"
  resource_group_name = data.azurerm_resource_group.main.name
  data_location       = "Europe"

  tags = local.config.tags
}

resource "azurerm_email_communication_service_domain" "main" {
  name              = "AzureManagedDomain"
  email_service_id  = azurerm_email_communication_service.main.id
  domain_management = "AzureManaged"
}

# Link the email domain to the ACS service so it can send emails
resource "azurerm_communication_service_email_domain_association" "main" {
  communication_service_id = azurerm_communication_service.main.id
  email_service_domain_id  = azurerm_email_communication_service_domain.main.id
}

# Store the ACS connection string in Key Vault so it can be consumed as a
# Key Vault reference in the function app settings.
resource "azurerm_key_vault_secret" "acs_connection_string" {
  name         = "ACS-EMAIL-CONNECTION-STRING"
  value        = azurerm_communication_service.main.primary_connection_string
  key_vault_id = data.azurerm_key_vault.main.id
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

    # BetsAPI credentials (same secrets as ingestion + display functions)
    BETS_API_TOKEN    = "@Microsoft.KeyVault(VaultName=${data.azurerm_key_vault.main.name};SecretName=BET365-API-TOKEN)"
    BETS_API_BASE_URL = local.config.bet365_base_url
    SPORT_ID          = tostring(local.config.sport_id)

    # Azure Communication Services — email notifications
    ACS_EMAIL_CONNECTION_STRING = "@Microsoft.KeyVault(VaultName=${data.azurerm_key_vault.main.name};SecretName=ACS-EMAIL-CONNECTION-STRING)"
    ACS_EMAIL_SENDER            = "DoNotReply@${azurerm_email_communication_service_domain.main.mail_from_sender_domain}"

    # Notification recipients (override in portal for different addresses)
    NOTIFICATION_EMAIL = "dasgupta.sagarnil@gmail.com"
    NOTIFICATION_PHONE = "+31611012323"

    # Twilio SMS — secrets must be added to Key Vault manually:
    #   kv secret "TWILIO-ACCOUNT-SID"  — from console.twilio.com
    #   kv secret "TWILIO-AUTH-TOKEN"   — from console.twilio.com
    #   kv secret "TWILIO-FROM-NUMBER"  — provisioned Twilio phone number e.g. +1234567890
    TWILIO_ACCOUNT_SID = "@Microsoft.KeyVault(VaultName=${data.azurerm_key_vault.main.name};SecretName=TWILIO-ACCOUNT-SID)"
    TWILIO_AUTH_TOKEN  = "@Microsoft.KeyVault(VaultName=${data.azurerm_key_vault.main.name};SecretName=TWILIO-AUTH-TOKEN)"
    TWILIO_FROM_NUMBER = "@Microsoft.KeyVault(VaultName=${data.azurerm_key_vault.main.name};SecretName=TWILIO-FROM-NUMBER)"

    AZURE_FUNCTIONS_WORKER_PROCESS_TERMINATE_TIMEOUT = local.config.azure_functions_worker_process_terminate_timeout
    AzureFunctionsJobHost__functionTimeout           = local.config.azure_functions_job_host__function_timeout
}

  tags = local.config.tags
}

# ── RBAC: Key Vault — read secrets for BetsAPI token, ACS, Twilio ────────────
resource "azurerm_role_assignment" "live_ml_kv_reader" {
  scope                = data.azurerm_key_vault.main.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_linux_function_app.live_ml.identity[0].principal_id
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
