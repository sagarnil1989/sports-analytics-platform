data "azurerm_resource_group" "main" {
  name = local.config.resource_group_name
}

data "azurerm_resource_group" "cricwebsite" {
  name = local.config.resource_group_cricwebsite
}

data "azurerm_storage_account" "function_storage" {
  name                = local.config.function_storage_account_name
  resource_group_name = data.azurerm_resource_group.main.name
}

data "azurerm_storage_account" "data_lake" {
  name                = local.config.data_lake_storage_account_name
  resource_group_name = data.azurerm_resource_group.main.name
}

data "azurerm_key_vault" "main" {
  name                = local.config.key_vault_name
  resource_group_name = data.azurerm_resource_group.main.name
}

data "azurerm_key_vault" "cricwebsite" {
  name                = local.config.key_vault_cricwebsite
  resource_group_name = data.azurerm_resource_group.cricwebsite.name
}

data "azurerm_application_insights" "main" {
  name                = local.config.application_insights_name
  resource_group_name = data.azurerm_resource_group.main.name
}

resource "azurerm_service_plan" "main" {
  name                = local.config.function_app_plan_name
  resource_group_name = data.azurerm_resource_group.main.name
  location            = data.azurerm_resource_group.main.location
  os_type             = "Linux"
  sku_name            = "Y1"
  tags                = local.config.tags
}

resource "azurerm_linux_function_app" "main" {
  name                       = local.config.function_app_name
  resource_group_name        = data.azurerm_resource_group.main.name
  location                   = data.azurerm_resource_group.main.location
  service_plan_id            = azurerm_service_plan.main.id
  storage_account_name       = data.azurerm_storage_account.function_storage.name
  storage_account_access_key = data.azurerm_storage_account.function_storage.primary_access_key

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
  SCM_DO_BUILD_DURING_DEPLOYMENT        = "true"
  ENABLE_ORYX_BUILD                     = "true"
  APPINSIGHTS_INSTRUMENTATIONKEY        = data.azurerm_application_insights.main.instrumentation_key
  APPLICATIONINSIGHTS_CONNECTION_STRING = data.azurerm_application_insights.main.connection_string

  AzureWebJobsStorage = data.azurerm_storage_account.function_storage.primary_connection_string

  DATA_STORAGE_CONNECTION_STRING = data.azurerm_storage_account.data_lake.primary_connection_string

  BETS_API_TOKEN = data.azurerm_key_vault_secret.bet365_api_token.value
  BETS_API_BASE_URL  = local.config.bet365_base_url
  SPORT_ID         = local.config.sport_id

  DATA_LAKE_STORAGE_ACCOUNT = data.azurerm_storage_account.data_lake.name
  DATA_LAKE_BLOB_ENDPOINT   = data.azurerm_storage_account.data_lake.primary_blob_endpoint
  KEY_VAULT_URI             = data.azurerm_key_vault.main.vault_uri
  SQL_SERVER_NAME           = local.config.sql_server_name
  SQL_DATABASE_NAME         = local.config.sql_database_name
  MAX_LIVE_MATCHES = local.config.max_live_matches
  MAX_SILVER_SNAPSHOTS_PER_RUN = local.config.max_silver_snapshots_per_run
  MAX_GOLD_EVENTS_PER_RUN = local.config.max_gold_events_per_run
  AZURE_FUNCTIONS_WORKER_PROCESS_TERMINATE_TIMEOUT = local.config.azure_functions_worker_process_terminate_timeout
  AzureFunctionsJobHost__functionTimeout = local.config.azure_functions_job_host__function_timeout
  # CricWebsite PostgreSQL (oddsdb on psql-cricwebsite)
  CRICWEBSITE_DB_HOST     = local.config.cricwebsite_db_host
  CRICWEBSITE_DB_PORT     = "5432"
  CRICWEBSITE_DB_NAME     = "oddsdb"
  CRICWEBSITE_DB_USER     = local.config.cricwebsite_db_user
  CRICWEBSITE_DB_PASSWORD = data.azurerm_key_vault_secret.cricwebsite_db_password.value
}

  tags = local.config.tags
}

resource "azurerm_role_assignment" "function_data_lake_contributor" {
  scope                = data.azurerm_storage_account.data_lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_linux_function_app.main.identity[0].principal_id
}

resource "azurerm_role_assignment" "function_key_vault_secrets_user" {
  scope                = data.azurerm_key_vault.main.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_linux_function_app.main.identity[0].principal_id
}
