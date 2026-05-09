data "azurerm_resource_group" "main" {
  name = local.config.resource_group_name
}

data "azurerm_storage_account" "data_lake" {
  name                = local.config.data_lake_storage_account_name
  resource_group_name = data.azurerm_resource_group.main.name
}

data "azurerm_linux_function_app" "function_app" {
  name                = local.config.function_app_name
  resource_group_name = data.azurerm_resource_group.main.name
}

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

# ---------------------------------------------------------------------------
# Linked service — Azure Function App
# Used by the batch pipeline to trigger HTTP-triggered silver functions.
# ---------------------------------------------------------------------------

resource "azurerm_data_factory_linked_service_azure_function" "function_app" {
  name            = "ls_function_app"
  data_factory_id = azurerm_data_factory.main.id
  url             = "https://${data.azurerm_linux_function_app.function_app.default_hostname}"

  # Function-level auth key fetched via Key Vault reference keeps the secret out of TF state.
  # Set the key value in the ADF UI or via az datafactory linked-service update after deploy.
  key = "dummy-replace-via-portal"
}

# ---------------------------------------------------------------------------
# Pipeline — batch silver processing for a single finished match
#
# Accepts parameter: event_id (string)
# Activity sequence:
#   1. AzureFunctionActivity → POST /api/mgmt/reprocess-silver?event_id={event_id}
#      (HTTP function to be built; calls silver pipeline for one event with
#       the corrected pg_over-only logic and writes to silver-batch container)
#
# This pipeline is designed to be:
#   - Triggered manually per event_id to fix historical bad silver data
#   - Called from a ForEach loop in a nightly batch pipeline over ended matches
# ---------------------------------------------------------------------------

resource "azurerm_data_factory_pipeline" "batch_silver_reprocess" {
  name            = "batch_silver_reprocess"
  data_factory_id = azurerm_data_factory.main.id
  description     = "Reprocess bronze → silver-batch for one finished match using corrected parsing logic."

  parameters = {
    event_id = ""
  }

  activities_json = jsonencode([
    # ── Lineage: START ──────────────────────────────────────────────────────
    {
      name = "LineageStart"
      type = "WebActivity"
      typeProperties = {
        url    = local.marquez_lineage_url != "" ? local.marquez_lineage_url : "http://localhost"
        method = "POST"
        headers = { "Content-Type" = "application/json" }
        body = {
          value = local.ol_start_reprocess
          type  = "Expression"
        }
      }
    },
    # ── Main work ───────────────────────────────────────────────────────────
    {
      name      = "ReprocessSilver"
      type      = "AzureFunctionActivity"
      dependsOn = [{ activity = "LineageStart", dependencyConditions = ["Succeeded"] }]
      linkedServiceName = {
        referenceName = "ls_function_app"
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        functionName = "mgmt/reprocess-silver"
        method       = "POST"
        body = {
          value = "@json(concat('{\"event_id\":\"', pipeline().parameters.event_id, '\"}'))"
          type  = "Expression"
        }
      }
    },
    # ── Lineage: COMPLETE ───────────────────────────────────────────────────
    {
      name      = "LineageComplete"
      type      = "WebActivity"
      dependsOn = [{ activity = "ReprocessSilver", dependencyConditions = ["Succeeded"] }]
      typeProperties = {
        url    = local.marquez_lineage_url != "" ? local.marquez_lineage_url : "http://localhost"
        method = "POST"
        headers = { "Content-Type" = "application/json" }
        body = {
          value = local.ol_complete_reprocess
          type  = "Expression"
        }
      }
    }
  ])
}

# ---------------------------------------------------------------------------
# Pipeline — nightly batch: discover finished matches and reprocess each
#
# 1. LookupActivity        → reads gold/cricket/innings_tracker/index.json
#                            to get list of event_ids where has_outcome=false
#                            (matches ended but silver not yet batch-processed)
# 2. ForEachActivity       → iterates over the list
#   └─ ExecutePipeline     → calls batch_silver_reprocess with event_id param
# ---------------------------------------------------------------------------

resource "azurerm_data_factory_pipeline" "nightly_batch_silver" {
  name            = "nightly_batch_silver"
  data_factory_id = azurerm_data_factory.main.id
  description     = "Nightly: find finished matches without clean silver and reprocess each."

  depends_on = [
    azurerm_data_factory_dataset_json.innings_tracker_index,
    azurerm_data_factory_pipeline.batch_silver_reprocess,
  ]

  activities_json = jsonencode([
    # ── Lineage: START ──────────────────────────────────────────────────────
    {
      name = "LineageStart"
      type = "WebActivity"
      typeProperties = {
        url    = local.marquez_lineage_url != "" ? local.marquez_lineage_url : "http://localhost"
        method = "POST"
        headers = { "Content-Type" = "application/json" }
        body = {
          value = local.ol_start_nightly
          type  = "Expression"
        }
      }
    },
    # ── Step 1: Lookup finished matches ─────────────────────────────────────
    {
      name      = "LookupFinishedMatches"
      type      = "Lookup"
      dependsOn = [{ activity = "LineageStart", dependencyConditions = ["Succeeded"] }]
      typeProperties = {
        source = {
          type       = "JsonSource"
          storeSettings = {
            type                     = "AzureBlobFSReadSettings"
            recursive                = false
            enablePartitionDiscovery = false
          }
          formatSettings = {
            type = "JsonReadSettings"
          }
        }
        dataset = {
          referenceName = "ds_innings_tracker_index"
          type          = "DatasetReference"
        }
        firstRowOnly = false
      }
    },
    # ── Step 2: ForEach → reprocess each match ───────────────────────────────
    {
      name      = "ForEachMatch"
      type      = "ForEach"
      dependsOn = [{ activity = "LookupFinishedMatches", dependencyConditions = ["Succeeded"] }]
      typeProperties = {
        items = {
          value = "@activity('LookupFinishedMatches').output.value[0].matches"
          type  = "Expression"
        }
        isSequential = false
        batchCount   = 5
        activities   = [
          {
            name = "ReprocessSilverForMatch"
            type = "ExecutePipeline"
            typeProperties = {
              pipeline = {
                referenceName = "batch_silver_reprocess"
                type          = "PipelineReference"
              }
              parameters = {
                event_id = {
                  value = "@item().event_id"
                  type  = "Expression"
                }
              }
              waitOnCompletion = true
            }
          }
        ]
      }
    },
    # ── Lineage: COMPLETE ───────────────────────────────────────────────────
    {
      name      = "LineageComplete"
      type      = "WebActivity"
      dependsOn = [{ activity = "ForEachMatch", dependencyConditions = ["Succeeded"] }]
      typeProperties = {
        url    = local.marquez_lineage_url != "" ? local.marquez_lineage_url : "http://localhost"
        method = "POST"
        headers = { "Content-Type" = "application/json" }
        body = {
          value = local.ol_complete_nightly
          type  = "Expression"
        }
      }
    }
  ])
}

# ---------------------------------------------------------------------------
# Dataset — innings tracker index (gold container)
# Used by the nightly pipeline Lookup to get finished match event_ids.
# ---------------------------------------------------------------------------

resource "azurerm_data_factory_dataset_json" "innings_tracker_index" {
  name                = "ds_innings_tracker_index"
  data_factory_id     = azurerm_data_factory.main.id
  linked_service_name = azurerm_data_factory_linked_service_data_lake_storage_gen2.data_lake.name

  azure_blob_storage_location {
    container                 = "gold"
    path                      = "cricket/innings_tracker"
    filename                  = "index.json"
    dynamic_container_enabled = false
    dynamic_path_enabled      = false
    dynamic_filename_enabled  = false
  }

  encoding = "UTF-8"
}

# ---------------------------------------------------------------------------
# Trigger — nightly schedule at 03:00 UTC
# ---------------------------------------------------------------------------

resource "azurerm_data_factory_trigger_schedule" "nightly_batch_silver" {
  name            = "trigger_nightly_batch_silver"
  data_factory_id = azurerm_data_factory.main.id
  pipeline_name   = azurerm_data_factory_pipeline.nightly_batch_silver.name

  interval  = 1
  frequency = "Day"

  schedule {
    hours   = [3]
    minutes = [0]
  }

  activated = true
}
