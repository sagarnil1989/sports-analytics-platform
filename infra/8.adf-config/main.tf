# ---------------------------------------------------------------------------
# ADF — Key Vault linked service
# Allows ADF to read the Databricks PAT from Key Vault at runtime.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# ADF — Blob Storage linked service (script distribution for Batch)
#
# ADF Custom activity downloads all blobs from folderPath in this LS
# into the Batch task working directory before running the command.
# ---------------------------------------------------------------------------

resource "azurerm_data_factory_linked_service_azure_blob_storage" "scripts" {
  name              = "ls_batch_scripts_storage"
  data_factory_id   = data.azurerm_data_factory.main.id
  connection_string = data.azurerm_storage_account.data_lake.primary_connection_string
}

# ---------------------------------------------------------------------------
# ADF — Azure Batch linked service
#
# azurerm provider has no dedicated resource for this; use azapi.
# Authentication: Batch account key stored directly (key is in TF state but
# this is an internal pipeline-only account with no external exposure).
# ---------------------------------------------------------------------------

resource "azapi_resource" "adf_ls_azure_batch" {
  type      = "Microsoft.DataFactory/factories/linkedservices@2018-06-01"
  name      = "ls_azure_batch"
  parent_id = data.azurerm_data_factory.main.id

  body = {
    properties = {
      type = "AzureBatch"
      typeProperties = {
        accountName = data.azurerm_batch_account.main.name
        batchUri    = "https://${data.azurerm_batch_account.main.name}.${data.azurerm_resource_group.main.location}.batch.azure.com"
        poolName    = local.batch_pool
        accessKey = {
          type  = "SecureString"
          value = data.azurerm_batch_account.main.primary_access_key
        }
        linkedServiceName = {
          referenceName = azurerm_data_factory_linked_service_azure_blob_storage.scripts.name
          type          = "LinkedServiceReference"
        }
      }
    }
  }

  schema_validation_enabled = false

  depends_on = [azurerm_data_factory_linked_service_azure_blob_storage.scripts]
}

# ---------------------------------------------------------------------------
# ADF — Key Vault linked service
# Allows ADF to read the Databricks PAT from Key Vault at runtime.
# ---------------------------------------------------------------------------

resource "azurerm_data_factory_linked_service_key_vault" "main" {
  name            = "ls_keyvault"
  data_factory_id = data.azurerm_data_factory.main.id
  key_vault_id    = data.azurerm_key_vault.main.id
}

# Grant ADF managed identity read access to Key Vault secrets (RBAC model).
resource "azurerm_role_assignment" "adf_kv_secrets_user" {
  scope                = data.azurerm_key_vault.main.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = data.azurerm_data_factory.main.identity[0].principal_id
}

# Grant ADF managed identity write access to blob storage.
# Required for the Web Activity that writes watermarks.json via storage REST API.
resource "azurerm_role_assignment" "adf_storage_blob_contributor" {
  scope                = data.azurerm_storage_account.data_lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_data_factory.main.identity[0].principal_id
}

# ---------------------------------------------------------------------------
# ADF — parameterized dataset for the landing index blob
#
# Used by the native Delete activity (cleanup_landing) to delete
# landing/{run_id}/index.json without any compute. The Delete activity
# handles "blob not found" gracefully — no error if index_new_snapshots
# itself failed before writing the file.
# ---------------------------------------------------------------------------

resource "azapi_resource" "adf_dataset_landing_index" {
  type      = "Microsoft.DataFactory/factories/datasets@2018-06-01"
  name      = "ds_landing_index"
  parent_id = data.azurerm_data_factory.main.id

  body = {
    properties = {
      type = "AzureBlob"
      linkedServiceName = {
        referenceName = azurerm_data_factory_linked_service_azure_blob_storage.scripts.name
        type          = "LinkedServiceReference"
      }
      parameters = {
        run_id = { type = "string" }
      }
      typeProperties = {
        folderPath = {
          value = "@concat('landing/', dataset().run_id)"
          type  = "Expression"
        }
        fileName = "index.json"
        format   = { type = "TextFormat" }
      }
    }
  }

  schema_validation_enabled = false
  depends_on = [azurerm_data_factory_linked_service_azure_blob_storage.scripts]
}

# ---------------------------------------------------------------------------
# ADF — Databricks linked service
#
# Uses job clusters (new cluster per pipeline run) — no always-on cost.
# Cluster spec: single-node Standard_F4s_v2 (4 vCPU, 8 GB), Databricks Runtime 15.4 LTS.
# IO-bound workload — more cores don't help; pipeline timeout set to 4 hours per activity.
# ---------------------------------------------------------------------------

resource "azurerm_data_factory_linked_service_azure_databricks" "main" {
  name            = "ls_databricks"
  data_factory_id = data.azurerm_data_factory.main.id
  description     = "Databricks workspace for silver/gold pipeline processing"
  adb_domain      = "https://${data.azurerm_databricks_workspace.main.workspace_url}"

  key_vault_password {
    linked_service_name = azurerm_data_factory_linked_service_key_vault.main.name
    secret_name         = "databricks-pat"
  }

  new_cluster_config {
    node_type             = "Standard_F4s_v2"   # 4 vCPU, 8 GB — F-series compute-optimized, cheapest supported node for IO-bound workload
    cluster_version       = "15.4.x-scala2.12"
    min_number_of_workers = 1
    max_number_of_workers = 1
    driver_node_type      = "Standard_F4s_v2"

    # Single-node mode: driver acts as both driver and executor.
    # The worker node is provisioned by ADF but Databricks does not use it for execution.
    # Terraform provider requires max_number_of_workers >= 1 so we cannot set 0 here.
    spark_config = {
      "spark.databricks.cluster.profile" = "singleNode"
      "spark.master"                     = "local[*]"
    }

    spark_environment_variables = {
      "PYSPARK_PYTHON" = "/databricks/python3/bin/python3"
    }

    custom_tags = {
      "project"     = local.config.project
      "environment" = local.config.environment
      "managed_by"  = "terraform"
    }
  }

  depends_on = [azurerm_data_factory_linked_service_key_vault.main]
}

# ---------------------------------------------------------------------------
# ADF — pipeline: build ended match (daily scheduled)
#
# Activity 1: bronze_to_silver notebook (no event_id = all quiet matches)
#   Parses bronze → silver for matches quiet >1 hour.
# Activity 2: silver_to_gold notebook (depends on Activity 1 success)
#   Rebuilds innings_1_from_silver.json from silver. No bronze access.
# Activity 3: discover_cricket_ended notebook (depends on Activity 2 success)
#   Scans gold tracker files and writes the ended index to bronze.
# Activity 4a: hypothesis_inn2_over6 notebook (depends on Activity 3 success)
#   Scans silver for inn2 over-6 odds favourites, writes gold hypothesis JSON.
# Activity 4b: hypothesis_timeout_wicket notebook (depends on Activity 3 success)
#   Detects strategic timeouts and checks for wickets, writes gold hypothesis JSON.
#   Runs in parallel with 4a.
#
# Schedule: daily at 02:00 CET (01:00 UTC)
# ---------------------------------------------------------------------------

resource "azurerm_data_factory_pipeline" "build_ended_match" {
  name            = "pl_build_ended_match"
  data_factory_id = data.azurerm_data_factory.main.id
  description     = "Daily (Batch): landing index → silver parse → gold rebuild → ended index → update watermark. Hypothesis runs separately via pl_hypothesis."

  activities_json = jsonencode([
    {
      name = "index_new_snapshots"
      type = "Custom"
      policy = {
        timeout = "0.01:00:00"
      }
      linkedServiceName = {
        referenceName = "ls_azure_batch"
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        command = "python3 index_new_snapshots.py"
        resourceLinkedService = {
          referenceName = azurerm_data_factory_linked_service_azure_blob_storage.scripts.name
          type          = "LinkedServiceReference"
        }
        folderPath          = "batch-scripts"
        retentionTimeInDays = 1
        extendedProperties = {
          KEY_VAULT_URI              = local.kv_uri
          MANAGED_IDENTITY_CLIENT_ID = data.azurerm_user_assigned_identity.batch_pool.client_id
          RUN_ID                     = "@pipeline().RunId"
        }
      }
    },
    {
      name = "bronze_to_silver"
      type = "Custom"
      policy = {
        timeout = "0.08:00:00"
      }
      dependsOn = [
        {
          activity             = "index_new_snapshots"
          dependencyConditions = ["Succeeded"]
        }
      ]
      linkedServiceName = {
        referenceName = "ls_azure_batch"
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        command = "python3 bronze_to_silver.py"
        resourceLinkedService = {
          referenceName = azurerm_data_factory_linked_service_azure_blob_storage.scripts.name
          type          = "LinkedServiceReference"
        }
        folderPath          = "batch-scripts"
        retentionTimeInDays = 1
        extendedProperties = {
          KEY_VAULT_URI              = local.kv_uri
          MANAGED_IDENTITY_CLIENT_ID = data.azurerm_user_assigned_identity.batch_pool.client_id
          RUN_ID                     = "@pipeline().RunId"
        }
      }
    },
    {
      name = "silver_to_gold"
      type = "Custom"
      policy = {
        timeout = "0.04:00:00"
      }
      dependsOn = [
        {
          activity             = "bronze_to_silver"
          dependencyConditions = ["Succeeded"]
        }
      ]
      linkedServiceName = {
        referenceName = "ls_azure_batch"
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        command = "python3 silver_to_gold.py"
        resourceLinkedService = {
          referenceName = azurerm_data_factory_linked_service_azure_blob_storage.scripts.name
          type          = "LinkedServiceReference"
        }
        folderPath          = "batch-scripts"
        retentionTimeInDays = 1
        extendedProperties = {
          KEY_VAULT_URI              = local.kv_uri
          MANAGED_IDENTITY_CLIENT_ID = data.azurerm_user_assigned_identity.batch_pool.client_id
          RUN_ID                     = "@pipeline().RunId"
        }
      }
    },
    {
      name = "discover_cricket_ended"
      type = "Custom"
      policy = {
        timeout = "0.01:00:00"
      }
      dependsOn = [
        {
          activity             = "silver_to_gold"
          dependencyConditions = ["Succeeded"]
        }
      ]
      linkedServiceName = {
        referenceName = "ls_azure_batch"
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        command = "python3 discover_cricket_ended.py"
        resourceLinkedService = {
          referenceName = azurerm_data_factory_linked_service_azure_blob_storage.scripts.name
          type          = "LinkedServiceReference"
        }
        folderPath          = "batch-scripts"
        retentionTimeInDays = 1
        extendedProperties = {
          KEY_VAULT_URI              = local.kv_uri
          MANAGED_IDENTITY_CLIENT_ID = data.azurerm_user_assigned_identity.batch_pool.client_id
          RUN_ID                     = "@pipeline().RunId"
        }
      }
    },
    {
      # Advance the watermark only after all processing has succeeded.
      name = "update_watermark"
      type = "Custom"
      policy = {
        timeout = "0.00:10:00"
      }
      dependsOn = [
        {
          activity             = "discover_cricket_ended"
          dependencyConditions = ["Succeeded"]
        }
      ]
      linkedServiceName = {
        referenceName = "ls_azure_batch"
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        command = "python3 update_watermark.py"
        resourceLinkedService = {
          referenceName = azurerm_data_factory_linked_service_azure_blob_storage.scripts.name
          type          = "LinkedServiceReference"
        }
        folderPath          = "batch-scripts"
        retentionTimeInDays = 1
        extendedProperties = {
          KEY_VAULT_URI              = local.kv_uri
          MANAGED_IDENTITY_CLIENT_ID = data.azurerm_user_assigned_identity.batch_pool.client_id
          RUN_ID                     = "@pipeline().RunId"
        }
      }
    },
    {
      # Native Delete activity — no compute, just a storage API call.
      # Handles "blob not found" gracefully if index_new_snapshots itself failed.
      name = "cleanup_landing"
      type = "Delete"
      policy = {
        timeout = "0.00:05:00"
        retry   = 0
      }
      dependsOn = [
        {
          activity             = "update_watermark"
          dependencyConditions = ["Failed", "Skipped"]
        }
      ]
      typeProperties = {
        dataset = {
          referenceName = "ds_landing_index"
          type          = "DatasetReference"
          parameters = {
            run_id = "@pipeline().RunId"
          }
        }
        enableLogging = false
        storeSettings = {
          type      = "AzureBlobStorageReadSettings"
          recursive = false
        }
      }
    },
    {
      # Explicitly fail the pipeline run so ADF shows Failed (not Succeeded)
      # even though cleanup_landing succeeded on the failure branch.
      name = "fail_pipeline"
      type = "Fail"
      dependsOn = [
        {
          activity             = "cleanup_landing"
          dependencyConditions = ["Completed"]
        }
      ]
      typeProperties = {
        message   = "Pipeline failed — see activity above for root cause. Landing index cleaned up."
        errorCode = "UpstreamFailure"
      }
    }
  ])

  depends_on = [azapi_resource.adf_ls_azure_batch, azapi_resource.adf_dataset_landing_index]
}

# ---------------------------------------------------------------------------
# ADF — pipeline: pl_hypothesis (Batch, manual trigger)
#
# Runs both hypothesis scripts in parallel against already-built gold data.
# Run this AFTER pl_build_ended_match has succeeded.
# ---------------------------------------------------------------------------

resource "azurerm_data_factory_pipeline" "hypothesis" {
  name            = "pl_hypothesis"
  data_factory_id = data.azurerm_data_factory.main.id
  description     = "Manual (Batch): runs both hypothesis scripts in parallel. Use after pl_build_ended_match has completed."

  activities_json = jsonencode([
    {
      name = "hypothesis_inn2_over6"
      type = "Custom"
      policy = {
        timeout = "0.01:00:00"
      }
      linkedServiceName = {
        referenceName = "ls_azure_batch"
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        command = "python3 hypothesis_inn2_over6.py"
        resourceLinkedService = {
          referenceName = azurerm_data_factory_linked_service_azure_blob_storage.scripts.name
          type          = "LinkedServiceReference"
        }
        folderPath          = "batch-scripts"
        retentionTimeInDays = 1
        extendedProperties = {
          KEY_VAULT_URI              = local.kv_uri
          MANAGED_IDENTITY_CLIENT_ID = data.azurerm_user_assigned_identity.batch_pool.client_id
          RUN_ID                     = "@pipeline().RunId"
        }
      }
    },
    {
      name = "hypothesis_timeout_wicket"
      type = "Custom"
      policy = {
        timeout = "0.01:00:00"
      }
      linkedServiceName = {
        referenceName = "ls_azure_batch"
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        command = "python3 hypothesis_timeout_wicket.py"
        resourceLinkedService = {
          referenceName = azurerm_data_factory_linked_service_azure_blob_storage.scripts.name
          type          = "LinkedServiceReference"
        }
        folderPath          = "batch-scripts"
        retentionTimeInDays = 1
        extendedProperties = {
          KEY_VAULT_URI              = local.kv_uri
          MANAGED_IDENTITY_CLIENT_ID = data.azurerm_user_assigned_identity.batch_pool.client_id
          RUN_ID                     = "@pipeline().RunId"
        }
      }
    }
  ])

  depends_on = [azapi_resource.adf_ls_azure_batch]
}

resource "azurerm_data_factory_trigger_schedule" "build_ended_match" {
  name            = "trigger_build_ended_match"
  data_factory_id = data.azurerm_data_factory.main.id
  pipeline_name   = azurerm_data_factory_pipeline.build_ended_match.name

  interval  = 1
  frequency = "Day"

  schedule {
    hours   = [1]   # 01:00 UTC = 02:00 CET / 03:00 CEST
    minutes = [0]
  }

  activated = true
}

# ---------------------------------------------------------------------------
# ADF — pipeline: backfill (manual trigger only)
#
# Activity 1: bronze_to_silver notebook
#   Pass event_id to process one quiet match, or leave empty to process all quiet matches.
# Activity 2: silver_to_gold notebook (depends on Activity 1 success)
#   Rebuilds gold for the same event_id (or all stale events if empty).
#
# Both activities receive the same event_id parameter.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# ADF — pipeline: build ended match — Databricks variant (daily scheduled)
#
# Mirror of pl_build_ended_match but running on Databricks job clusters.
# Activity 0: index_new_snapshots   — delta-indexes new bronze manifests into landing/
# Activity 1: bronze_to_silver      — reads landing index, writes silver
# Activity 2: silver_to_gold        — rebuilds gold from silver
# Activity 3: discover_cricket_ended — scans gold tracker files, writes ended index
# Activity 4a/4b: hypothesis notebooks — parallel, depend on Activity 3
#
# Trigger is created but deactivated — activate manually when switching pipelines.
# ---------------------------------------------------------------------------

resource "azurerm_data_factory_pipeline" "build_ended_match_databricks" {
  name            = "pl_build_ended_match_databricks"
  data_factory_id = data.azurerm_data_factory.main.id
  description     = "Daily (Databricks): landing index → silver parse → gold rebuild → ended index → update watermark. Mirror of pl_build_ended_match. Hypothesis runs separately via pl_hypothesis_databricks."

  activities_json = jsonencode([
    {
      name = "index_new_snapshots"
      type = "DatabricksNotebook"
      linkedServiceName = {
        referenceName = azurerm_data_factory_linked_service_azure_databricks.main.name
        type          = "LinkedServiceReference"
      }
      policy = {
        timeout = "0.01:00:00"
      }
      typeProperties = {
        notebookPath = "/cricket-pipeline/index_new_snapshots"
        baseParameters = {
          run_id   = { value = "@pipeline().RunId", type = "Expression" }
          event_id = ""
        }
      }
    },
    {
      name = "bronze_to_silver"
      type = "DatabricksNotebook"
      linkedServiceName = {
        referenceName = azurerm_data_factory_linked_service_azure_databricks.main.name
        type          = "LinkedServiceReference"
      }
      policy = {
        timeout = "0.08:00:00"
      }
      dependsOn = [
        {
          activity             = "index_new_snapshots"
          dependencyConditions = ["Succeeded"]
        }
      ]
      typeProperties = {
        notebookPath = "/cricket-pipeline/bronze_to_silver"
        baseParameters = {
          run_id   = { value = "@pipeline().RunId", type = "Expression" }
          event_id = ""
        }
      }
    },
    {
      name = "silver_to_gold"
      type = "DatabricksNotebook"
      linkedServiceName = {
        referenceName = azurerm_data_factory_linked_service_azure_databricks.main.name
        type          = "LinkedServiceReference"
      }
      policy = {
        timeout = "0.04:00:00"
      }
      dependsOn = [
        {
          activity             = "bronze_to_silver"
          dependencyConditions = ["Succeeded"]
        }
      ]
      typeProperties = {
        notebookPath = "/cricket-pipeline/silver_to_gold"
        baseParameters = {
          run_id   = { value = "@pipeline().RunId", type = "Expression" }
          event_id = ""
        }
      }
    },
    {
      name = "discover_cricket_ended"
      type = "DatabricksNotebook"
      linkedServiceName = {
        referenceName = azurerm_data_factory_linked_service_azure_databricks.main.name
        type          = "LinkedServiceReference"
      }
      policy = {
        timeout = "0.01:00:00"
      }
      dependsOn = [
        {
          activity             = "silver_to_gold"
          dependencyConditions = ["Succeeded"]
        }
      ]
      typeProperties = {
        notebookPath = "/cricket-pipeline/discover_cricket_ended"
        baseParameters = {
          run_id = { value = "@pipeline().RunId", type = "Expression" }
        }
      }
    },
    {
      # Web Activity writes watermarks.json via blob storage REST API.
      # No Databricks cluster spun up — ADF managed identity authenticates directly.
      # Body comes from index_new_snapshots notebook exit output (watermark JSON payload).
      name = "update_watermark"
      type = "WebActivity"
      policy = {
        timeout = "0.00:05:00"
        retry   = 1
      }
      dependsOn = [
        {
          activity             = "discover_cricket_ended"
          dependencyConditions = ["Succeeded"]
        }
      ]
      typeProperties = {
        url    = "https://${data.azurerm_storage_account.data_lake.name}.blob.core.windows.net/landing/control/watermarks.json"
        method = "PUT"
        headers = {
          "x-ms-blob-type" = "BlockBlob"
          "Content-Type"   = "application/json"
        }
        body = "@activity('index_new_snapshots').output.runOutput"
        authentication = {
          type     = "MSI"
          resource = "https://storage.azure.com/"
        }
      }
    },
    {
      # Native Delete activity — no compute, just a storage API call.
      name = "cleanup_landing"
      type = "Delete"
      policy = {
        timeout = "0.00:05:00"
        retry   = 0
      }
      dependsOn = [
        {
          activity             = "update_watermark"
          dependencyConditions = ["Failed", "Skipped"]
        }
      ]
      typeProperties = {
        dataset = {
          referenceName = "ds_landing_index"
          type          = "DatasetReference"
          parameters = {
            run_id = "@pipeline().RunId"
          }
        }
        enableLogging = false
        storeSettings = {
          type      = "AzureBlobStorageReadSettings"
          recursive = false
        }
      }
    },
    {
      name = "fail_pipeline"
      type = "Fail"
      dependsOn = [
        {
          activity             = "cleanup_landing"
          dependencyConditions = ["Completed"]
        }
      ]
      typeProperties = {
        message   = "Pipeline failed — see activity above for root cause. Landing index cleaned up."
        errorCode = "UpstreamFailure"
      }
    }
  ])

  depends_on = [azurerm_data_factory_linked_service_azure_databricks.main, azapi_resource.adf_dataset_landing_index]
}

resource "azurerm_data_factory_trigger_schedule" "build_ended_match_databricks" {
  name            = "trigger_build_ended_match_databricks"
  data_factory_id = data.azurerm_data_factory.main.id
  pipeline_name   = azurerm_data_factory_pipeline.build_ended_match_databricks.name

  interval  = 1
  frequency = "Day"

  schedule {
    hours   = [1]   # 01:00 UTC = 02:00 CET / 03:00 CEST
    minutes = [0]
  }

  activated = false
}

resource "azurerm_data_factory_pipeline" "backfill" {
  name            = "pl_backfill"
  data_factory_id = data.azurerm_data_factory.main.id
  description     = "Manual backfill (Batch): landing index → silver parse → gold rebuild → update watermark. Pass event_id for one match, or empty for full backfill."

  parameters = {
    event_id = ""
  }

  activities_json = jsonencode([
    {
      name = "index_new_snapshots"
      type = "Custom"
      policy = {
        timeout = "0.01:00:00"
      }
      linkedServiceName = {
        referenceName = "ls_azure_batch"
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        command = "python3 index_new_snapshots.py"
        resourceLinkedService = {
          referenceName = azurerm_data_factory_linked_service_azure_blob_storage.scripts.name
          type          = "LinkedServiceReference"
        }
        folderPath          = "batch-scripts"
        retentionTimeInDays = 1
        extendedProperties = {
          KEY_VAULT_URI              = local.kv_uri
          MANAGED_IDENTITY_CLIENT_ID = data.azurerm_user_assigned_identity.batch_pool.client_id
          RUN_ID                     = "@pipeline().RunId"
          EVENT_ID                   = "@pipeline().parameters.event_id"
        }
      }
    },
    {
      name = "bronze_to_silver"
      type = "Custom"
      policy = {
        timeout = "0.04:00:00"
      }
      dependsOn = [
        {
          activity             = "index_new_snapshots"
          dependencyConditions = ["Succeeded"]
        }
      ]
      linkedServiceName = {
        referenceName = "ls_azure_batch"
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        command = "python3 bronze_to_silver.py"
        resourceLinkedService = {
          referenceName = azurerm_data_factory_linked_service_azure_blob_storage.scripts.name
          type          = "LinkedServiceReference"
        }
        folderPath          = "batch-scripts"
        retentionTimeInDays = 1
        extendedProperties = {
          KEY_VAULT_URI              = local.kv_uri
          MANAGED_IDENTITY_CLIENT_ID = data.azurerm_user_assigned_identity.batch_pool.client_id
          RUN_ID                     = "@pipeline().RunId"
          EVENT_ID                   = "@pipeline().parameters.event_id"
        }
      }
    },
    {
      name = "silver_to_gold"
      type = "Custom"
      policy = {
        timeout = "0.04:00:00"
      }
      dependsOn = [
        {
          activity             = "bronze_to_silver"
          dependencyConditions = ["Succeeded"]
        }
      ]
      linkedServiceName = {
        referenceName = "ls_azure_batch"
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        command = "python3 silver_to_gold.py"
        resourceLinkedService = {
          referenceName = azurerm_data_factory_linked_service_azure_blob_storage.scripts.name
          type          = "LinkedServiceReference"
        }
        folderPath          = "batch-scripts"
        retentionTimeInDays = 1
        extendedProperties = {
          KEY_VAULT_URI              = local.kv_uri
          MANAGED_IDENTITY_CLIENT_ID = data.azurerm_user_assigned_identity.batch_pool.client_id
          RUN_ID                     = "@pipeline().RunId"
          EVENT_ID                   = "@pipeline().parameters.event_id"
        }
      }
    },
    {
      # update_watermark is a no-op when EVENT_ID is set (backfill mode).
      name = "update_watermark"
      type = "Custom"
      policy = {
        timeout = "0.00:10:00"
      }
      dependsOn = [
        {
          activity             = "silver_to_gold"
          dependencyConditions = ["Succeeded"]
        }
      ]
      linkedServiceName = {
        referenceName = "ls_azure_batch"
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        command = "python3 update_watermark.py"
        resourceLinkedService = {
          referenceName = azurerm_data_factory_linked_service_azure_blob_storage.scripts.name
          type          = "LinkedServiceReference"
        }
        folderPath          = "batch-scripts"
        retentionTimeInDays = 1
        extendedProperties = {
          KEY_VAULT_URI              = local.kv_uri
          MANAGED_IDENTITY_CLIENT_ID = data.azurerm_user_assigned_identity.batch_pool.client_id
          RUN_ID                     = "@pipeline().RunId"
          EVENT_ID                   = "@pipeline().parameters.event_id"
        }
      }
    },
    {
      name = "cleanup_landing"
      type = "Delete"
      policy = {
        timeout = "0.00:05:00"
        retry   = 0
      }
      dependsOn = [
        {
          activity             = "update_watermark"
          dependencyConditions = ["Failed", "Skipped"]
        }
      ]
      typeProperties = {
        dataset = {
          referenceName = "ds_landing_index"
          type          = "DatasetReference"
          parameters = {
            run_id = "@pipeline().RunId"
          }
        }
        enableLogging = false
        storeSettings = {
          type      = "AzureBlobStorageReadSettings"
          recursive = false
        }
      }
    },
    {
      name = "fail_pipeline"
      type = "Fail"
      dependsOn = [
        {
          activity             = "cleanup_landing"
          dependencyConditions = ["Completed"]
        }
      ]
      typeProperties = {
        message   = "Pipeline failed — see activity above for root cause. Landing index cleaned up."
        errorCode = "UpstreamFailure"
      }
    }
  ])

  depends_on = [azapi_resource.adf_ls_azure_batch, azapi_resource.adf_dataset_landing_index]
}

# ---------------------------------------------------------------------------
# ADF — pipeline: backfill (Databricks version)
# Manual trigger only. Same logic as pl_backfill but runs on Databricks.
# ---------------------------------------------------------------------------

resource "azurerm_data_factory_pipeline" "backfill_databricks" {
  name            = "pl_backfill_databricks"
  data_factory_id = data.azurerm_data_factory.main.id
  description     = "Manual backfill (Databricks): landing index → silver parse → gold rebuild → update watermark. Mirror of pl_backfill."

  parameters = {
    event_id = ""
  }

  activities_json = jsonencode([
    {
      name = "index_new_snapshots"
      type = "DatabricksNotebook"
      linkedServiceName = {
        referenceName = azurerm_data_factory_linked_service_azure_databricks.main.name
        type          = "LinkedServiceReference"
      }
      policy = {
        timeout = "0.01:00:00"
      }
      typeProperties = {
        notebookPath = "/cricket-pipeline/index_new_snapshots"
        baseParameters = {
          run_id   = { value = "@pipeline().RunId", type = "Expression" }
          event_id = { value = "@pipeline().parameters.event_id", type = "Expression" }
        }
      }
    },
    {
      name = "bronze_to_silver"
      type = "DatabricksNotebook"
      linkedServiceName = {
        referenceName = azurerm_data_factory_linked_service_azure_databricks.main.name
        type          = "LinkedServiceReference"
      }
      policy = {
        timeout = "0.04:00:00"
      }
      dependsOn = [
        {
          activity             = "index_new_snapshots"
          dependencyConditions = ["Succeeded"]
        }
      ]
      typeProperties = {
        notebookPath = "/cricket-pipeline/bronze_to_silver"
        baseParameters = {
          run_id   = { value = "@pipeline().RunId", type = "Expression" }
          event_id = { value = "@pipeline().parameters.event_id", type = "Expression" }
        }
      }
    },
    {
      name = "silver_to_gold"
      type = "DatabricksNotebook"
      linkedServiceName = {
        referenceName = azurerm_data_factory_linked_service_azure_databricks.main.name
        type          = "LinkedServiceReference"
      }
      policy = {
        timeout = "0.04:00:00"
      }
      dependsOn = [
        {
          activity             = "bronze_to_silver"
          dependencyConditions = ["Succeeded"]
        }
      ]
      typeProperties = {
        notebookPath = "/cricket-pipeline/silver_to_gold"
        baseParameters = {
          run_id   = { value = "@pipeline().RunId", type = "Expression" }
          event_id = { value = "@pipeline().parameters.event_id", type = "Expression" }
        }
      }
    },
    {
      name = "update_watermark"
      type = "DatabricksNotebook"
      linkedServiceName = {
        referenceName = azurerm_data_factory_linked_service_azure_databricks.main.name
        type          = "LinkedServiceReference"
      }
      policy = {
        timeout = "0.00:10:00"
      }
      dependsOn = [
        {
          activity             = "silver_to_gold"
          dependencyConditions = ["Succeeded"]
        }
      ]
      typeProperties = {
        notebookPath = "/cricket-pipeline/update_watermark"
        baseParameters = {
          run_id   = { value = "@pipeline().RunId", type = "Expression" }
          event_id = { value = "@pipeline().parameters.event_id", type = "Expression" }
        }
      }
    },
    {
      name = "cleanup_landing"
      type = "Delete"
      policy = {
        timeout = "0.00:05:00"
        retry   = 0
      }
      dependsOn = [
        {
          activity             = "update_watermark"
          dependencyConditions = ["Failed", "Skipped"]
        }
      ]
      typeProperties = {
        dataset = {
          referenceName = "ds_landing_index"
          type          = "DatasetReference"
          parameters = {
            run_id = "@pipeline().RunId"
          }
        }
        enableLogging = false
        storeSettings = {
          type      = "AzureBlobStorageReadSettings"
          recursive = false
        }
      }
    },
    {
      name = "fail_pipeline"
      type = "Fail"
      dependsOn = [
        {
          activity             = "cleanup_landing"
          dependencyConditions = ["Completed"]
        }
      ]
      typeProperties = {
        message   = "Pipeline failed — see activity above for root cause. Landing index cleaned up."
        errorCode = "UpstreamFailure"
      }
    }
  ])

  depends_on = [azurerm_data_factory_linked_service_azure_databricks.main, azapi_resource.adf_dataset_landing_index]
}

# ---------------------------------------------------------------------------
# ADF — pipeline: hypothesis (Databricks, manual trigger)
#
# Runs the two hypothesis notebooks in parallel after a completed
# pl_build_ended_match run. Intended as a standalone re-run when only
# hypothesis logic needs refreshing without re-processing bronze/silver/gold.
# ---------------------------------------------------------------------------

resource "azurerm_data_factory_pipeline" "hypothesis_databricks" {
  name            = "pl_hypothesis_databricks"
  data_factory_id = data.azurerm_data_factory.main.id
  description     = "Manual (Databricks): runs both hypothesis notebooks in parallel. Use after pl_build_ended_match has completed."

  activities_json = jsonencode([
    {
      name = "hypothesis_inn2_over6"
      type = "DatabricksNotebook"
      linkedServiceName = {
        referenceName = azurerm_data_factory_linked_service_azure_databricks.main.name
        type          = "LinkedServiceReference"
      }
      policy = {
        timeout = "0.01:00:00"
      }
      typeProperties = {
        notebookPath = "/cricket-pipeline/hypothesis/inn2_over6"
        baseParameters = {
          run_id = { value = "@pipeline().RunId", type = "Expression" }
        }
      }
    },
    {
      name = "hypothesis_timeout_wicket"
      type = "DatabricksNotebook"
      linkedServiceName = {
        referenceName = azurerm_data_factory_linked_service_azure_databricks.main.name
        type          = "LinkedServiceReference"
      }
      policy = {
        timeout = "0.01:00:00"
      }
      typeProperties = {
        notebookPath = "/cricket-pipeline/hypothesis/timeout_wicket"
        baseParameters = {
          run_id = { value = "@pipeline().RunId", type = "Expression" }
        }
      }
    }
  ])

  depends_on = [azurerm_data_factory_linked_service_azure_databricks.main]
}

