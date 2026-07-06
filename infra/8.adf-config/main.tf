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
# Retained for reference and backward compatibility with any manual
# pl_backfill runs that still use the old landing-index pattern.
# The active pipelines (pl_build_ended_match, pl_backfill, and their
# Databricks mirrors) no longer depend on this dataset — they use
# process-queue/in-progress/{run_id}.json instead.
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
  depends_on                = [azurerm_data_factory_linked_service_azure_blob_storage.scripts]
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
    node_type             = "Standard_F4s_v2" # 4 vCPU, 8 GB — F-series compute-optimized, cheapest supported node for IO-bound workload
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
  description     = "Daily (Batch): process-queue → silver parse → gold rebuild → ended index → delete pending markers. Hypothesis runs separately via pl_hypothesis."

  activities_json = jsonencode([
    {
      name = "read_pending_queue"
      type = "Custom"
      policy = {
        timeout = "0.01:00:00"
      }
      linkedServiceName = {
        referenceName = "ls_azure_batch"
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        command = "python3 read_pending_queue.py"
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
          activity             = "read_pending_queue"
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
      name = "refresh_event_finals"
      type = "Custom"
      policy = {
        timeout = "0.00:30:00"
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
        command = "python3 refresh_event_finals.py"
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
          activity             = "refresh_event_finals"
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
      # Delete pending markers for processed events only after full success.
      # On failure the markers stay in place so the next run retries automatically.
      name = "delete_processed_markers"
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
        command = "python3 delete_processed_markers.py"
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

# NOTE: hypothesis, ML retrain, and Over/Under retrain used to be three
# separate pipelines (pl_hypothesis, pl_ml_retrain, pl_over_under_retrain),
# all on the same Saturday 03:00 UTC schedule. Consolidated 2026-06-28 into
# a single pipeline — see azurerm_data_factory_pipeline.ml_and_hypothesis
# below. Only the Over/Under branch actually depends on the train/test
# cutoff date; hypothesis and the other ML notebooks do not, and run
# independently of it and of each other.

resource "azurerm_data_factory_trigger_schedule" "build_ended_match" {
  name            = "trigger_build_ended_match"
  data_factory_id = data.azurerm_data_factory.main.id
  pipeline_name   = azurerm_data_factory_pipeline.build_ended_match.name

  interval  = 1
  frequency = "Day"

  schedule {
    hours   = [1] # 01:00 UTC = 02:00 CET / 03:00 CEST
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
# Activity 0: read_pending_queue    — lists process-queue/pending/ markers, writes in-progress
# Activity 1: bronze_to_silver      — reads in-progress file, writes silver
# Activity 2: silver_to_gold        — rebuilds gold from silver
# Activity 3: discover_cricket_ended — scans gold tracker files, writes ended index
# Activity 4: delete_processed_markers — deletes pending markers for succeeded events
#
# Trigger is created but deactivated — activate manually when switching pipelines.
# ---------------------------------------------------------------------------

resource "azurerm_data_factory_pipeline" "build_ended_match_databricks" {
  name            = "pl_build_ended_match_databricks"
  data_factory_id = data.azurerm_data_factory.main.id
  description     = "Daily (Databricks): process-queue → silver parse → gold rebuild → ended index → delete pending markers. Mirror of pl_build_ended_match. Hypothesis runs separately via pl_hypothesis_databricks."

  activities_json = jsonencode([
    {
      name = "read_pending_queue"
      type = "DatabricksNotebook"
      linkedServiceName = {
        referenceName = azurerm_data_factory_linked_service_azure_databricks.main.name
        type          = "LinkedServiceReference"
      }
      policy = {
        timeout = "0.01:00:00"
      }
      typeProperties = {
        notebookPath = "/cricket-pipeline/read_pending_queue"
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
          activity             = "read_pending_queue"
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
      name = "refresh_event_finals"
      type = "Custom"
      policy = {
        timeout = "0.00:30:00"
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
        command = "python3 refresh_event_finals.py"
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
          activity             = "refresh_event_finals"
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
      # Delete pending markers for processed events only after full success.
      # On failure the markers stay in place so the next run retries automatically.
      name = "delete_processed_markers"
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
          activity             = "discover_cricket_ended"
          dependencyConditions = ["Succeeded"]
        }
      ]
      typeProperties = {
        notebookPath = "/cricket-pipeline/delete_processed_markers"
        baseParameters = {
          run_id   = { value = "@pipeline().RunId", type = "Expression" }
          event_id = ""
        }
      }
    }
  ])

  depends_on = [azurerm_data_factory_linked_service_azure_databricks.main]
}

resource "azurerm_data_factory_trigger_schedule" "build_ended_match_databricks" {
  name            = "trigger_build_ended_match_databricks"
  data_factory_id = data.azurerm_data_factory.main.id
  pipeline_name   = azurerm_data_factory_pipeline.build_ended_match_databricks.name

  interval  = 1
  frequency = "Day"

  schedule {
    hours   = [1] # 01:00 UTC = 02:00 CET / 03:00 CEST
    minutes = [0]
  }

  activated = false
}

resource "azurerm_data_factory_pipeline" "backfill" {
  name            = "pl_backfill"
  data_factory_id = data.azurerm_data_factory.main.id
  description     = "Manual backfill (Batch): process-queue → silver parse → gold rebuild → delete markers (no-op in backfill mode). Pass event_id for one match, or empty for all pending."

  parameters = {
    event_id = ""
  }

  activities_json = jsonencode([
    {
      name = "read_pending_queue"
      type = "Custom"
      policy = {
        timeout = "0.01:00:00"
      }
      linkedServiceName = {
        referenceName = "ls_azure_batch"
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        command = "python3 read_pending_queue.py"
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
          activity             = "read_pending_queue"
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
      # delete_processed_markers is a no-op when EVENT_ID is set (backfill mode).
      name = "delete_processed_markers"
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
        command = "python3 delete_processed_markers.py"
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
    }
  ])

  depends_on = [azapi_resource.adf_ls_azure_batch]
}

# ---------------------------------------------------------------------------
# ADF — pipeline: backfill (Databricks version)
# Manual trigger only. Same logic as pl_backfill but runs on Databricks.
# ---------------------------------------------------------------------------

resource "azurerm_data_factory_pipeline" "backfill_databricks" {
  name            = "pl_backfill_databricks"
  data_factory_id = data.azurerm_data_factory.main.id
  description     = "Manual backfill (Databricks): process-queue → silver parse → gold rebuild → delete markers (no-op in backfill mode). Mirror of pl_backfill."

  parameters = {
    event_id = ""
  }

  activities_json = jsonencode([
    {
      name = "read_pending_queue"
      type = "DatabricksNotebook"
      linkedServiceName = {
        referenceName = azurerm_data_factory_linked_service_azure_databricks.main.name
        type          = "LinkedServiceReference"
      }
      policy = {
        timeout = "0.01:00:00"
      }
      typeProperties = {
        notebookPath = "/cricket-pipeline/read_pending_queue"
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
          activity             = "read_pending_queue"
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
      # delete_processed_markers is a no-op when event_id is set (backfill mode).
      name = "delete_processed_markers"
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
        notebookPath = "/cricket-pipeline/delete_processed_markers"
        baseParameters = {
          run_id   = { value = "@pipeline().RunId", type = "Expression" }
          event_id = { value = "@pipeline().parameters.event_id", type = "Expression" }
        }
      }
    }
  ])

  depends_on = [azurerm_data_factory_linked_service_azure_databricks.main]
}

# ---------------------------------------------------------------------------
# ADF — pipeline: hypothesis (Databricks, manual trigger)
#
# Runs the two hypothesis notebooks in parallel after a completed
# pl_build_ended_match run. Intended as a standalone re-run when only
# hypothesis logic needs refreshing without re-processing bronze/silver/gold.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# ADF — pipeline: pl_ml_and_hypothesis (weekly, Saturday 03:00 UTC)
#
# Single weekly pipeline consolidating what used to be three separate
# pipelines (pl_hypothesis, pl_ml_retrain, pl_over_under_retrain), all of
# which ran on the same schedule. Hypothesis activities use ls_azure_batch
# (Custom/Batch), ML activities use ls_databricks (DatabricksNotebook).
#
# Activity names match the website page each one feeds (see cricket_display
# function app routes), not the underlying notebook/script filename:
#   HypothesisInn2Over6        -> /api/hypothesis/inn2-over6
#   HypothesisTimeoutWicket    -> /api/hypothesis/timeout-wicket
#   HypothesisInn1Prematch     -> /api/hypothesis/inn1-prematch
#   MlRetrainSummaryFeatureExtraction / MlRetrainSummaryModelTraining
#                               -> /api/ml/retrain-summary
#   MlWinPredictor              -> /api/ml/win-predictor
#   MlScorePredictor             -> /api/ml/score-predictor
#   MlOverUnderFeatureExtraction / MlOverUnderModelTraining
#                               -> /api/ml/over-under
#   MlCutoffGetConfig / MlCutoffCheckStale / MlCutoffUpdate
#                               -> no page of their own; shared plumbing that
#                                  refreshes gold/ml/train_config.json before
#                                  every ML activity that reads it.
#
# Dependency groups (only chained where actually necessary):
#   - HypothesisInn2Over6 / HypothesisTimeoutWicket / HypothesisInn1Prematch
#       — independent of everything else, run in parallel. Don't use the
#         train/test cutoff at all.
#   - MlCutoffGetConfig -> MlCutoffCheckStale
#       — runs first for every ML activity below. Reads gold/ml/train_config.json
#         via the display function's GET /api/ml/over-under/config endpoint.
#         If that cutoff is older than (run date - 7 days), POSTs an update so
#         the cutoff always trails the run date by at most 7 days. This is the
#         ONE standard cutoff every ML notebook reads — no per-notebook cutoffs.
#   - MlCutoffCheckStale -> MlRetrainSummaryFeatureExtraction -> MlRetrainSummaryModelTraining
#       — feature extraction tags each row with split=train/test from the
#         cutoff; model training fits on train only and evaluates held-out
#         on test.
#   - MlCutoffCheckStale -> MlWinPredictor
#   - MlCutoffCheckStale -> MlScorePredictor
#       — these two read trackers directly (not MlRetrainSummaryFeatureExtraction's
#         Parquet) but still split train/test on the same cutoff.
#   - MlCutoffCheckStale -> MlOverUnderFeatureExtraction -> MlOverUnderModelTraining
#
# Train/test split is controlled by gold/ml/train_config.json:
#   {"train_cutoff_date": "2025-12-31"}
# Rows with match_date_utc < cutoff → split=train, else → split=test.
# If the file is absent, all rows are treated as train.
# ---------------------------------------------------------------------------

resource "azurerm_data_factory_pipeline" "ml_and_hypothesis" {
  name            = "pl_ml_and_hypothesis"
  data_factory_id = data.azurerm_data_factory.main.id
  description     = "Weekly: hypothesis refresh (Batch) + ML retrain (Databricks) + Over/Under retrain with cutoff-date refresh (Databricks)."

  activities_json = jsonencode([
    {
      name = "OddsMovementAnalysis"
      type = "Custom"
      policy = {
        timeout = "0.01:00:00"
      }
      linkedServiceName = {
        referenceName = "ls_azure_batch"
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        command = "python3 hypothesis_odds_movement.py"
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
      name = "HypothesisInn2Over6"
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
      name = "HypothesisTimeoutWicket"
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
    },
    {
      name = "HypothesisInn1Prematch"
      type = "Custom"
      policy = {
        timeout = "0.01:00:00"
      }
      linkedServiceName = {
        referenceName = "ls_azure_batch"
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        command = "python3 hypothesis_inn1_prematch.py"
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
      name = "MlRetrainSummaryFeatureExtraction"
      type = "DatabricksNotebook"
      policy = {
        timeout = "0.01:00:00"
      }
      dependsOn = [
        {
          activity             = "MlCutoffCheckStale"
          dependencyConditions = ["Succeeded"]
        }
      ]
      linkedServiceName = {
        referenceName = azurerm_data_factory_linked_service_azure_databricks.main.name
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        notebookPath = "/cricket-pipeline/ml/ml_feature_extraction"
      }
    },
    {
      name = "MlRetrainSummaryModelTraining"
      type = "DatabricksNotebook"
      policy = {
        timeout = "0.01:00:00"
      }
      dependsOn = [
        {
          activity             = "MlRetrainSummaryFeatureExtraction"
          dependencyConditions = ["Succeeded"]
        }
      ]
      linkedServiceName = {
        referenceName = azurerm_data_factory_linked_service_azure_databricks.main.name
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        notebookPath = "/cricket-pipeline/ml/ml_model_training"
      }
    },
    {
      name = "MlWinPredictor"
      type = "DatabricksNotebook"
      policy = {
        timeout = "0.01:00:00"
      }
      dependsOn = [
        {
          activity             = "MlCutoffCheckStale"
          dependencyConditions = ["Succeeded"]
        }
      ]
      linkedServiceName = {
        referenceName = azurerm_data_factory_linked_service_azure_databricks.main.name
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        notebookPath = "/cricket-pipeline/ml/ml_win_predictor"
      }
    },
    {
      name = "MlScorePredictor"
      type = "DatabricksNotebook"
      policy = {
        timeout = "0.01:00:00"
      }
      dependsOn = [
        {
          activity             = "MlCutoffCheckStale"
          dependencyConditions = ["Succeeded"]
        }
      ]
      linkedServiceName = {
        referenceName = azurerm_data_factory_linked_service_azure_databricks.main.name
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        notebookPath = "/cricket-pipeline/ml/inn1_score_predictor"
      }
    },
    {
      name = "MlCutoffGetConfig"
      type = "WebActivity"
      policy = {
        timeout = "0.00:05:00"
      }
      typeProperties = {
        url    = "https://func-ramanuj-display.azurewebsites.net/api/ml/over-under/config"
        method = "GET"
      }
    },
    {
      name = "MlCutoffCheckStale"
      type = "IfCondition"
      dependsOn = [
        {
          activity             = "MlCutoffGetConfig"
          dependencyConditions = ["Succeeded"]
        }
      ]
      typeProperties = {
        expression = {
          type  = "Expression"
          value = "@less(activity('MlCutoffGetConfig').output.train_cutoff_date, formatDateTime(addDays(pipeline().TriggerTime, -7), 'yyyy-MM-dd'))"
        }
        ifTrueActivities = [
          {
            name = "MlCutoffUpdate"
            type = "WebActivity"
            policy = {
              timeout = "0.00:05:00"
            }
            typeProperties = {
              url    = "https://func-ramanuj-display.azurewebsites.net/api/ml/over-under/config"
              method = "POST"
              headers = {
                "Content-Type" = "application/json"
              }
              body = "@concat('{\"train_cutoff_date\":\"', formatDateTime(addDays(pipeline().TriggerTime, -7), 'yyyy-MM-dd'), '\"}')"
            }
          }
        ]
      }
    },
    {
      name = "MlOverUnderFeatureExtraction"
      type = "DatabricksNotebook"
      linkedServiceName = {
        referenceName = azurerm_data_factory_linked_service_azure_databricks.main.name
        type          = "LinkedServiceReference"
      }
      policy = {
        timeout = "0.02:00:00"
      }
      dependsOn = [
        {
          activity             = "MlCutoffCheckStale"
          dependencyConditions = ["Succeeded"]
        }
      ]
      typeProperties = {
        notebookPath = "/cricket-pipeline/ml/extract_over_under_features"
        baseParameters = {
          event_id = ""
        }
      }
    },
    {
      name = "MlOverUnderModelTraining"
      type = "DatabricksNotebook"
      linkedServiceName = {
        referenceName = azurerm_data_factory_linked_service_azure_databricks.main.name
        type          = "LinkedServiceReference"
      }
      policy = {
        timeout = "0.02:00:00"
      }
      dependsOn = [
        {
          activity             = "MlOverUnderFeatureExtraction"
          dependencyConditions = ["Succeeded"]
        }
      ]
      typeProperties = {
        notebookPath = "/cricket-pipeline/ml/train_over_under"
      }
    }
  ])

  depends_on = [
    azapi_resource.adf_ls_azure_batch,
    azurerm_data_factory_linked_service_azure_databricks.main,
  ]
}

resource "azurerm_data_factory_trigger_schedule" "ml_and_hypothesis" {
  name            = "trigger_ml_and_hypothesis"
  data_factory_id = data.azurerm_data_factory.main.id
  pipeline_name   = azurerm_data_factory_pipeline.ml_and_hypothesis.name

  interval  = 1
  frequency = "Week"

  schedule {
    days_of_week = ["Saturday"]
    hours        = [3]
    minutes      = [0]
  }

  activated = true
}

resource "azurerm_data_factory_pipeline" "hypothesis_databricks" {
  name            = "pl_hypothesis_databricks"
  data_factory_id = data.azurerm_data_factory.main.id
  description     = "Manual (Databricks): runs both hypothesis notebooks in parallel. Use after pl_build_ended_match has completed."

  activities_json = jsonencode([
    {
      name = "HypothesisInn2Over6"
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
      name = "HypothesisTimeoutWicket"
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
    },
    {
      name = "HypothesisInn1Prematch"
      type = "DatabricksNotebook"
      linkedServiceName = {
        referenceName = azurerm_data_factory_linked_service_azure_databricks.main.name
        type          = "LinkedServiceReference"
      }
      policy = {
        timeout = "0.01:00:00"
      }
      typeProperties = {
        notebookPath = "/cricket-pipeline/hypothesis/inn1_prematch"
        baseParameters = {
          run_id = { value = "@pipeline().RunId", type = "Expression" }
        }
      }
    }
  ])

  depends_on = [azurerm_data_factory_linked_service_azure_databricks.main]
}

