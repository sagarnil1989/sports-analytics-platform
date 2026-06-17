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

  body = jsonencode({
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
  })

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
  description     = "Daily: silver parse → gold rebuild → ended index. Each activity depends on the previous succeeding."

  activities_json = jsonencode([
    {
      name = "bronze_to_silver"
      type = "Custom"
      policy = {
        timeout = "0.04:00:00"
      }
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
        folderPath         = "batch-scripts"
        retentionTimeInDays = 1
        extendedProperties = {
          KEY_VAULT_URI              = local.kv_uri
          MANAGED_IDENTITY_CLIENT_ID = data.azurerm_user_assigned_identity.batch_pool.client_id
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
        folderPath         = "batch-scripts"
        retentionTimeInDays = 1
        extendedProperties = {
          KEY_VAULT_URI              = local.kv_uri
          MANAGED_IDENTITY_CLIENT_ID = data.azurerm_user_assigned_identity.batch_pool.client_id
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
        folderPath         = "batch-scripts"
        retentionTimeInDays = 1
        extendedProperties = {
          KEY_VAULT_URI              = local.kv_uri
          MANAGED_IDENTITY_CLIENT_ID = data.azurerm_user_assigned_identity.batch_pool.client_id
        }
      }
    },
    {
      name = "hypothesis_inn2_over6"
      type = "Custom"
      policy = {
        timeout = "0.01:00:00"
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
        command = "python3 hypothesis_inn2_over6.py"
        resourceLinkedService = {
          referenceName = azurerm_data_factory_linked_service_azure_blob_storage.scripts.name
          type          = "LinkedServiceReference"
        }
        folderPath         = "batch-scripts"
        retentionTimeInDays = 1
        extendedProperties = {
          KEY_VAULT_URI              = local.kv_uri
          MANAGED_IDENTITY_CLIENT_ID = data.azurerm_user_assigned_identity.batch_pool.client_id
        }
      }
    },
    {
      name = "hypothesis_timeout_wicket"
      type = "Custom"
      policy = {
        timeout = "0.01:00:00"
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
        command = "python3 hypothesis_timeout_wicket.py"
        resourceLinkedService = {
          referenceName = azurerm_data_factory_linked_service_azure_blob_storage.scripts.name
          type          = "LinkedServiceReference"
        }
        folderPath         = "batch-scripts"
        retentionTimeInDays = 1
        extendedProperties = {
          KEY_VAULT_URI              = local.kv_uri
          MANAGED_IDENTITY_CLIENT_ID = data.azurerm_user_assigned_identity.batch_pool.client_id
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

resource "azurerm_data_factory_pipeline" "backfill" {
  name            = "pl_backfill"
  data_factory_id = data.azurerm_data_factory.main.id
  description     = "Manual backfill: silver then gold. Pass event_id for one match, or leave empty for full backfill."

  parameters = {
    event_id = ""
  }

  activities_json = jsonencode([
    {
      name = "bronze_to_silver"
      type = "Custom"
      policy = {
        timeout = "0.04:00:00"
      }
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
        folderPath         = "batch-scripts"
        retentionTimeInDays = 1
        extendedProperties = {
          KEY_VAULT_URI              = local.kv_uri
          MANAGED_IDENTITY_CLIENT_ID = data.azurerm_user_assigned_identity.batch_pool.client_id
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
        folderPath         = "batch-scripts"
        retentionTimeInDays = 1
        extendedProperties = {
          KEY_VAULT_URI              = local.kv_uri
          MANAGED_IDENTITY_CLIENT_ID = data.azurerm_user_assigned_identity.batch_pool.client_id
          EVENT_ID                   = "@pipeline().parameters.event_id"
        }
      }
    }
  ])

  depends_on = [azapi_resource.adf_ls_azure_batch]
}

