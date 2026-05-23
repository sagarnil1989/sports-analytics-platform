# ---------------------------------------------------------------------------
# ADF — Key Vault linked service
# Allows ADF to read the Databricks PAT from Key Vault at runtime.
# ---------------------------------------------------------------------------

resource "azurerm_data_factory_linked_service_key_vault" "main" {
  name            = "ls_keyvault"
  data_factory_id = data.azurerm_data_factory.main.id
  key_vault_id    = data.azurerm_key_vault.main.id
}

# Grant ADF managed identity Get permission on Key Vault secrets (Access Policy model)
resource "azurerm_key_vault_access_policy" "adf" {
  key_vault_id = data.azurerm_key_vault.main.id
  tenant_id    = local.config.tenant_id
  object_id    = data.azurerm_data_factory.main.identity[0].principal_id

  secret_permissions = ["Get"]
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
# Activity 1: silver_build_ended_match notebook
#   Parses bronze → silver for matches quiet >1 hour.
# Activity 2: gold_build_ended_match notebook (depends on Activity 1 success)
#   Rebuilds innings_1_from_silver.json from silver. No bronze access.
# Activity 3: discover_cricket_ended notebook (depends on Activity 2 success)
#   Scans gold tracker files and writes the ended index to bronze.
#
# Schedule: daily at 02:00 CET (01:00 UTC)
# ---------------------------------------------------------------------------

resource "azurerm_data_factory_pipeline" "build_ended_match" {
  name            = "pl_build_ended_match"
  data_factory_id = data.azurerm_data_factory.main.id
  description     = "Daily: silver parse → gold rebuild → ended index. Each activity depends on the previous succeeding."

  activities_json = jsonencode([
    {
      name = "RunSilverBuildEndedMatch"
      type = "DatabricksNotebook"
      policy = {
        timeout = "0.04:00:00"
      }
      linkedServiceName = {
        referenceName = azurerm_data_factory_linked_service_azure_databricks.main.name
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        notebookPath = "/cricket-pipeline/silver_build_ended_match"
      }
    },
    {
      name = "RunGoldBuildEndedMatch"
      type = "DatabricksNotebook"
      policy = {
        timeout = "0.04:00:00"
      }
      dependsOn = [
        {
          activity             = "RunSilverBuildEndedMatch"
          dependencyConditions = ["Succeeded"]
        }
      ]
      linkedServiceName = {
        referenceName = azurerm_data_factory_linked_service_azure_databricks.main.name
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        notebookPath = "/cricket-pipeline/gold_build_ended_match"
      }
    },
    {
      name = "RunDiscoverCricketEnded"
      type = "DatabricksNotebook"
      policy = {
        timeout = "0.01:00:00"
      }
      dependsOn = [
        {
          activity             = "RunGoldBuildEndedMatch"
          dependencyConditions = ["Succeeded"]
        }
      ]
      linkedServiceName = {
        referenceName = azurerm_data_factory_linked_service_azure_databricks.main.name
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        notebookPath = "/cricket-pipeline/discover_cricket_ended"
      }
    }
  ])

  depends_on = [azurerm_data_factory_linked_service_azure_databricks.main]
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
# Activity 1: silver_backfill notebook
#   Pass event_id to process one quiet match, or leave empty to loop 4 hours.
# Activity 2: gold_backfill notebook (depends on Activity 1 success)
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
      name = "RunSilverBackfill"
      type = "DatabricksNotebook"
      policy = {
        timeout = "0.04:00:00"
      }
      linkedServiceName = {
        referenceName = azurerm_data_factory_linked_service_azure_databricks.main.name
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        notebookPath   = "/cricket-pipeline/silver_backfill"
        baseParameters = {
          event_id = "@pipeline().parameters.event_id"
        }
      }
    },
    {
      name = "RunGoldBackfill"
      type = "DatabricksNotebook"
      policy = {
        timeout = "0.04:00:00"
      }
      dependsOn = [
        {
          activity             = "RunSilverBackfill"
          dependencyConditions = ["Succeeded"]
        }
      ]
      linkedServiceName = {
        referenceName = azurerm_data_factory_linked_service_azure_databricks.main.name
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        notebookPath   = "/cricket-pipeline/gold_backfill"
        baseParameters = {
          event_id = "@pipeline().parameters.event_id"
        }
      }
    }
  ])

  depends_on = [azurerm_data_factory_linked_service_azure_databricks.main]
}

