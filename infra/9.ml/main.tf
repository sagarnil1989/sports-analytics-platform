# ---------------------------------------------------------------------------
# ML notebooks — registered in the existing Databricks workspace
# (workspace itself is managed by infra/7.databricks)
# ---------------------------------------------------------------------------

resource "databricks_notebook" "ml_feature_extraction" {
  source   = "${path.module}/notebooks/ml_feature_extraction.py"
  path     = "/cricket-pipeline/ml/ml_feature_extraction"
  language = "PYTHON"
}

resource "databricks_notebook" "ml_model_training" {
  source   = "${path.module}/notebooks/ml_model_training.py"
  path     = "/cricket-pipeline/ml/ml_model_training"
  language = "PYTHON"
}

resource "databricks_notebook" "ml_win_predictor" {
  source   = "${path.module}/notebooks/ml_win_predictor.py"
  path     = "/cricket-pipeline/ml/ml_win_predictor"
  language = "PYTHON"
}

resource "databricks_notebook" "inn1_score_predictor" {
  source   = "${path.module}/notebooks/inn1_score_predictor.py"
  path     = "/cricket-pipeline/ml/inn1_score_predictor"
  language = "PYTHON"
}

# ---------------------------------------------------------------------------
# ADF — pipeline: ml_retrain (weekly)
#
# Activity 1: ml_feature_extraction
#   Reads all ended-match gold trackers. Filters to T20. Writes Parquet.
# Activity 2: ml_model_training (depends on Activity 1 success)
#   Reads Parquet. Trains XGBoost score + O/U models. Logs to MLflow.
#
# Uses the Databricks linked service already deployed in infra/8.adf-config.
# Schedule: weekly, Sunday 03:00 UTC (quiet period, no live IPL matches)
# ---------------------------------------------------------------------------

resource "azurerm_data_factory_pipeline" "ml_retrain" {
  name            = "pl_ml_retrain"
  data_factory_id = data.azurerm_data_factory.main.id
  description     = "Weekly ML retraining: feature extraction then model training. Each activity depends on the previous succeeding."

  activities_json = jsonencode([
    {
      name = "RunMLFeatureExtraction"
      type = "DatabricksNotebook"
      policy = {
        timeout = "0.01:00:00"
      }
      linkedServiceName = {
        referenceName = "ls_databricks"
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        notebookPath = "/cricket-pipeline/ml/ml_feature_extraction"
      }
    },
    {
      name = "RunMLModelTraining"
      type = "DatabricksNotebook"
      policy = {
        timeout = "0.01:00:00"
      }
      dependsOn = [
        {
          activity             = "RunMLFeatureExtraction"
          dependencyConditions = ["Succeeded"]
        }
      ]
      linkedServiceName = {
        referenceName = "ls_databricks"
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        notebookPath = "/cricket-pipeline/ml/ml_model_training"
      }
    },
    {
      name = "RunMLWinPredictor"
      type = "DatabricksNotebook"
      policy = {
        timeout = "0.01:00:00"
      }
      dependsOn = [
        {
          activity             = "RunMLFeatureExtraction"
          dependencyConditions = ["Succeeded"]
        }
      ]
      linkedServiceName = {
        referenceName = "ls_databricks"
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        notebookPath = "/cricket-pipeline/ml/ml_win_predictor"
      }
    },
    {
      name = "RunInn1ScorePredictor"
      type = "DatabricksNotebook"
      policy = {
        timeout = "0.01:00:00"
      }
      dependsOn = [
        {
          activity             = "RunMLFeatureExtraction"
          dependencyConditions = ["Succeeded"]
        }
      ]
      linkedServiceName = {
        referenceName = "ls_databricks"
        type          = "LinkedServiceReference"
      }
      typeProperties = {
        notebookPath = "/cricket-pipeline/ml/inn1_score_predictor"
      }
    },
  ])
}

resource "azurerm_data_factory_trigger_schedule" "ml_retrain" {
  name            = "trigger_ml_retrain"
  data_factory_id = data.azurerm_data_factory.main.id
  pipeline_name   = azurerm_data_factory_pipeline.ml_retrain.name

  interval  = 1
  frequency = "Week"

  schedule {
    days_of_week = ["Sunday"]
    hours        = [3]    # 03:00 UTC — quiet period, no live IPL matches
    minutes      = [0]
  }

  activated = true
}

# ---------------------------------------------------------------------------
# ADF — pipeline: hypothesis (weekly, same cadence as ml_retrain)
#
# Runs both hypothesis notebooks in parallel after each other.
# Deliberately separate from pl_ml_retrain so ML failures don't block
# hypothesis refresh and vice versa.
#
# Schedule: weekly, Sunday 03:00 UTC
# ---------------------------------------------------------------------------

resource "azurerm_data_factory_pipeline" "hypothesis" {
  name            = "pl_hypothesis"
  data_factory_id = data.azurerm_data_factory.main.id
  description     = "Weekly hypothesis refresh: inn2_over6 and timeout_wicket notebooks run in parallel."

  activities_json = jsonencode([
    {
      name = "RunHypothesisInn2Over6"
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
          referenceName = "ls_batch_scripts_storage"
          type          = "LinkedServiceReference"
        }
        folderPath         = "batch-scripts"
        retentionTimeInDays = 1
        extendedProperties = {
          KEY_VAULT_URI              = "https://${local.config.key_vault_name}.vault.azure.net/"
          MANAGED_IDENTITY_CLIENT_ID = data.azurerm_user_assigned_identity.batch_pool.client_id
        }
      }
    },
    {
      name = "RunHypothesisTimeoutWicket"
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
          referenceName = "ls_batch_scripts_storage"
          type          = "LinkedServiceReference"
        }
        folderPath         = "batch-scripts"
        retentionTimeInDays = 1
        extendedProperties = {
          KEY_VAULT_URI              = "https://${local.config.key_vault_name}.vault.azure.net/"
          MANAGED_IDENTITY_CLIENT_ID = data.azurerm_user_assigned_identity.batch_pool.client_id
        }
      }
    }
  ])
}

resource "azurerm_data_factory_trigger_schedule" "hypothesis" {
  name            = "trigger_hypothesis"
  data_factory_id = data.azurerm_data_factory.main.id
  pipeline_name   = azurerm_data_factory_pipeline.hypothesis.name

  interval  = 1
  frequency = "Week"

  schedule {
    days_of_week = ["Sunday"]
    hours        = [3]
    minutes      = [0]
  }

  activated = true
}
