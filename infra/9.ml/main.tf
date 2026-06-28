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

# NOTE: pl_ml_retrain and pl_hypothesis (and pl_over_under_retrain, defined
# in infra/8.adf-config) were merged 2026-06-28 into a single pipeline,
# pl_ml_and_hypothesis, all on the same Saturday 03:00 UTC schedule. See
# azurerm_data_factory_pipeline.ml_and_hypothesis in infra/8.adf-config/main.tf.
# A duplicate pl_hypothesis pipeline + trigger used to live here too — it
# clobbered the real one on every deploy because infra_9_ml runs after
# infra_8_adf_config; removed at the same time.
