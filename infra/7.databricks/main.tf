# ---------------------------------------------------------------------------
# Databricks workspace
# ---------------------------------------------------------------------------

resource "azurerm_databricks_workspace" "main" {
  name                = "dbw-${local.config.project}"
  resource_group_name = data.azurerm_resource_group.main.name
  location            = data.azurerm_resource_group.main.location
  sku                 = "premium"

  tags = local.config.tags
}

# ---------------------------------------------------------------------------
# Databricks secret scope + secrets
#
# Databricks-native scope (encrypted in Databricks control plane).
# Notebooks access secrets via: dbutils.secrets.get("cricket-pipeline", "<key>")
# ---------------------------------------------------------------------------

resource "databricks_secret_scope" "cricket" {
  name = "cricket-pipeline"
}

resource "databricks_secret" "storage_connection_string" {
  scope        = databricks_secret_scope.cricket.name
  key          = "DATA_STORAGE_CONNECTION_STRING"
  string_value = data.azurerm_storage_account.data_lake.primary_connection_string
}

resource "databricks_secret" "sport_id" {
  scope        = databricks_secret_scope.cricket.name
  key          = "SPORT_ID"
  string_value = tostring(local.config.sport_id)
}

# ---------------------------------------------------------------------------
# DBFS — upload pipeline source files
#
# Notebooks import these files directly from /dbfs/FileStore/cricket-pipeline/src/
# Terraform re-uploads whenever the local file changes (hash-based detection).
# ---------------------------------------------------------------------------

resource "databricks_dbfs_file" "storage_py" {
  source = "${local.src_path}/storage.py"
  path   = "${local.dbfs_src_path}/storage.py"
}

resource "databricks_dbfs_file" "silver_py" {
  source = "${local.src_path}/silver.py"
  path   = "${local.dbfs_src_path}/silver.py"
}

resource "databricks_dbfs_file" "gold_py" {
  source = "${local.src_path}/gold.py"
  path   = "${local.dbfs_src_path}/gold.py"
}

resource "databricks_dbfs_file" "innings_tracker_py" {
  source = "${local.src_path}/innings_tracker.py"
  path   = "${local.dbfs_src_path}/innings_tracker.py"
}

resource "databricks_dbfs_file" "leagues_py" {
  source = "${local.src_path}/leagues.py"
  path   = "${local.dbfs_src_path}/leagues.py"
}

resource "databricks_dbfs_file" "views_py" {
  source = "${local.src_path}/views.py"
  path   = "${local.dbfs_src_path}/views.py"
}

# ---------------------------------------------------------------------------
# Databricks notebooks
# ---------------------------------------------------------------------------

resource "databricks_notebook" "silver_build_ended_match" {
  source   = "${path.module}/notebooks/silver_build_ended_match.py"
  path     = "/cricket-pipeline/silver_build_ended_match"
  language = "PYTHON"
}

resource "databricks_notebook" "gold_build_ended_match" {
  source   = "${path.module}/notebooks/gold_build_ended_match.py"
  path     = "/cricket-pipeline/gold_build_ended_match"
  language = "PYTHON"
}

resource "databricks_notebook" "gold_backfill" {
  source   = "${path.module}/notebooks/gold_backfill.py"
  path     = "/cricket-pipeline/gold_backfill"
  language = "PYTHON"
}

resource "databricks_notebook" "silver_backfill" {
  source   = "${path.module}/notebooks/silver_backfill.py"
  path     = "/cricket-pipeline/silver_backfill"
  language = "PYTHON"
}

resource "databricks_notebook" "discover_cricket_ended" {
  source   = "${path.module}/notebooks/discover_cricket_ended.py"
  path     = "/cricket-pipeline/discover_cricket_ended"
  language = "PYTHON"
}

resource "databricks_notebook" "analysis_bronze_silver_counts" {
  source   = "${path.module}/notebooks/analysis_bronze_silver_counts.py"
  path     = "/cricket-pipeline/analysis/bronze_silver_counts"
  language = "PYTHON"
}
