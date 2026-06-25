# ---------------------------------------------------------------------------
# Databricks workspace
# ---------------------------------------------------------------------------

resource "azurerm_databricks_workspace" "main" {
  name                = "dbw-${local.config.project}"
  resource_group_name = data.azurerm_resource_group.main.name
  location            = data.azurerm_resource_group.main.location
  sku                 = "premium"

  custom_parameters {
    # no_public_ip = false → cluster nodes keep their own public IPs for outbound
    # traffic, so no NAT gateway is required. Azure will NOT provision one.
    no_public_ip = false

    virtual_network_id = azurerm_virtual_network.databricks.id

    public_subnet_name                                  = azurerm_subnet.databricks_public.name
    public_subnet_network_security_group_association_id = azurerm_subnet_network_security_group_association.databricks_public.id

    private_subnet_name                                  = azurerm_subnet.databricks_private.name
    private_subnet_network_security_group_association_id = azurerm_subnet_network_security_group_association.databricks_private.id
  }

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

resource "databricks_secret" "bets_api_token" {
  scope        = databricks_secret_scope.cricket.name
  key          = "BETS_API_TOKEN"
  string_value = data.azurerm_key_vault_secret.bet365_api_token.value
}

# ---------------------------------------------------------------------------
# DBFS — upload pipeline source files
#
# Notebooks import these files directly from /dbfs/FileStore/cricket-pipeline/src/
# Terraform re-uploads whenever the local file changes (hash-based detection).
# ---------------------------------------------------------------------------

resource "databricks_dbfs_file" "util_py" {
  source = "${local.src_path}/util.py"
  path   = "${local.dbfs_src_path}/util.py"
}

resource "databricks_dbfs_file" "snapshot_parser_py" {
  source = "${path.module}/lib/snapshot_parser.py"
  path   = "${local.dbfs_src_path}/snapshot_parser.py"
}

resource "databricks_dbfs_file" "match_page_builder_py" {
  source = "${path.module}/lib/match_page_builder.py"
  path   = "${local.dbfs_src_path}/match_page_builder.py"
}

resource "databricks_dbfs_file" "tracker_writer_py" {
  source = "${path.module}/lib/tracker_writer.py"
  path   = "${local.dbfs_src_path}/tracker_writer.py"
}

resource "databricks_dbfs_file" "league_config_py" {
  source = "${local.src_path}/league_config.py"
  path   = "${local.dbfs_src_path}/league_config.py"
}

resource "databricks_dbfs_file" "gold_rebuild_py" {
  source = "${path.module}/lib/gold_rebuild.py"
  path   = "${local.dbfs_src_path}/gold_rebuild.py"
}

resource "databricks_dbfs_file" "prematch_page_builder_py" {
  source = "${path.module}/lib/prematch_page_builder.py"
  path   = "${local.dbfs_src_path}/prematch_page_builder.py"
}


# ---------------------------------------------------------------------------
# Databricks notebooks
# ---------------------------------------------------------------------------

resource "databricks_notebook" "bronze_to_silver" {
  source   = "${path.module}/notebooks/bronze_to_silver.py"
  path     = "/cricket-pipeline/bronze_to_silver"
  language = "PYTHON"
}

resource "databricks_notebook" "silver_to_gold" {
  source   = "${path.module}/notebooks/silver_to_gold.py"
  path     = "/cricket-pipeline/silver_to_gold"
  language = "PYTHON"
}

resource "databricks_notebook" "gold_build_prematch_pages" {
  source   = "${path.module}/notebooks/gold_build_prematch_pages.py"
  path     = "/cricket-pipeline/gold_build_prematch_pages"
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

resource "databricks_notebook" "analysis_snapshot_timeline" {
  source   = "${path.module}/notebooks/analysis_snapshot_timeline.py"
  path     = "/cricket-pipeline/analysis/snapshot_timeline"
  language = "PYTHON"
}

resource "databricks_notebook" "bronze_dedup_cleanup" {
  source   = "${path.module}/notebooks/bronze_dedup_cleanup.py"
  path     = "/cricket-pipeline/ops/bronze_dedup_cleanup"
  language = "PYTHON"
}

resource "databricks_notebook" "analysis_match_data_explorer" {
  source   = "${path.module}/notebooks/analysis_match_data_explorer.py"
  path     = "/cricket-pipeline/analysis/match_data_explorer"
  language = "PYTHON"
}

resource "databricks_dbfs_file" "hypothesis_py" {
  source = "${path.module}/lib/hypothesis.py"
  path   = "${local.dbfs_src_path}/hypothesis.py"
}

resource "databricks_dbfs_file" "landing_index_py" {
  source = "${path.module}/lib/landing_index.py"
  path   = "${local.dbfs_src_path}/landing_index.py"
}

resource "databricks_dbfs_file" "process_queue_py" {
  source = "${path.module}/lib/process_queue.py"
  path   = "${local.dbfs_src_path}/process_queue.py"
}

resource "databricks_dbfs_file" "over_under_predictor_py" {
  source = "${path.module}/lib/over_under_predictor.py"
  path   = "${local.dbfs_src_path}/over_under_predictor.py"
}

resource "databricks_notebook" "hypothesis_inn2_over6" {
  source   = "${path.module}/notebooks/hypothesis_inn2_over6.py"
  path     = "/cricket-pipeline/hypothesis/inn2_over6"
  language = "PYTHON"
}

resource "databricks_notebook" "hypothesis_timeout_wicket" {
  source   = "${path.module}/notebooks/hypothesis_timeout_wicket.py"
  path     = "/cricket-pipeline/hypothesis/timeout_wicket"
  language = "PYTHON"
}

resource "databricks_notebook" "hypothesis_inn1_prematch" {
  source   = "${path.module}/notebooks/hypothesis_inn1_prematch.py"
  path     = "/cricket-pipeline/hypothesis/inn1_prematch"
  language = "PYTHON"
}

resource "databricks_notebook" "analysis_ended_match_table" {
  source   = "${path.module}/notebooks/analysis_ended_match_table.py"
  path     = "/cricket-pipeline/analysis/ended_match_table"
  language = "PYTHON"
}

resource "databricks_notebook" "read_pending_queue" {
  source   = "${path.module}/notebooks/read_pending_queue.py"
  path     = "/cricket-pipeline/read_pending_queue"
  language = "PYTHON"
}

resource "databricks_notebook" "delete_processed_markers" {
  source   = "${path.module}/notebooks/delete_processed_markers.py"
  path     = "/cricket-pipeline/delete_processed_markers"
  language = "PYTHON"
}

resource "databricks_notebook" "ml_extract_over_under_features" {
  source   = "${path.module}/notebooks/ml_extract_over_under_features.py"
  path     = "/cricket-pipeline/ml/extract_over_under_features"
  language = "PYTHON"
}

resource "databricks_notebook" "ml_train_over_under" {
  source   = "${path.module}/notebooks/ml_train_over_under.py"
  path     = "/cricket-pipeline/ml/train_over_under"
  language = "PYTHON"
}

