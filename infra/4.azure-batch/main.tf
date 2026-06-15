# ---------------------------------------------------------------------------
# Azure Batch account
# No auto-storage — ADF Custom activity handles script distribution via
# the ls_batch_scripts_storage linked service.
# ---------------------------------------------------------------------------

resource "azurerm_batch_account" "main" {
  name                 = local.batch_account
  resource_group_name  = data.azurerm_resource_group.main.name
  location             = data.azurerm_resource_group.main.location
  pool_allocation_mode = "BatchService"

  tags = local.config.tags
}

# ---------------------------------------------------------------------------
# Batch pool — cricket-pipeline
#
# Standard_B2s (2 vCPU, 4 GB RAM), Ubuntu 22.04 LTS.
# Auto-scales: 0 nodes at idle → 1 node when tasks are pending.
# Start task installs Python packages on every fresh node.
# ---------------------------------------------------------------------------

resource "azurerm_batch_pool" "main" {
  name                = local.pool_name
  resource_group_name = data.azurerm_resource_group.main.name
  account_name        = azurerm_batch_account.main.name
  display_name        = "Cricket Pipeline Pool"
  vm_size             = "Standard_B2s"
  node_agent_sku_id   = "batch.node.ubuntu 22.04"

  storage_image_reference {
    publisher = "canonical"
    offer     = "0001-com-ubuntu-server-jammy"
    sku       = "22_04-lts"
    version   = "latest"
  }

  identity {
    type = "SystemAssigned"
  }

  # Scale to 0 at idle; scale up to 1 when there are pending tasks.
  auto_scale {
    evaluation_interval = "PT5M"
    formula             = <<-EOT
      startingNumberOfVMs = 0;
      maxNumberofVMs = 1;
      pendingTaskSamplePercent = $PendingTasks.GetSamplePercent(180 * TimeInterval_Second);
      pendingTaskSamples = pendingTaskSamplePercent < 70 ? startingNumberOfVMs : avg($PendingTasks.GetSample(180 * TimeInterval_Second));
      $TargetDedicatedNodes = min(maxNumberofVMs, pendingTaskSamples);
      $NodeDeallocationOption = taskcompletion;
    EOT
  }

  start_task {
    command_line     = "bash -c 'pip3 install azure-storage-blob azure-identity azure-keyvault-secrets requests'"
    wait_for_success = true

    user_identity {
      auto_user {
        scope           = "pool"
        elevation_level = "admin"
      }
    }
  }
}

# ---------------------------------------------------------------------------
# Role assignments — Batch pool managed identity
#
# Tasks within the pool authenticate to Storage and Key Vault using
# the pool's system-assigned managed identity.
# ---------------------------------------------------------------------------

resource "azurerm_role_assignment" "batch_pool_storage" {
  scope                = data.azurerm_storage_account.data_lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_batch_pool.main.identity[0].principal_id
}

resource "azurerm_role_assignment" "batch_pool_kv" {
  scope                = data.azurerm_key_vault.main.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_batch_pool.main.identity[0].principal_id
}

# ---------------------------------------------------------------------------
# Key Vault secrets consumed by Batch task scripts
# ---------------------------------------------------------------------------

resource "azurerm_key_vault_secret" "storage_conn_str" {
  name         = "DATA-STORAGE-CONNECTION-STRING"
  value        = data.azurerm_storage_account.data_lake.primary_connection_string
  key_vault_id = data.azurerm_key_vault.main.id
}

resource "azurerm_key_vault_secret" "sport_id" {
  name         = "SPORT-ID"
  value        = tostring(local.config.sport_id)
  key_vault_id = data.azurerm_key_vault.main.id
}

# ---------------------------------------------------------------------------
# Storage container — batch-scripts
#
# ADF Custom activity downloads all blobs from this container into
# the task working directory before executing the command.
# ---------------------------------------------------------------------------

resource "azurerm_storage_container" "scripts" {
  name               = local.scripts_container
  storage_account_id = data.azurerm_storage_account.data_lake.id
}

# ---------------------------------------------------------------------------
# Pipeline scripts
# ---------------------------------------------------------------------------

resource "azurerm_storage_blob" "script_bronze_to_silver" {
  name                   = "bronze_to_silver.py"
  storage_account_name   = data.azurerm_storage_account.data_lake.name
  storage_container_name = azurerm_storage_container.scripts.name
  type                   = "Block"
  source                 = "${path.module}/scripts/bronze_to_silver.py"
  content_md5            = filemd5("${path.module}/scripts/bronze_to_silver.py")
}

resource "azurerm_storage_blob" "script_silver_to_gold" {
  name                   = "silver_to_gold.py"
  storage_account_name   = data.azurerm_storage_account.data_lake.name
  storage_container_name = azurerm_storage_container.scripts.name
  type                   = "Block"
  source                 = "${path.module}/scripts/silver_to_gold.py"
  content_md5            = filemd5("${path.module}/scripts/silver_to_gold.py")
}

resource "azurerm_storage_blob" "script_discover_cricket_ended" {
  name                   = "discover_cricket_ended.py"
  storage_account_name   = data.azurerm_storage_account.data_lake.name
  storage_container_name = azurerm_storage_container.scripts.name
  type                   = "Block"
  source                 = "${path.module}/scripts/discover_cricket_ended.py"
  content_md5            = filemd5("${path.module}/scripts/discover_cricket_ended.py")
}

resource "azurerm_storage_blob" "script_hypothesis_inn2_over6" {
  name                   = "hypothesis_inn2_over6.py"
  storage_account_name   = data.azurerm_storage_account.data_lake.name
  storage_container_name = azurerm_storage_container.scripts.name
  type                   = "Block"
  source                 = "${path.module}/scripts/hypothesis_inn2_over6.py"
  content_md5            = filemd5("${path.module}/scripts/hypothesis_inn2_over6.py")
}

resource "azurerm_storage_blob" "script_hypothesis_timeout_wicket" {
  name                   = "hypothesis_timeout_wicket.py"
  storage_account_name   = data.azurerm_storage_account.data_lake.name
  storage_container_name = azurerm_storage_container.scripts.name
  type                   = "Block"
  source                 = "${path.module}/scripts/hypothesis_timeout_wicket.py"
  content_md5            = filemd5("${path.module}/scripts/hypothesis_timeout_wicket.py")
}

# ---------------------------------------------------------------------------
# Lib files shared by scripts
# util.py and league_config.py come from the ingestion function source.
# The rest come from infra/7.databricks/lib/.
# ---------------------------------------------------------------------------

resource "azurerm_storage_blob" "lib_util" {
  name                   = "util.py"
  storage_account_name   = data.azurerm_storage_account.data_lake.name
  storage_container_name = azurerm_storage_container.scripts.name
  type                   = "Block"
  source                 = "${local.src_path}/util.py"
  content_md5            = filemd5("${local.src_path}/util.py")
}

resource "azurerm_storage_blob" "lib_league_config" {
  name                   = "league_config.py"
  storage_account_name   = data.azurerm_storage_account.data_lake.name
  storage_container_name = azurerm_storage_container.scripts.name
  type                   = "Block"
  source                 = "${local.src_path}/league_config.py"
  content_md5            = filemd5("${local.src_path}/league_config.py")
}

resource "azurerm_storage_blob" "lib_snapshot_parser" {
  name                   = "snapshot_parser.py"
  storage_account_name   = data.azurerm_storage_account.data_lake.name
  storage_container_name = azurerm_storage_container.scripts.name
  type                   = "Block"
  source                 = "${local.lib_path}/snapshot_parser.py"
  content_md5            = filemd5("${local.lib_path}/snapshot_parser.py")
}

resource "azurerm_storage_blob" "lib_gold_rebuild" {
  name                   = "gold_rebuild.py"
  storage_account_name   = data.azurerm_storage_account.data_lake.name
  storage_container_name = azurerm_storage_container.scripts.name
  type                   = "Block"
  source                 = "${local.lib_path}/gold_rebuild.py"
  content_md5            = filemd5("${local.lib_path}/gold_rebuild.py")
}

resource "azurerm_storage_blob" "lib_hypothesis" {
  name                   = "hypothesis.py"
  storage_account_name   = data.azurerm_storage_account.data_lake.name
  storage_container_name = azurerm_storage_container.scripts.name
  type                   = "Block"
  source                 = "${local.lib_path}/hypothesis.py"
  content_md5            = filemd5("${local.lib_path}/hypothesis.py")
}

resource "azurerm_storage_blob" "lib_tracker_writer" {
  name                   = "tracker_writer.py"
  storage_account_name   = data.azurerm_storage_account.data_lake.name
  storage_container_name = azurerm_storage_container.scripts.name
  type                   = "Block"
  source                 = "${local.lib_path}/tracker_writer.py"
  content_md5            = filemd5("${local.lib_path}/tracker_writer.py")
}
