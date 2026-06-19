resource "azurerm_resource_provider_registration" "batch" {
  name = "Microsoft.Batch"
}

# ---------------------------------------------------------------------------
# User-assigned managed identity for the Batch pool
#
# Azure Batch pools only support user-assigned managed identity (not
# system-assigned). This identity is granted access to Storage and Key Vault
# so that tasks can authenticate without storing credentials.
# ---------------------------------------------------------------------------

resource "azurerm_user_assigned_identity" "batch_pool" {
  name                = "id-batch-${local.config.project}"
  resource_group_name = data.azurerm_resource_group.main.name
  location            = data.azurerm_resource_group.main.location

  tags = local.config.tags
}

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

  depends_on = [azurerm_resource_provider_registration.batch]
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
  vm_size             = "Standard_A2_v2"
  node_agent_sku_id   = "batch.node.ubuntu 22.04"

  storage_image_reference {
    publisher = "canonical"
    offer     = "0001-com-ubuntu-server-jammy"
    sku       = "22_04-lts"
    version   = "latest"
  }

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.batch_pool.id]
  }

  # Scale to 0 at idle; scale up to 1 when there are pending tasks.
  auto_scale {
    evaluation_interval = "PT5M"
    formula             = <<-EOT
      $tasks = max($PendingTasks.GetSample(TimeInterval_Minute * 5));
      $TargetDedicatedNodes = ($tasks > 0) ? 1 : 0;
      $NodeDeallocationOption = taskcompletion;
    EOT
  }

  start_task {
    command_line     = "/bin/bash -c 'apt-get update -qq && apt-get install -y -qq python3-pip && pip3 install azure-storage-blob azure-identity azure-keyvault-secrets requests'"
    wait_for_success = true

    user_identity {
      auto_user {
        scope           = "Pool"
        elevation_level = "Admin"
      }
    }
  }
}

# ---------------------------------------------------------------------------
# Role assignments — Batch pool managed identity
#
# Tasks within the pool authenticate to Storage and Key Vault using
# the pool's user-assigned managed identity.
# ---------------------------------------------------------------------------

resource "azurerm_role_assignment" "batch_pool_storage" {
  scope                = data.azurerm_storage_account.data_lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_user_assigned_identity.batch_pool.principal_id
}

resource "azurerm_role_assignment" "batch_pool_kv" {
  scope                = data.azurerm_key_vault.main.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_user_assigned_identity.batch_pool.principal_id
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
  name                 = "bronze_to_silver.py"
  storage_container_id = azurerm_storage_container.scripts.id
  type                 = "Block"
  source               = "${path.module}/scripts/bronze_to_silver.py"
  content_md5          = filemd5("${path.module}/scripts/bronze_to_silver.py")
}

resource "azurerm_storage_blob" "script_silver_to_gold" {
  name                 = "silver_to_gold.py"
  storage_container_id = azurerm_storage_container.scripts.id
  type                 = "Block"
  source               = "${path.module}/scripts/silver_to_gold.py"
  content_md5          = filemd5("${path.module}/scripts/silver_to_gold.py")
}

resource "azurerm_storage_blob" "script_discover_cricket_ended" {
  name                 = "discover_cricket_ended.py"
  storage_container_id = azurerm_storage_container.scripts.id
  type                 = "Block"
  source               = "${path.module}/scripts/discover_cricket_ended.py"
  content_md5          = filemd5("${path.module}/scripts/discover_cricket_ended.py")
}

resource "azurerm_storage_blob" "script_hypothesis_inn2_over6" {
  name                 = "hypothesis_inn2_over6.py"
  storage_container_id = azurerm_storage_container.scripts.id
  type                 = "Block"
  source               = "${path.module}/scripts/hypothesis_inn2_over6.py"
  content_md5          = filemd5("${path.module}/scripts/hypothesis_inn2_over6.py")
}

resource "azurerm_storage_blob" "script_hypothesis_timeout_wicket" {
  name                 = "hypothesis_timeout_wicket.py"
  storage_container_id = azurerm_storage_container.scripts.id
  type                 = "Block"
  source               = "${path.module}/scripts/hypothesis_timeout_wicket.py"
  content_md5          = filemd5("${path.module}/scripts/hypothesis_timeout_wicket.py")
}

# ---------------------------------------------------------------------------
# Lib files shared by scripts
# util.py and league_config.py come from the ingestion function source.
# The rest come from infra/7.databricks/lib/.
# ---------------------------------------------------------------------------

resource "azurerm_storage_blob" "lib_util" {
  name                 = "util.py"
  storage_container_id = azurerm_storage_container.scripts.id
  type                 = "Block"
  source               = "${local.src_path}/util.py"
  content_md5          = filemd5("${local.src_path}/util.py")
}

resource "azurerm_storage_blob" "lib_league_config" {
  name                 = "league_config.py"
  storage_container_id = azurerm_storage_container.scripts.id
  type                 = "Block"
  source               = "${local.src_path}/league_config.py"
  content_md5          = filemd5("${local.src_path}/league_config.py")
}

resource "azurerm_storage_blob" "lib_snapshot_parser" {
  name                 = "snapshot_parser.py"
  storage_container_id = azurerm_storage_container.scripts.id
  type                 = "Block"
  source               = "${local.lib_path}/snapshot_parser.py"
  content_md5          = filemd5("${local.lib_path}/snapshot_parser.py")
}

resource "azurerm_storage_blob" "lib_gold_rebuild" {
  name                 = "gold_rebuild.py"
  storage_container_id = azurerm_storage_container.scripts.id
  type                 = "Block"
  source               = "${local.lib_path}/gold_rebuild.py"
  content_md5          = filemd5("${local.lib_path}/gold_rebuild.py")
}

resource "azurerm_storage_blob" "lib_hypothesis" {
  name                 = "hypothesis.py"
  storage_container_id = azurerm_storage_container.scripts.id
  type                 = "Block"
  source               = "${local.lib_path}/hypothesis.py"
  content_md5          = filemd5("${local.lib_path}/hypothesis.py")
}

resource "azurerm_storage_blob" "lib_tracker_writer" {
  name                 = "tracker_writer.py"
  storage_container_id = azurerm_storage_container.scripts.id
  type                 = "Block"
  source               = "${local.lib_path}/tracker_writer.py"
  content_md5          = filemd5("${local.lib_path}/tracker_writer.py")
}

resource "azurerm_storage_blob" "lib_landing_index" {
  name                 = "landing_index.py"
  storage_container_id = azurerm_storage_container.scripts.id
  type                 = "Block"
  source               = "${local.lib_path}/landing_index.py"
  content_md5          = filemd5("${local.lib_path}/landing_index.py")
}

resource "azurerm_storage_blob" "script_index_new_snapshots" {
  name                 = "index_new_snapshots.py"
  storage_container_id = azurerm_storage_container.scripts.id
  type                 = "Block"
  source               = "${path.module}/scripts/index_new_snapshots.py"
  content_md5          = filemd5("${path.module}/scripts/index_new_snapshots.py")
}

resource "azurerm_storage_blob" "script_update_watermark" {
  name                 = "update_watermark.py"
  storage_container_id = azurerm_storage_container.scripts.id
  type                 = "Block"
  source               = "${path.module}/scripts/update_watermark.py"
  content_md5          = filemd5("${path.module}/scripts/update_watermark.py")
}

resource "azurerm_storage_blob" "script_cleanup_landing" {
  name                 = "cleanup_landing.py"
  storage_container_id = azurerm_storage_container.scripts.id
  type                 = "Block"
  source               = "${path.module}/scripts/cleanup_landing.py"
  content_md5          = filemd5("${path.module}/scripts/cleanup_landing.py")
}
