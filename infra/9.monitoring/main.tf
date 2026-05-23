# ---------------------------------------------------------------------------
# Action group — where alerts are sent
# ---------------------------------------------------------------------------

resource "azurerm_monitor_action_group" "main" {
  name                = "ag-${local.config.project}"
  resource_group_name = data.azurerm_resource_group.main.name
  short_name          = "ramanuj"

  email_receiver {
    name          = "owner"
    email_address = local.config.alert_email
  }
}

# ---------------------------------------------------------------------------
# vCPU quota alert — Standard DSv1 family (used by Databricks job clusters)
#
# Scope is the subscription + region, not a resource group.
# Fires when usage >= vcpu_alert_threshold (default 10).
#
# To find the exact metric name for your SKU family:
#   Azure Portal → Monitor → Metrics → scope = subscription →
#   namespace = "Microsoft.Compute/locations/usages" → browse metric names.
#
# Common names:
#   "Standard DSv1 Family vCPUs"   — Standard_DS1_v2, Standard_DS2_v2
#   "Standard DSv2 Family vCPUs"   — Standard_DS3_v2, Standard_DS4_v2
#   "Total Regional vCPUs"          — all families combined
# ---------------------------------------------------------------------------

resource "azurerm_monitor_metric_alert" "vcpu_quota_dsv1" {
  name                = "alert-vcpu-quota-dsv1-${local.config.project}"
  resource_group_name = data.azurerm_resource_group.main.name
  description         = "Fires when Standard DSv1 Family vCPU usage reaches ${local.config.vcpu_alert_threshold} cores — approaching the subscription quota limit."

  scopes = [
    "/subscriptions/${local.config.subscription_id}/providers/Microsoft.Compute/locations/${data.azurerm_resource_group.main.location}"
  ]

  criteria {
    metric_namespace = "Microsoft.Compute/locations/usages"
    metric_name      = "Standard DSv1 Family vCPUs"
    aggregation      = "Average"
    operator         = "GreaterThanOrEqual"
    threshold        = local.config.vcpu_alert_threshold
  }

  window_size        = "PT5M"
  frequency          = "PT1M"
  severity           = 2   # Warning

  action {
    action_group_id = azurerm_monitor_action_group.main.id
  }
}

resource "azurerm_monitor_metric_alert" "vcpu_quota_total" {
  name                = "alert-vcpu-quota-total-${local.config.project}"
  resource_group_name = data.azurerm_resource_group.main.name
  description         = "Fires when total regional vCPU usage reaches ${local.config.vcpu_alert_threshold} cores — approaching the subscription quota limit."

  scopes = [
    "/subscriptions/${local.config.subscription_id}/providers/Microsoft.Compute/locations/${data.azurerm_resource_group.main.location}"
  ]

  criteria {
    metric_namespace = "Microsoft.Compute/locations/usages"
    metric_name      = "Total Regional vCPUs"
    aggregation      = "Average"
    operator         = "GreaterThanOrEqual"
    threshold        = local.config.vcpu_alert_threshold
  }

  window_size        = "PT5M"
  frequency          = "PT1M"
  severity           = 2   # Warning

  action {
    action_group_id = azurerm_monitor_action_group.main.id
  }
}

# ---------------------------------------------------------------------------
# ADF pipeline failure alert
# Fires when any ADF pipeline run fails.
# ---------------------------------------------------------------------------

data "azurerm_data_factory" "main" {
  name                = "adf-${local.config.project}"
  resource_group_name = data.azurerm_resource_group.main.name
}

resource "azurerm_monitor_metric_alert" "adf_pipeline_failed" {
  name                = "alert-adf-pipeline-failed-${local.config.project}"
  resource_group_name = data.azurerm_resource_group.main.name
  description         = "Fires when an ADF pipeline run fails."

  scopes   = [data.azurerm_data_factory.main.id]
  severity = 1   # Error

  criteria {
    metric_namespace = "Microsoft.DataFactory/factories"
    metric_name      = "PipelineFailedRuns"
    aggregation      = "Total"
    operator         = "GreaterThan"
    threshold        = 0
  }

  window_size = "PT5M"
  frequency   = "PT1M"

  action {
    action_group_id = azurerm_monitor_action_group.main.id
  }
}
