terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.70"
    }
  }

  backend "azurerm" {
    resource_group_name  = "rg-tf-state"
    storage_account_name = "tfstateramanuj"
    container_name       = "tfstate"
    key                  = "9.ml.tfstate"
  }
}

provider "azurerm" {
  features {}
  tenant_id       = local.config.tenant_id
  subscription_id = local.config.subscription_id
}

# Databricks provider points at the existing workspace (created in infra/7.databricks)
provider "databricks" {
  host                        = "https://adb-7405619046702431.11.azuredatabricks.net"
  azure_workspace_resource_id = "/subscriptions/${local.config.subscription_id}/resourceGroups/${local.config.resource_group_name}/providers/Microsoft.Databricks/workspaces/dbw-${local.config.project}"
}
