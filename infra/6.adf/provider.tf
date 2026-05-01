terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }

  backend "azurerm" {
    resource_group_name  = "rg-tf-state"
    storage_account_name = "tfstateramanuj"
    container_name       = "tfstate"
    key                  = "6.adf.tfstate"
  }
}

provider "azurerm" {
  features {}
  tenant_id       = local.config.tenant_id
  subscription_id = local.config.subscription_id
}
