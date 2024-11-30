terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.59.0"
    }
  }
}


provider "databricks" {
  azure_workspace_resource_id = azurerm_databricks_workspace.db.id
  azure_client_id             = "2390d7fb-4694-46e5-93da-d9bce26b1f4d"
  azure_tenant_id             = "d13446e9-13fa-4948-8888-4481af7f1d21"
  azure_client_secret         = "3oc8Q~wWKa6I1hZzON7d4BCZT7DGJyMQFpbcybml"
}

provider "azurerm" {
  subscription_id = "b2b76ff2-a2f9-4317-b197-2e023c60b829"
  client_id       = "2390d7fb-4694-46e5-93da-d9bce26b1f4d"
  tenant_id       = "d13446e9-13fa-4948-8888-4481af7f1d21"
  client_secret   = "3oc8Q~wWKa6I1hZzON7d4BCZT7DGJyMQFpbcybml"
  features {}
}

