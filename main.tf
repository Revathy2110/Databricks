resource "azurerm_resource_group" "rg" {
  name     = "Terraform_rg"
  location = "South India"

}


resource "azurerm_databricks_workspace" "db" {
  location            = azurerm_resource_group.rg.location
  name                = "Terraform_db"
  resource_group_name = azurerm_resource_group.rg.name
  sku                 = "standard"

}


data "databricks_node_type" "smallest" {
  depends_on = [azurerm_databricks_workspace.db]
  local_disk = true
}

data "databricks_spark_version" "latest_lts" {
  depends_on        = [azurerm_databricks_workspace.db]
  long_term_support = true
}

resource "databricks_instance_pool" "pool" {
  instance_pool_name = "Terraform_pool"
  min_idle_instances = 0
  max_capacity       = 10
  node_type_id       = data.databricks_node_type.smallest.id

  idle_instance_autotermination_minutes = 10
}



resource "databricks_cluster" "shared_autoscaling" {
  depends_on              = [azurerm_databricks_workspace.db]
  instance_pool_id        = databricks_instance_pool.pool.id
  cluster_name            = "cluster_db"
  spark_version           = data.databricks_spark_version.latest_lts.id
  autotermination_minutes = 10

  autoscale {
    min_workers = 1
    max_workers = 10
  }

  spark_conf = {
    "spark.databricks.io.cache.enabled" = true
  }
}

