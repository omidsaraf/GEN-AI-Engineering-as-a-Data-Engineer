# Resource Group
resource "azurerm_resource_group" "rg" {
  name     = var.rg_name
  location = var.location
}

# Virtual Network
module "vnet" {
  source = "./modules/vnet"
  rg_name = azurerm_resource_group.rg.name
  location = var.location
}

# Azure Databricks Workspace
module "databricks" {
  source  = "./modules/databricks"
  rg_name = azurerm_resource_group.rg.name
  location = var.location
}

# Azure Key Vault
resource "azurerm_key_vault" "kv" {
  name                = var.kv_name
  location            = var.location
  resource_group_name = azurerm_resource_group.rg.name
  sku_name            = "standard"
  tenant_id           = var.tenant_id
  soft_delete_enabled = true
}
