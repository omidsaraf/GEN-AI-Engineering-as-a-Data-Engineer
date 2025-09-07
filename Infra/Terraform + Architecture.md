
##  Infrastructure (Terraform + Architecture)

### Azure Resource Layout

**Key resources & topology**

| Component                                | Purpose                                | Notes / Best Practices                                                                        |
| ---------------------------------------- | -------------------------------------- | --------------------------------------------------------------------------------------------- |
| **Resource Group**                       | Logical container                      | `rg-optus-lakehouse`, location `australiaeast`                                                |
| **ADLS Gen2 Storage**                    | Bronze/Silver/Gold layers              | HNS enabled, LRS replication, hierarchical namespace, mount points in Databricks              |
| **Databricks Workspace**                 | Main compute & Lakehouse orchestration | Workspace deployed in VNET, private link enabled                                              |
| **Key Vault**                            | Secrets, tokens, DBX API keys          | Enable soft delete, purge protection, access policies via Azure AD                            |
| **Databricks Metastore (Unity Catalog)** | Governed catalog                       | Maps storage to tables, supports ABAC/RBAC, lineage tracking                                  |
| **VNET / Subnets**                       | Network isolation                      | Dedicated subnets for Databricks, Key Vault, ADLS; Network Security Groups; Private Endpoints |
| **Azure Private Link**                   | Secure access                          | Databricks workspace → Key Vault, Storage; optional SQL endpoints                             |
| **Monitoring & Logs**                    | Observability                          | Log Analytics workspace, audit logs, diagnostic settings, alerting                            |

### Terraform Examples

**Azure Resource Group + ADLS + Key Vault**

```hcl
provider "azurerm" { features {} }

resource "azurerm_resource_group" "rg" {
  name     = "rg-optus-lakehouse"
  location = "australiaeast"
}

resource "azurerm_storage_account" "adls" {
  name                     = "optuslakehouseadls"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true
}

resource "azurerm_key_vault" "kv" {
  name                = "kv-optus-lakehouse"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  sku_name            = "standard"
}
```

**Databricks Workspace + Cluster + Unity Catalog**

```hcl
provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

resource "databricks_metastore" "uc" {
  name          = "optus-uc"
  storage_root  = "abfss://uc@${var.adls_name}.dfs.core.windows.net/"
  force_destroy = false
}

resource "databricks_metastore_assignment" "ws" {
  workspace_id         = var.workspace_id
  metastore_id         = databricks_metastore.uc.id
  default_catalog_name = "main"
}

resource "databricks_cluster" "etl_small" {
  cluster_name            = "etl-small"
  spark_version           = "14.3.x-scala2.12"
  node_type_id            = "Standard_DS3_v2"
  autotermination_minutes = 30
  num_workers             = 2
}
```

### Networking Considerations

* **VNET injection** for Databricks to enforce private network traffic.
* **NSG rules** for subnet isolation.
* **Private endpoints** for ADLS, Key Vault, SQL endpoints.
* Optional **Azure Firewall / route tables** for outbound security.

### Identity & Access

* **Azure AD** SSO and SCIM for user provisioning.
* **Role-based (RBAC) and attribute-based (ABAC) access** via Unity Catalog.
* **Secrets management** through Key Vault and Databricks Secret Scopes.

### Monitoring & Observability

* **Azure Monitor + Log Analytics** for Databricks job metrics.
* **Audit logs** from UC, Key Vault, and Storage.
* **Alerts & dashboards** for job failures, SLA violations, security events.

---

