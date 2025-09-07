variable "rg_name" {
  type        = string
  description = "Resource Group name"
}

variable "location" {
  type        = string
  description = "Azure region"
}

variable "kv_name" {
  type        = string
  description = "Key Vault name"
}

variable "tenant_id" {
  type        = string
  description = "Azure Tenant ID"
}
