### Environment & Networking (Infra First)

**Inputs:** Subscription/tenant IDs, naming/tags.
**Actions:** Provision **RG, VNet/Subnet, ADLS, Key Vault, Databricks Workspace, Access Connector** via Terraform; add **Private Endpoints** and **Private DNS** for Storage/KV. *(See Infra TF.)*
**Outputs:** Resource IDs, private data plane, MI principal.
**Validation:** `terraform apply` success; `nslookup *.privatelink.*` resolves.

### Governance, Unity Catalog, Tables & Base Security (EARLY)

> Moved earlier as requested: **Unity Catalog setup + table DDL + base security** now come **before** any pipelines.

**Inputs:** Access Connector, Storage URL, Entra groups, cluster policy JSON.
**Actions:**

1. **Unity Catalog bootstrap (SQL):** create **STORAGE CREDENTIAL**, **EXTERNAL LOCATION**, **CATALOGS** (`niloomid_{env}`), **SCHEMAS** (`raw/clean/gold/meta/ops`), **VOLUMES**; apply **GRANTS**.
2. **Tables setup (DDL):** create **Bronze/Silver/Gold** tables with constraints & properties (see DDL below).
3. **Base security:** create **KV‑backed secret scope**; enforce **Single‑User + Photon** cluster policy; cost tags.

