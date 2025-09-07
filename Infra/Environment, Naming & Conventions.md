
**Workspaces & UC**

* Workspaces: `niloomid-{dev|test|prod}`; Resource Groups: `rg-niloomid-{env}`.
* Unity Catalog objects:

  * Catalogs: `niloomid_{env}` (e.g., `niloomid_dev`).
  * Schemas: `raw`, `clean`, `gold`, `meta`, `ops`.
  * Tables follow `{domain}_{entity}_{layer}` e.g., `events_bronze`, `events_silver`, `kpi_daily`.
* Jobs & DAGs: `RAG_{domain}_{env}`; Clusters: `dbrx-{layer}-{env}`.

**RBAC**

* Roles: `de_admin`, `de_pipeline`, `data_analyst`, `secops`.
* Minimal grants (examples):

  * `GRANT USE CATALOG ON CATALOG niloomid_{env} TO de_admin, de_pipeline, data_analyst;`
  * `GRANT SELECT ON SCHEMA niloomid_{env}.gold TO data_analyst;`
  * Row‑/column‑level masking via views (see §17.3).

---

## Cluster Policies (Security & Cost)

**Policy JSON (example)**

```json
{
  "spark_version": {"type": "fixed", "value": "14.3.x-scala2.12"},
  "autotermination_minutes": {"type": "range", "minValue": 10, "maxValue": 120, "defaultValue": 30},
  "num_workers": {"type": "range", "minValue": 1, "maxValue": 10, "defaultValue": 2},
  "data_security_mode": {"type": "fixed", "value": "SINGLE_USER"},
  "runtime_engine": {"type": "fixed", "value": "PHOTON"},
  "aws_attributes": {"availability": {"type": "fixed", "value": "SPOT_WITH_FALLBACK"}},
  "azure_attributes": {"first_on_demand": {"type": "fixed", "value": 1}},
  "custom_tags": {"CostCenter": "NILOOMID-DE", "Owner": "DataPlatform"}
}
```

Attach to all jobs; enforce spot-with-fallback (or Azure low‑priority) with on‑demand minimum.

---
