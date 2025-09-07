
## Unity Catalog Setup

> Run in a UC‑enabled SQL warehouse or Databricks SQL.

```sql
-- 1) Storage credential via managed identity (example)
CREATE STORAGE CREDENTIAL sc_niloomid
  WITH AZURE_MANAGED_IDENTITY
  COMMENT 'MI for lake access';

-- 2) External location for landing/raw
CREATE EXTERNAL LOCATION loc_lake_landing
  URL 'abfss://landing@<storage_account>.dfs.core.windows.net/'
  WITH STORAGE CREDENTIAL sc_niloomid
  COMMENT 'Landing zone';

-- 3) Catalogs per environment
CREATE CATALOG IF NOT EXISTS niloomid_dev;
CREATE CATALOG IF NOT EXISTS niloomid_test;
CREATE CATALOG IF NOT EXISTS niloomid_prod;

-- 4) Schemas (databases)
CREATE SCHEMA IF NOT EXISTS niloomid_dev.raw;
CREATE SCHEMA IF NOT EXISTS niloomid_dev.clean;
CREATE SCHEMA IF NOT EXISTS niloomid_dev.gold;
CREATE SCHEMA IF NOT EXISTS niloomid_dev.meta;
CREATE SCHEMA IF NOT EXISTS niloomid_dev.ops;

-- 5) Volumes for unstructured content
CREATE VOLUME IF NOT EXISTS niloomid_dev.raw.docs VOLATILE;

-- 6) Grants (principals = groups/SPNs)
GRANT USE CATALOG ON CATALOG niloomid_dev TO `de_admin`, `de_pipeline`, `data_analyst`;
GRANT SELECT ON SCHEMA niloomid_dev.gold TO `data_analyst`;
GRANT MODIFY, SELECT ON SCHEMA niloomid_dev.clean TO `de_pipeline`;
```

**Best Practices**

* Use **external locations** for Bronze/landing; **managed tables** for Silver/Gold.
* Enforce **constraints** and **table properties** (see §29 DDL) and enable change data feed if required.
* Prefer **service principals** mapped to UC groups; avoid user PATs for pipelines.

---

## Security Best Practices (UC + Network)

* **Identity & Access**: UC groups for roles; service principals for pipelines; **least privilege**.
* **Secrets**: Key Vault–backed secret scopes; no secrets in notebooks/CI logs.
* **Network**: Private Link/Service Endpoints to Storage; restrict egress; IP access lists.
* **Compute**: Single‑user mode for production jobs; cluster policies enforcing Photon, auto‑termination, tags; pinned runtimes.
* **Data**: Dynamic views for row/column masking; PII redaction at Silver; DLP scanning in CI.
* **Lineage/Audit**: UC lineage + Delta history; log `run_id`, inputs/outputs per task.

**Dynamic Row Filter (example)**

```sql
CREATE OR REPLACE VIEW niloomid_dev.clean.events_rls AS
SELECT * FROM niloomid_dev.clean.events_silver
WHERE CASE
  WHEN current_user() IN ('analyst_apac') THEN region = 'APAC'
  WHEN current_user() IN ('analyst_eu')   THEN region = 'EU'
  ELSE true END;
```

---

## Data Model DDL (Bronze → Silver → Gold)

```sql
-- Bronze
CREATE TABLE IF NOT EXISTS niloomid_dev.raw.events_bronze (
  event_id STRING,
  event_ts TIMESTAMP,
  event_type STRING,
  content STRING,
  src_file STRING,
  ingest_ts TIMESTAMP
) USING DELTA TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
);

-- Silver (constraints + expectations mirrored)
CREATE TABLE IF NOT EXISTS niloomid_dev.clean.events_silver (
  event_id STRING NOT NULL,
  event_ts TIMESTAMP NOT NULL,
  event_type STRING NOT NULL,
  content STRING,
  event_dt DATE GENERATED ALWAYS AS (CAST(event_ts AS DATE))
) USING DELTA TBLPROPERTIES (
  delta.enableChangeDataFeed = true,
  delta.constraints.event_id_chk = 'event_id RLIKE "^[A-Z0-9_-]{12,}$"'
);

-- Gold KPIs
CREATE TABLE IF NOT EXISTS niloomid_dev.gold.kpi_daily AS
SELECT event_dt, event_type, COUNT(*) AS cnt
FROM niloomid_dev.clean.events_silver
GROUP BY event_dt, event_type;

-- Docs & chunks for RAG
CREATE TABLE IF NOT EXISTS niloomid_dev.clean.docs_raw (
  doc_id STRING,
  source STRING,
  content STRING,
  load_ts TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS niloomid_dev.clean.docs_chunks (
  doc_id STRING,
  chunk_id STRING,
  chunk_text STRING
) USING DELTA;
```
