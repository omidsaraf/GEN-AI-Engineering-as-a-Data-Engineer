### Phase — Environment & Networking (Infra First)

**Inputs:** Subscription/tenant IDs, naming/tags.
**Actions:** Provision **RG, VNet/Subnet, ADLS, Key Vault, Databricks Workspace, Access Connector** via Terraform; add **Private Endpoints** and **Private DNS** for Storage/KV. *(See Infra TF.)*
**Outputs:** Resource IDs, private data plane, MI principal.
**Validation:** `terraform apply` success; `nslookup *.privatelink.*` resolves.

### Phase — Governance, Unity Catalog, Tables & Base Security (EARLY)

> Moved earlier as requested: **Unity Catalog setup + table DDL + base security** now come **before** any pipelines.

**Inputs:** Access Connector, Storage URL, Entra groups, cluster policy JSON.
**Actions:**

1. **Unity Catalog bootstrap (SQL):** create **STORAGE CREDENTIAL**, **EXTERNAL LOCATION**, **CATALOGS** (`niloomid_{env}`), **SCHEMAS** (`raw/clean/gold/meta/ops`), **VOLUMES**; apply **GRANTS**.
2. **Tables setup (DDL):** create **Bronze/Silver/Gold** tables with constraints & properties (see DDL below).
3. **Base security:** create **KV‑backed secret scope**; enforce **Single‑User + Photon** cluster policy; cost tags.


**DDL — Data Model (Bronze → Silver → Gold)**

```sql
-- Bronze (raw)
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

-- Silver (constraints + CDF)
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

-- RAG corpus
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

**Outputs:** Governed catalogs/schemas, **tables created early**, secret scope, policy ID.
**Validation:** `SHOW CATALOGS/SCHEMAS/GRANTS`; `DESCRIBE HISTORY` on tables; `dbutils.secrets.get` works; policy blocks out‑of‑policy edits.

### Phase — Repository & CI/CD Scaffolding

**Inputs:** Repo URL, Python toolchain.
**Actions:** Adopt the **Repository Layout (Final, Validated)**; create **GitHub Actions** workflow (lint, pytest, GE, CodeQL, TruffleHog, Bundles deploy).
**Outputs:** Clean repo; `.github/workflows/ci.yml`.
**Validation:** CI green on fresh clone; PR checks enforced.

### Phase — HLA/LLD & Data Flows (Pipelines)

**Inputs:** Ingestion patterns, domain model.
**Actions:** Publish **HLA**, **LLD** per pipeline, **DLT dataflow**, **RAG sequence** diagrams in `/docs/diagrams` (Mermaid).
**Outputs:** Approved diagrams (non‑crossing, left‑to‑right).
**Validation:** Design review sign‑off.

### Phase — Data Contracts & DQ (Great Expectations)

**Inputs:** Source schema, SLAs, samples.
**Actions:** Author **contracts** (`/contracts/*.yml`); implement **GE** suites & checkpoints; wire CI schema‑diff gate; define **quarantine & backfill** runbooks.
**Outputs:** Contracts, GE context/suites, runbooks.
**Validation:** GE pass ≥ 99% locally and in cluster; CI blocks drift.

### Phase — Bronze Ingestion (Streaming & Batch)

**Inputs:** Landing paths, schema registry, optional Kafka.
**Actions:** Configure **Autoloader** for JSON/CSV, checkpoints; optional Kafka source; backfill job. *(See `10_autoloader_bronze.py`.)*
**Outputs:** `raw.events_bronze`.
**Validation:** Lag < 5m; schema tracked; partitions healthy.

### Phase — Silver Cleanse, Conform & GE Gate

**Inputs:** Bronze table, GE suites.
**Actions:** Deduplicate, cast, derive `event_dt`; run **GE** gate; route failures to `ops.quarantine_events`. *(See `20_silver_cleaning.py`.)*
**Outputs:** `clean.events_silver`, quarantine table.
**Validation:** GE ≥ 99%; constraints enforced.

### Phase — Gold KPIs & Curated Text

**Inputs:** Silver tables.
**Actions:** Build **`gold.kpi_daily`** and **`gold.docs_text`** for RAG. *(See `30_gold_kpis.sql`.)*
**Outputs:** KPI aggregates; curated text.
**Validation:** Counts/partitions as expected; dashboards read OK.

### Phase — Retrieval & Indexing (Vector + BM25)

**Inputs:** `gold.docs_text`.
**Actions:** Clean & chunk; **embed**; build **FAISS** or **Qdrant** index; enable **BM25** (OpenSearch/Elastic). *(See `preprocessing.py`, `embed.py`.)*
**Outputs:** ANN index + lexical index.
**Validation:** kNN smoke relevant; cosine ≥ 0.6.

### Phase — RAG Flow & Judgement

**Inputs:** Indices, prompts.
**Actions:** Hybrid retrieve → cross‑encoder **rerank** → grounded prompt to LLM; return citations; add **LLM‑as‑judge** with circuit breakers. *(See RAG Flow & Judgement.)*
**Outputs:** Answer with citations + metrics.
**Validation:** Hit‑rate ≥ 0.85; faithfulness ≥ 0.75; p95 ≤ 2.5s.

### Phase — Agent & API (Serving)

**Inputs:** RAG chain, KV secrets.
**Actions:** Build **LangGraph** agent; expose **FastAPI** `/qa`; add **OTel** traces & SLA guardrails; containerize with Docker. *(See `agent.py`, `api.py`, Dockerfile.)*
**Outputs:** `niloomid/ai-api` image; service endpoint.
**Validation:** Canary p95 ≤ 2.5s; traces visible; no PII in logs.

### Phase — Orchestration (DLT, Workflows, Airflow)

**Inputs:** `dlt/pipeline.json`, notebooks, Bundles config, Airflow provider.
**Actions:** Run **DLT** continuous with expectations; declare **Workflows** via **Bundles**; schedule **Airflow** DAG for batch sequences.
**Outputs:** Continuous ELT + scheduled jobs.
**Validation:** Expectations firing; green runs; SLAs met.

### Phase — CI/CD Promotions & Security Scans

**Inputs:** Actions workflows.
**Actions:** Lint, tests, GE, **CodeQL**, **TruffleHog**; Bundles deploy dev→prod with approvals & rollback.
**Outputs:** Immutable deploy pipeline.
**Validation:** All checks green; rollback tested.

### Phase — Observability & SRE

**Inputs:** OTel exporter, metrics spec.
**Actions:** Emit logs/metrics/traces; **Grafana/DBSQL** dashboards for throughput/lag/GE/LLM cost; alerts.
**Outputs:** Dashboards + alerting.
**Validation:** Synthetic checks; on‑call rota active.

### Phase — Evaluation & Continuous Improvement

**Inputs:** Labeled eval set; golden questions.
**Actions:** Run offline harness (precision\@k, hit‑rate, faithfulness); enable online judge; log `ops.rag_eval_metrics`; analyze drifts; iterate.
**Outputs:** Daily evaluation aggregates; improvement backlog.
**Validation:** Thresholds maintained; regressions auto‑blocked.

### Phase — Security Hardening & Compliance

**Inputs:** Policies, audits.
**Actions:** Implement masking/RLS; rotate secrets; remove PATs; IP allowlists; audit lineage; apply pen‑test fixes.
**Outputs:** Compliant posture.
**Validation:** Access review & audit evidence.

### Phase — Go‑Live & Handover

**Inputs:** Validation checklist & evidence.
**Actions:** 10% canary; monitor 24h; promote to 100%; publish runbooks and ownership.
**Outputs:** Live system; SRE handover complete.
**Validation:** SLO & cost steady; incident drill passed.


**Validation**

* CI must pass on fresh clone (`pytest`, GE, scans).
* Notebooks runnable under UC with the configured scopes/policies.
* Bundles deploy creates the intended Workflows; DAG succeeds end‑to‑end.
