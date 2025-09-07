# GENAI — Data Engineering Project

> **Goal:** a single, production‑ready blueprint, end‑to‑end project with HLA, LLD, Data Flows, code templates, governance, CI/CD, tests, and runbooks. Optimized for **Azure Databricks + Delta/Unity Catalog**, **Airflow** orchestration, **Azure DevOps/GitHub Actions** CI/CD, and **Agentic/RAG** workloads.

---
<img width="518" height="155" alt="image" src="https://github.com/user-attachments/assets/b6018e19-d14d-4604-8b30-3ef5041223c0" />

## Executive Summary & SLOs

**Scope**

* Data lakehouse (Bronze → Silver → Gold) on Databricks/Delta (Unity Catalog enabled)
* Batch + Streaming ingestion, validation (Great Expectations), lineage, logging
* RAG search over curated text (Gold/Text) with vector DB (FAISS/Qdrant)
* Agentic workflows (LangGraph) + FastAPI service layer + Observability
* Infra as Code (Terraform/Bicep), CI/CD (GitHub Actions/Azure Pipelines)

**SLOs & Guardrails**

* p95 API latency ≤ 2.5s; embedding throughput ≥ 1k chunks/min (autoscale)
* DQ pass rate ≥ 99% at Silver gates; schema drift blocked by CI
* RAG: retrieval hit‑rate ≥ 0.85; faithfulness ≥ 0.75; hallucination ≤ 5%
* Cost budget: ≤ \$X/1k requests; storage lifecycle policies active


**Plan (Initiate → Build → Operate)**

Start with Goals, publish a tools map + HLA, then complete foundational setups (infra, networking, Unity Catalog, security). After that, build ingestion, Silver/Gold pipelines, RAG/Agent/API, orchestration, CI/CD, and SRE observability.
Each phase follows: Inputs → Actions → Outputs → Validation Gate.

---
## 1.High‑Level Architecture (HLA)

```mermaid
flowchart LR
  %% --- Sources ---
  subgraph Sources
    s3["S3 / ADLS / HTTP"]
    db["OLTP / DB"]
    docs["Docs / PDF / Web"]
    kafka["Kafka"]
  end

  %% --- Lakehouse ---
  subgraph Lakehouse["Databricks + Delta + Unity Catalog"]
    subgraph Bronze["Bronze"]
      auto["Auto Loader / Batch Landing"]
    end
    subgraph Silver["Silver"]
      clean["Cleanse & Conform"]
      dq["Great Expectations"]
    end
    subgraph Gold["Gold"]
      kpi["KPI / Features"]
      text["Curated Text"]
    end
  end

  %% --- AI / Agentic ---
  subgraph AI["RAG + Agentic"]
    embed["Embeddings"]
    vdb["Vector DB (FAISS · Qdrant)"]
    retr["Retriever"]
    llm["LLM / Prompt Layer"]
    agent["LangGraph Agent"]
    api["FastAPI Service"]
  end

  %% --- Ops ---
  subgraph Ops["CI/CD & Observability"]
    ci["GitHub Actions · Azure Pipelines"]
    mon["OTel · Logs · Metrics"]
    sec["Key Vault · RBAC · Policies"]
  end

  %% --- Edges ---
  s3 --> auto
  db --> auto
  docs --> auto
  kafka --> auto

  auto --> clean --> dq --> kpi
  clean --> text

  text --> embed --> vdb --> retr --> llm --> agent --> api

  ci -.-> Lakehouse
  ci -.-> AI
  mon -.-> AI
  sec -.-> AI
```

**Notes**

- Unity Catalog enforces RBAC & isolation.

- Private Endpoints secure data plane.

- Observability: OTel traces + Prometheus/Grafana dashboards.



## 2. Infra & Tools

### System Integrations

**HLA — System Integration Map**

```mermaid
flowchart LR
  subgraph ORCH["Orchestration & CI/CD"]
    gh["GitHub Actions"]
    af["Airflow"]
    dab["Databricks Workflows / Bundles"]
    tf["Terraform / Bicep"]
  end
  subgraph SEC["Security & Governance"]
    kv["Azure Key Vault"]
    uc["Unity Catalog"]
    pol["Cluster Policies"]
    plink["Private Link / VNet"]
  end
  subgraph DATA["Data Plane"]
    adls["ADLS Gen2"]
    dbx["Databricks Runtime (Photon)"]
    delta["Delta Lake"]
    ge["Great Expectations"]
    kafka["Kafka"]
    vdb["Vector DB (FAISS · Qdrant)"]
    search["BM25 (OpenSearch / Elastic)"]
    llm["LLM Provider"]
  end
  subgraph SRV["Serving"]
    agent["LangGraph Agent"]
    api["FastAPI Service"]
  end
  subgraph OBS["Observability"]
    otel["OpenTelemetry"]
    graf["Prometheus / Grafana / DBSQL"]
    secscan["CodeQL / TruffleHog"]
  end
  tf --> dbx
  tf --> adls
  gh --> dab
  gh --> secscan
  af --> dbx
  dab --> dbx
  kv -.-> dbx
  kv -.-> api
  uc --> dbx
  plink -.-> adls
  pol --> dbx
  adls --> dbx
  kafka --> dbx
  dbx --> delta
  delta --> vdb
  delta --> api
  agent --> vdb
  agent --> search
  api --> agent
  agent --> llm
  otel -.-> api
  otel -.-> agent
  otel -.-> dbx
  graf -.-> api
  graf -.-> dbx
```

**Tools Table — Purpose, Reasons, Benefits**

| Area          | Tool / Service                 | Purpose / Role                                    | Why This Choice                                 | Key Benefits                        | Notes / Alternatives             |
| ------------- | ------------------------------ | ------------------------------------------------- | ----------------------------------------------- | ----------------------------------- | -------------------------------- |
| Infra         | Terraform / Bicep              | IaC for Azure (RG, VNet, Storage, KV, Databricks) | Reproducible, reviewable infra; drift detection | Idempotent deploys, versioned state | Pulumi/Terragrunt optional       |
| Orchestration | Airflow                        | Batch/seq control of Databricks jobs              | Mature scheduler, DBX provider                  | Clear DAGs, retries, SLA alerts     | DBX Workflows for simpler graphs |
| CI/CD         | GitHub Actions                 | Lint, tests, GE, scans, Bundles deploy            | Native to GitHub                                | Automates gates & promotions        | Azure Pipelines also fits        |
| Secrets       | Azure Key Vault (+ DBX scopes) | Secret storage & rotation                         | Managed HSM, RBAC                               | Keeps secrets out of code/logs      | KV‑backed DBX scopes             |
| Governance    | Unity Catalog                  | Central RBAC, lineage                             | Fine‑grained access, audit                      | Cross‑workspace consistency         | Use Entra groups                 |
| Compute       | Databricks Photon              | ETL/ELT, ML, vector ops                           | Optimized Spark                                 | High throughput, lower cost         | Pin runtime via policy           |
| Storage       | ADLS Gen2                      | Lake storage                                      | Azure‑native, POSIX ACLs                        | Scale + Private Link                | S3 if AWS                        |
| Format        | Delta Lake                     | ACID tables                                       | Time travel, CDF, MERGE                         | Reliable lakehouse                  | Parquet lacks ACID               |
| DQ            | Great Expectations             | Data quality gates                                | Declarative, CI‑friendly                        | Early failure, quarantine           | Deequ/dbt‑tests alt              |
| Streaming     | Kafka                          | Real‑time ingestion                               | Ubiquitous, Spark‑friendly                      | Low latency, scalable               | Event Hubs compatible            |
| Vector        | FAISS / Qdrant                 | ANN retrieval                                     | FAISS fast local; Qdrant service                | ms‑level vector search              | Milvus/Weaviate alt              |
| Lexical       | OpenSearch/Elastic (BM25)      | Keyword retrieval                                 | Hybrid improves recall                          | Search, filters                     | `rank_bm25` lightweight          |
| Agent         | LangGraph                      | Deterministic agents                              | Graph over prompts                              | Debuggable tool‑use                 | LC Agents/Guidance alt           |
| API           | FastAPI                        | Serve `/qa` & ops                                 | Async, type‑safe                                | Easy auth/obs                       | Flask/Starlette alt              |
| Obs           | OpenTelemetry                  | Traces/metrics/logs                               | Open standard                                   | E2E tracing                         | Pair Azure Monitor               |
| Monitoring    | Prometheus/Grafana/DBSQL       | Metrics dashboards                                | OSS + DBSQL                                     | Single‑pane SLOs                    | Azure Workbooks                  |
| Sec Scans     | CodeQL / TruffleHog            | SAST + secrets                                    | Shift‑left security                             | Blocks risky PRs                    | Semgrep/Gitleaks alt             |
| Packaging     | Databricks Bundles             | Declarative deploys                               | Env‑aware deployment                            | Reproducible jobs                   | dbx CLI alt                      |
| Network       | Private Link / VNet            | Private plane                                     | Compliance, egress control                      | Reduce exposure                     | NSGs/Firewall req                |


---------------

## 3.Repository Layout

```
GENAI-ai-engineer/
├── .github/
│   └── workflows/
│       └── ci.yml                   # Lint, Pytest, GE checks, CodeQL, TruffleHog, bundle deploys
├── infra/
│   ├── terraform/                    # RG, VNet, ADLS, KV, Databricks workspace & access connector
│   └── bicep/                        # Optional Azure-native templates for IaC
├── workflows/
│   └── databricks.yml                # Asset Bundles / job orchestration config
├── notebooks/
│   ├── setup_uc.sql               # Unity Catalog setup, catalogs, schemas, grants
│   ├── autoloader_bronze.py       # Bronze ingestion (batch & streaming)
│   ├── silver_cleaning.py         # Silver cleaning, dedup, GE validation
│   ├── gold_kpis.sql              # KPI aggregations
│   └── embed_index.py                 # Build embeddings & FAISS/Qdrant index
├── dlt/
│   └── pipeline.json                 # Delta Live Tables pipeline config
├── dags/
│   └── rag_pipeline.py               # Airflow DAG (silver → gold → embed → API)
├── src/
│   ├── ingestion.py                  # Landing helpers (S3/ADLS/HTTP/Kafka)
│   ├── validation.py                 # GE & contract validation wrappers
│   ├── preprocessing.py              # Text cleaning & chunking
│   ├── embed.py                      # Embeddings & vector DB helpers
│   ├── rag.py                        # Retriever + reranker + QA chain
│   ├── agent.py                      # LangGraph agent orchestration
│   ├── api.py                        # FastAPI service (/qa)
│   └── utils.py                      # Logging, retries, configuration helpers
├── ge/
│   ├── great_expectations.yml
│   ├── expectations/                 # GE expectation suites (Silver / Gold)
│   └── checkpoints/                  # GE checkpoint configs
├── contracts/
│   └── events.yml                    # Schema, SLA, retention, quality contracts
├── docker/
│   ├── api/
│   │   └── Dockerfile                # FastAPI container
│   └── qdrant/
│       └── docker-compose.yml        # Local Qdrant/Vector DB deployment
├── tests/
│   ├── test_chunks.py
│   ├── test_rag_eval.py
│   ├── test_ingestion.py
│   └── test_api_contracts.py
├── docs/
│   ├── adr/                          # Architecture Decision Records
│   ├── policies/                     # SLOs, RBAC, privacy, security
│   └── diagrams/                     # HLA.mmd, DLT_flow.mmd, RAG_sequence.mmd
├── data/
│   ├── raw/                           # Optional raw data for local testing
│   └── samples/                       # Sample events / text / embeddings
├── requirements.txt
├── .env.example
├── README.md
└── LICENSE

```
--------
## 4.Low‑Level Design (LLD): Data & AI Pipelines

```mermaid
flowchart LR
  %% left-to-right, quoted labels, no parentheses

  subgraph "Bronze"
    B["events_bronze"]
  end

  subgraph "Silver"
    S["events_silver - GE passed"]
  end

  subgraph "Gold"
    G["docs_for_rag"]
  end

  subgraph "Vectors"
    E["embeddings"]
    X["vector_index"]
  end

  B --> S --> G --> E --> X
```
* **Bronze → Silver**: `raw.events_bronze` → `clean.events_silver` (GE gate)
* **Silver → Gold**: `clean.events_silver` → `gold.kpi_daily`, `gold.docs_text`
* **Gold/Text → Vector DB**: `gold.docs_text` → embeddings → FAISS/Qdrant index
* **Vector DB → API**: retriever → LLM → agent → FastAPI (served)


### Ingestion(bronze)

* **Batch**: S3/ADLS/HTTP → Bronze via `src/ingestion.py` with retries, idempotent writes
* **Streaming**: Kafka → Bronze Autoloader with 2h watermark for joins
**Bronze Notebook (`autoloader_bronze.py`)**


### Silver (Cleanse/Conform)

- Dedup, null/PII handling, type conformance
- **DQ Gate**: Great Expectations suite must pass → otherwise quarantine & alert
- **Silver Notebook (`silver_cleaning.py`)**
- **Great Expectations (example suite)** — `ge/suites/events_silver.json`

```json
{
  "expectations": [
    {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "event_type"}},
    {"expectation_type": "expect_column_values_to_match_regex", "kwargs": {"column": "event_id", "regex": "^[A-Z0-9_-]{12,}$"}},
    {"expectation_type": "expect_table_row_count_to_be_between", "kwargs": {"min_value": 1}}
  ]
}
```

### Gold (KPIs/Curated Text)

- KPIs aggregation + curated text for RAG.
- **Gold SQL (`gold_kpis.sql`)**



### RAG & Vector Indexing

```mermaid
sequenceDiagram
  autonumber
  participant U as User
  participant API as FastAPI
  participant AG as Agent (LangGraph)
  participant RT as Retriever (Hybrid)
  participant RR as Reranker
  participant LLM as LLM
  participant JDG as Judge (LLM-as-judge)

  U->>API: POST /qa {question}
  API->>AG: build state (trace_id, user_id)
  AG->>RT: retrieve top_k (FAISS + BM25)
  RT-->>AG: candidates (docs + scores)
  AG->>RR: cross-encoder rerank
  RR-->>AG: reranked top_k
  AG->>LLM: grounded prompt (context, rules)
  LLM-->>AG: answer + citations
  AG->>JDG: evaluate (faithfulness, relevance)
  JDG-->>AG: metrics + pass/fail
  AG->>API: response (answer, citations, metrics)
  API-->>U: JSON (answer, sources, eval)
```


- **Preprocess & Chunk (`src/preprocessing.py`)**
- **Embeddings + FAISS (`src/embed.py`)**
- **Retriever + QA (`src/rag.py`)**

### Agentic Workflow (LangGraph)
- **Agent (`src/agent.py`)**

### API Layer (FastAPI)
- **Service (`src/api.py`)**

------
## 5.Build


#### Deployment Guide (Step‑by‑Step)

1. **Infra**: Deploy Databricks workspace, Storage, VNets, Key Vault (Terraform/Bicep)
2. **Unity Catalog**: Create catalog/schemas + RBAC; mount ADLS (MI)
3. **Secrets**: Create secret scopes for LLM/DB creds
4. **Data**: Configure Autoloader paths; land sample JSON/CSV
5. **DLT**: Import `pipeline.json`, attach notebooks, start continuous mode
6. **GE**: Initialize context; run suites on Silver before Gold writes
7. **RAG**: Run preprocessing → embeddings → FAISS/Qdrant index build
8. **Agent/API**: `uvicorn src.api:app --host 0.0.0.0 --port 8080`
9. **Orchestration**: Enable Airflow DAG; set SLA and alert rules
10. **CI/CD**: Protect main; require tests + GE; enable environment promotion


### Security, Governance, & Lineage

* **Identity & Access**: UC groups for roles; service principals for pipelines; **least privilege**.
* **Secrets**: Key Vault–backed secret scopes; no secrets in notebooks/CI logs, no plaintext keys in code/CI.
* **Network**: Private Link/Service Endpoints to Storage; restrict egress; IP access lists.
* **Compute**: Single‑user mode for production jobs; cluster policies enforcing Photon, auto‑termination, tags; pinned runtimes.
* **Data**: Dynamic views for row/column masking; PII redaction at Silver; DLP scanning in CI.
* **Lineage/Audit**: UC lineage + Delta history; log `run_id`, inputs/outputs per task.

#### Networking & Secrets

* **Private Link** / service endpoints for Storage & Databricks control plane.
* **Key Vault** backed secret scopes: `kv-llm-key`, `kv-faiss`, `kv-azure-openai`.
* No PATs in CI; use OIDC‑based federation to Databricks & Azure.

### Workspaces & UC

* Workspaces: `GENAI-{dev|test|prod}`; Resource Groups: `rg-GENAI-{env}`.
* Unity Catalog objects:

  * Catalogs: `GENAI_{env}` (e.g., `GENAI_dev`).
  * Schemas: `raw`, `clean`, `gold`, `meta`, `ops`.
  * Tables follow `{domain}_{entity}_{layer}` e.g., `events_bronze`, `events_silver`, `kpi_daily`.
* Jobs & DAGs: `RAG_{domain}_{env}`; Clusters: `dbrx-{layer}-{env}`.

**RBAC**

* Roles: `de_admin`, `de_pipeline`, `data_analyst`, `secops`.
* Minimal grants (examples):

  * `GRANT USE CATALOG ON CATALOG GENAI_{env} TO de_admin, de_pipeline, data_analyst;`
  * `GRANT SELECT ON SCHEMA GENAI_{env}.gold TO data_analyst;`
  * Row‑/column‑level masking via views (see §17.3).
    
### Cluster Policies (Security & Cost)


## 15) Cluster Policies (Security & Cost)

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
  "custom_tags": {"CostCenter": "GENAI-DE", "Owner": "DataPlatform"}
}
```

Attach to all jobs; enforce spot-with-fallback (or Azure low‑priority) with on‑demand minimum.

---

### Data Model DDL (Bronze → Silver → Gold)

```sql
-- Bronze
CREATE TABLE IF NOT EXISTS GENAI_dev.raw.events_bronze (
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
CREATE TABLE IF NOT EXISTS GENAI_dev.clean.events_silver (
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
CREATE TABLE IF NOT EXISTS GENAI_dev.gold.kpi_daily AS
SELECT event_dt, event_type, COUNT(*) AS cnt
FROM GENAI_dev.clean.events_silver
GROUP BY event_dt, event_type;

-- Docs & chunks for RAG
CREATE TABLE IF NOT EXISTS GENAI_dev.clean.docs_raw (
  doc_id STRING,
  source STRING,
  content STRING,
  load_ts TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS GENAI_dev.clean.docs_chunks (
  doc_id STRING,
  chunk_id STRING,
  chunk_text STRING
) USING DELTA;
```


### Masking View (Gold)

```sql
CREATE OR REPLACE VIEW GENAI_{env}.gold.events_masked AS
SELECT event_id,
       event_ts,
       event_type,
       CASE WHEN is_member('data_analyst_pii') THEN content ELSE substr(sha2(content,256),1,16) END AS content
FROM GENAI_{env}.clean.events_silver;
```

### Great Expectations (suite as YAML)**

```yaml
expectations:
  - expect_column_values_to_not_be_null: {column: event_type}
  - expect_column_values_to_match_regex: {column: event_id, regex: "^[A-Z0-9_-]{12,}$"}
  - expect_table_row_count_to_be_between: {min_value: 1}
```

### Quarantine & Backfill

* On GE failure: write failing rows to `GENAI_{env}.ops.quarantine_events` with run\_id & suite.
* Open incident, page on‑call, and execute backfill notebook with partition filters.
  

### Data Contracts (Schema, SLAs, DQ)

**Contract YAML (events) — `contracts/events.yml`**

### Contract YAML (events)

```yaml
name: events
owner: ai-platform@GENAI.com
sla:
  freshness: 15m
  availability: 99.5%
schema:
  event_id: {type: string, required: true, regex: "^[A-Z0-9_-]{12,}$"}
  event_ts:  {type: timestamp, required: true}
  event_type:{type: string, required: true, allowed: [CLICK, VIEW, ERROR]}
  content:   {type: string, required: false}
quality_gates:
  - non_null: [event_id, event_ts, event_type]
  - unique: [event_id]
  - row_count_min: 1
retention:
  bronze: {mode: days, value: 7}
  silver: {mode: months, value: 12}
privacy:
  pii_columns: [content]
  policy: redact
```

**Enforcement**: Validate contracts in CI (schema diff), and at runtime via GE suite mapping.

-------

### RAG Flow

**Hybrid Retrieval**

* Vector search (FAISS/Qdrant) + lexical BM25 (Elastic/OpenSearch or `rank_bm25`) → union → rerank (cross‑encoder) → top‑k.
* Document chunking: 512–1024 tokens with 10–50 overlap; normalize whitespace; strip boilerplate; attach metadata (doc\_id, section, timestamp, source).

**Grounding & Prompt Rules**

* Always include **system instructions**: “Answer strictly from context. If insufficient, say ‘I don’t know.’ Return citations as `[doc_id:chunk_id]`.”
* Inject **guardrails** (PII redaction, safe completion) and **domain glossary** to reduce ambiguity.

---

### RAG Judgement & Evaluation (Automated)

**Metrics**: `retrieval_hit_rate`, `precision@k`, `faithfulness`, `answer_relevance`, `latency_p95`, `cost_per_req`.

**Eval Harness (offline)** — `tests/test_rag_faithfulness.py`

**Online Eval**

* Log per‑request: retrieved\_ids, rerank\_scores, token\_usage, latency, judge\_scores.
* Canary gating: if `faithfulness < 0.7` or `hit_rate < 0.8`, trip circuit → fallback (template reply or escalate to human‑in‑loop).



---

### RAG Evaluation Harness
tests/test_rag_eval.py

**Metrics to track**: retrieval hit‑rate, precision\@k, faithfulness (LLM judge), answer latency, token usage, cost/request.

------

## API & Agent (Hardened)

**Prompting**

* System: “You are an enterprise assistant. Use only supplied context. If missing, say ‘I don’t know.’ Return citations.”
* Policy snippets: blocked topics/PII; max answer length; cite top‑3 contexts.

**FastAPI (with tracing & limits)**

```python
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import time

app = FastAPI()

class Q(BaseModel):
    question: str

@app.post("/qa")
def qa(q: Q):
    t0 = time.time()
    # retrieve → rerank → llm → judge (omitted)
    latency = time.time() - t0
    if latency > 2.5:  # p95 guardrail sample
        raise HTTPException(503, "SLA breach")
    return {"answer": "stub", "latency": latency, "citations": []}
```
---

## 6.Delta Live Tables (DLT) — Dataflow

<img width="289" height="374" alt="image" src="https://github.com/user-attachments/assets/561c3739-3027-45a7-b137-1eff85b501eb" />


```mermaid
flowchart LR
  A[Autoloader: landing/events] --> B((Bronze events_bronze))
  B --> C[GE Gate]
  C --> D((Silver events_silver))
  D --> E{Branch}
  E -->|KPIs| F((Gold kpi_daily))
  E -->|Text| G((Gold docs_text))
  G --> H[Embeddings]
  H --> I[FAISS/Qdrant]
  I --> J[Retriever → LLM → Agent → API]
```

**DLT `pipeline.json`**

```json
{
  "name": "GENAI-dlt",
  "edition": "ADVANCED",
  "clusters": [{"num_workers": 2}],
  "libraries": [],
  "continuous": true,
  "development": true,
  "photon": true
}
```
### DLT Tables with Expectations (Enforced)

**DLT notebook snippet**

```python
import dlt
from pyspark.sql.functions import col

@dlt.table(name="events_bronze")
def bronze():
    return spark.readStream.format("cloudFiles").option("cloudFiles.format","json").load("/mnt/lake/landing/events")

@dlt.expect("valid_event_type", "event_type IS NOT NULL")
@dlt.expect_or_drop("valid_id", "event_id RLIKE '^[A-Z0-9_-]{12,}$'")
@dlt.table(name="events_silver")
def silver():
    return dlt.read_stream("events_bronze").dropDuplicates(["event_id"]).withColumn("event_dt", col("event_ts").cast("date"))

@dlt.table(name="kpi_daily")
def kpi():
    return dlt.read("events_silver").groupBy("event_dt","event_type").count()
```

---
## 6.Orchestration — Airflow DAG

 ### version1- Airflow Setup (Docker + Databricks Provider)

**Connections**

* `databricks_default`: host/workspace, OAuth or PAT (prefer OAuth via OIDC).
* `GENAI_kv`: for pulling non-DBX secrets if absolutely needed.

**DAG** — `dags/rag_pipeline.py`

```python
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime

new_cluster = {
  "spark_version": "14.3.x-scala2.12",
  "num_workers": 2,
  "data_security_mode": "SINGLE_USER",
  "spark_conf": {"spark.databricks.delta.properties.defaults.checkpointInterval": "10"}
}

with DAG("rag_pipeline", start_date=datetime(2025,1,1), schedule_interval="@daily", catchup=False) as dag:
    silver = DatabricksSubmitRunOperator(
        task_id="silver",
        json={"new_cluster": new_cluster,
              "notebook_task": {"notebook_path": "/Repos/GENAI/20_silver_cleaning.py"}}
    )
    gold = DatabricksSubmitRunOperator(
        task_id="gold",
        json={"new_cluster": new_cluster,
              "notebook_task": {"notebook_path": "/Repos/GENAI/30_gold_kpis.sql"}}
    )
    embed = DatabricksSubmitRunOperator(
        task_id="embed",
        json={"new_cluster": new_cluster,
              "notebook_task": {"notebook_path": "/Repos/GENAI/embed_index.py"}}
    )
    silver >> gold >> embed
```

### Version2- Orchestration — Airflow DAG

```python
# dags/rag_pipeline.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG("rag_pipeline", start_date=datetime(2025,1,1), schedule_interval="@daily", catchup=False) as dag:
    ingest = PythonOperator(task_id="ingest", python_callable=lambda: None)
    validate = PythonOperator(task_id="validate", python_callable=lambda: None)
    silver = PythonOperator(task_id="to_silver", python_callable=lambda: None)
    gold = PythonOperator(task_id="to_gold", python_callable=lambda: None)
    index = PythonOperator(task_id="build_index", python_callable=lambda: None)
    serve = PythonOperator(task_id="deploy_api", python_callable=lambda: None)

    ingest >> validate >> silver >> gold >> index >> serve
```

**Watermarks & Joins**: Use 2h watermark for stream‑batch joins in Silver to avoid late data skew.

---

## 7.CI/CD — Tests, Quality Gates, Deploy

```mermaid
flowchart LR
  C[Commit/PR] --> Lint[Lint + Type Check]
  Lint --> Py[Pytest]
  Py --> GE[GE Suites]
  GE --> Sec[SAST/Secrets scan]
  Sec --> Pack[Build Docker + DLT cfg]
  Pack --> Plan[Terraform Plan]
  Plan --> Apply[Deploy Dev]
  Apply --> Smoke[Smoke Tests]
  Smoke --> Promote{Promote?}
  Promote -->|Yes| Prod[Deploy Prod]
  Promote -->|No| Fix[Fail & Rollback]
```

**GitHub Actions (`.github/workflows/ci.yml`)**

```yaml
name: ci
on: [push, pull_request]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: {python-version: '3.11'}
      - run: pip install -r requirements.txt
      - run: pytest -q
      - run: echo "Run GE suites here"
      - run: docker build -t GENAI/api ./docker/api
```
### Version2- CI/CD — Advanced (Bundles, Scans, Promotion)

**Databricks Asset Bundles (DAB) — `databricks.yml`**

**GitHub Actions — hardened**


**Promotion Gate**

* Require: unit+integration tests green, GE pass ≥ 99%, cost budget OK, drift < threshold, p95 latency ≤ 2.5s on canary.

---

## 8.Observability & Runbooks

* **Logging**: Structured logs (JSON) with correlation IDs from DAG → notebooks → API
* **Metrics**: Throughput, lag, error rate, GE pass %, top‑k recall, LLM token usage
* **Tracing**: OTel spans around retriever/LLM calls; propagate request IDs
* **Alerts**: Slack/Email on GE failures, 5xx spikes, cost anomalies

**Runbooks**

* Data quality failure → quarantine partition, open incident, backfill
* Model drift → lower confidence, trigger re‑embed + re‑index
* Cost spike → autoscaling policy review, cache thresholds, batch window tuning

**Incident Response & SRE Playbook**

* **Sev1**: pipeline down or PII leak suspected → freeze writes, rotate keys, incident bridge.
* **Sev2**: GE failure > 30m → quarantine + backfill; RCA within 24h.
* **Sev3**: KPI drift → review transformations; schedule re‑embed.


---


## 9.Ready‑to‑Use Checklists

**Env & UC**

* [ ] Catalogs/schemas exist; RBAC grants logged (screenshot or SQL history link)
* [ ] Private Link enabled; subnets isolated

**Pipelines**

* [ ] Bronze Autoloader active ≥ 2h; watermark 2h; lag < 5m
* [ ] Silver GE pass ≥ 99%; quarantine empty
* [ ] Gold KPIs populated; docs\_text non‑empty

**RAG/Agent/API**

* [ ] Index contains ≥ N vectors; cosine sim average ≥ 0.6 on sample
* [ ] p95 latency ≤ 2.5s on canary; 0 error spikes in last 24h

**CI/CD & Security**

* [ ] CI green (tests, lint, SAST, secret scan)
* [ ] DAB deploy succeeded; Workflows scheduled
* [ ] Cost dashboard within budget

**Pre‑Prod**

* [ ] Unity Catalog RBAC; secrets mounted
* [ ] DLT pipeline green ≥ 24h
* [ ] GE suites ≥ 99% pass at Silver; schema registry stable
* [ ] CI gates: lint, tests, GE, SAST, secret scan

**Go‑Live**

* [ ] Canary 10% traffic; monitor p95 latency
* [ ] Cost guardrails; autoscaling verified
* [ ] On‑call rota and runbooks published


## 10.Project — Tools‑Integrated Step‑by‑Step

> Each step includes **Inputs → Actions → Outputs → Validation** and shows the **tool(s)** used.

### Step 1 — Bootstrap & Repo

**Tools:** GitHub, GitHub Actions.
**Inputs:** Repo URL, workstation.
**Actions:** Clone, set Python venv, install reqs; add base Actions workflow skeleton (lint/tests).
**Outputs:** Local dev env; `.github/workflows/ci.yml`.
**Validation:** `pytest -q` green; Actions trigger on PR.

### Step 2 — Provision Infra (IaC)

**Tools:** Terraform/Bicep.
**Inputs:** Subscription, region, naming vars.
**Actions:** Deploy RG, VNet/Subnet, ADLS Gen2, Key Vault, **Databricks Workspace**, **Access Connector** (*see Phase 1 TF*).
**Outputs:** Infra resources, MI principal.
**Validation:** `terraform apply` success; resources visible in portal.

### Step 3 — Private Networking

**Tools:** Private Link, VNet, DNS.
**Inputs:** VNet/subnets.
**Actions:** Add Private Endpoints + DNS zones for Storage/KV.
**Outputs:** Private data plane.
**Validation:** Name resolution to `privatelink.*`; public egress blocked.

### Step 4 — Unity Catalog Setup

**Tools:** Databricks SQL, Unity Catalog.
**Inputs:** Storage URL, Access Connector.
**Actions:** Create **storage credential**, **external location**, **catalogs**, **schemas**, **volumes**, **grants** (*§27*).
**Outputs:** `GENAI_{env}` with `raw/clean/gold/meta/ops`.
**Validation:** `SHOW CATALOGS/SCHEMAS/GRANTS` output captured.

### Step 5 — Secrets & Policies

**Tools:** Key Vault, Databricks Secret Scopes, Cluster Policies.
**Inputs:** LLM/API keys; policy JSON (*§15*).
**Actions:** Create KV‑backed scope; apply **Single‑User + Photon** policy; tag clusters.
**Outputs:** `kv-GENAI` scope; policy ID.
**Validation:** `dbutils.secrets.get` works; policy blocks unauthorized edits.

### Step 6 — Contracts & DQ

**Tools:** Great Expectations, GitHub Actions.
**Inputs:** `/contracts/*.yml`.
**Actions:** Author suites; wire schema diff & GE runs in CI (*§16–17*).
**Outputs:** GE context/suites; CI gate.
**Validation:** GE passes locally and in CI; failures quarantine rows.

### Step 7 — Bronze Ingestion

**Tools:** Databricks (Autoloader, Photon), Kafka (optional).
**Inputs:** Landing paths; schema registry.
**Actions:** Start Autoloader stream; checkpointing; optional Kafka source (*§3.1*).
**Outputs:** `raw.events_bronze`.
**Validation:** Lag < 5m; schema persisted; table populated.

### Step 8 — Silver + GE Gate

**Tools:** Databricks, GE.
**Inputs:** Bronze; suites.
**Actions:** Cleanse/conform; derive `event_dt`; apply GE; quarantine failures (*§3.2, §17.2*).
**Outputs:** `clean.events_silver`; `ops.quarantine_events`.
**Validation:** GE ≥ 99%; constraints enforced (*§29*).

### Step 9 — Gold KPIs & Text

**Tools:** Databricks SQL, Delta.
**Inputs:** Silver data.
**Actions:** Create `gold.kpi_daily`; build `gold.docs_text` (*§3.3, §29*).
**Outputs:** KPI table; curated text.
**Validation:** Partition health; expected counts.

### Step 10 — Vector & Search

**Tools:** FAISS/Qdrant, OpenSearch/Elastic (BM25).
**Inputs:** `gold.docs_text`.
**Actions:** Chunk/clean → embeddings → FAISS index or Qdrant collection; enable BM25 index (*§3.4*).
**Outputs:** ANN index; lexical index.
**Validation:** kNN returns relevant ids; cosine ≥ 0.6.

### Step 11 — RAG & Judgement

**Tools:** LangChain/FAISS + Cross‑Encoder, LLM provider.
**Inputs:** Indices, prompts.
**Actions:** Hybrid retrieval → rerank → grounded prompt → **LLM** → **Judge** (*§25–26*).
**Outputs:** Answers with citations + metrics.
**Validation:** `hit_rate ≥ 0.85`, `faithfulness ≥ 0.75`; latency p95 ≤ 2.5s.

### Step 12 — Agent & API

**Tools:** LangGraph, FastAPI, OpenTelemetry.
**Inputs:** RAG chain; keys in KV.
**Actions:** Build agent graph; expose `/qa`; add tracing & limits; containerize (*§3.5–3.6, §32*).
**Outputs:** Docker image + service.
**Validation:** Canary p95 ≤ 2.5s; trace spans present.

### Step 13 — DLT & Workflows

**Tools:** DLT, Databricks Workflows/Bundles.
**Inputs:** `dlt/pipeline.json`, notebooks.
**Actions:** Run DLT continuous; schedule Workflows; bundle deploy (*§4, §18–19, §30*).
**Outputs:** Continuous ELT; scheduled jobs.
**Validation:** Expectations firing; jobs green.

### Step 14 — Airflow DAGs

**Tools:** Airflow + Databricks provider.
**Inputs:** DBX connection; DAG code.
**Actions:** Deploy `rag_pipeline.py` (*§31*).
**Outputs:** Daily batch orchestration.
**Validation:** Task success; SLA emails.

### Step 15 — CI/CD & Security Scans

**Tools:** GitHub Actions, CodeQL, TruffleHog, Databricks Bundles.
**Inputs:** Workflow YAMLs.
**Actions:** Lint/tests/GE; secret & SAST scans; bundles deploy to dev→prod with approvals (*§6, §19*).
**Outputs:** Green checks; immutable deploys.
**Validation:** Promotion gates enforced; rollback path tested.

### Step 16 — Observability & SRE

**Tools:** OpenTelemetry, Prometheus/Grafana, Databricks SQL.
**Inputs:** Metrics spec.
**Actions:** Emit logs/metrics/traces; build dashboards for throughput/lag/GE/LLM cost; alerts.
**Outputs:** Dashboards + alerts.
**Validation:** Synthetic checks; on‑call rota active.

### Step 17 — Go‑Live & Evidence

**Tools:** All above.
**Inputs:** Validation checklist (*§24*, §33).
**Actions:** 10% canary; monitor 24h; capture evidence (grants, lineage, DLT, DAG, CI logs, eval metrics).
**Outputs:** Prod 100%; runbooks published.
**Validation:** SLO & cost steady; incident drill passed.

### Tool → Step Matrix

| Tool                                 | Steps    |
| ------------------------------------ | -------- |
| Terraform/Bicep                      | 2, 3     |
| Unity Catalog                        | 4        |
| Key Vault + Scopes                   | 5        |
| Cluster Policies                     | 5        |
| Great Expectations                   | 6, 8     |
| Databricks (Autoloader/Photon/Delta) | 7–9      |
| FAISS/Qdrant + BM25                  | 10–11    |
| LangGraph + FastAPI                  | 12       |
| DLT + Workflows/Bundles              | 13       |
| Airflow                              | 14       |
| GitHub Actions + CodeQL/TruffleHog   | 1, 6, 15 |
| OpenTelemetry + Grafana/DBSQL        | 16       |

---


## 11.Full Validation Checklist (Pass/Fail with Evidence)

---
## Validation Evidence — What to Capture

* **Screenshots/links**: UC grants, lineage graph, DLT run with expectations, Airflow DAG runs.
* **Tables**: metrics table `ops.rag_eval_metrics` with daily aggregates.
* **Logs**: structured request logs with `trace_id`, token usage, latency.
-----


## 12.Appendices

Metadata Tables (optional).
Traceability Map (Bronze → Silver → Gold → Vector DB → API).
Ready-to-Use Checklists (Pre-Prod + Go-Live).
Environment, Naming & Conventions.
Data Contracts (YAML example preserved).

## Parameterization Matrix

| Layer     | Parameters                                             |
| --------- | ------------------------------------------------------ |
| Ingestion | landing path, schema registry path, maxFilesPerTrigger |
| Silver    | GE suite name, quarantine path/table, primary keys     |
| Gold      | KPI definitions, partition columns                     |
| RAG       | chunk\_size, overlap, top\_k, reranker\_model          |
| API       | max\_tokens, timeout\_s, p95\_budget\_s                |
| CI/CD     | branches, env targets, promotion rules                 |



**This ordering preserves and references all prior code/config sections** (DLT, UC SQL, TF, Airflow, CI, RAG, API, security). Use it as the master rollout runbook.

**End of Blueprint**
