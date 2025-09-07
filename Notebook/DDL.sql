
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

