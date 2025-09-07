```sql
CREATE TABLE IF NOT EXISTS niloomid_ai.gold.kpi_daily AS
SELECT event_dt, event_type, COUNT(*) AS cnt
FROM niloomid_ai.clean.events_silver
GROUP BY event_dt, event_type;

CREATE TABLE IF NOT EXISTS niloomid_ai.gold.docs_text AS
SELECT doc_id,
       CONCAT_WS('\n', COLLECT_LIST(chunk_text)) AS full_text
FROM niloomid_ai.clean.docs_chunks
GROUP BY doc_id;
```
