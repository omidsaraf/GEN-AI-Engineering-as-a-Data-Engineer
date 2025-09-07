```sql
CREATE OR REPLACE VIEW niloomid_{env}.gold.events_masked AS
SELECT event_id,
       event_ts,
       event_type,
       CASE WHEN is_member('data_analyst_pii') THEN content ELSE substr(sha2(content,256),1,16) END AS content
FROM niloomid_{env}.clean.events_silver;
```
