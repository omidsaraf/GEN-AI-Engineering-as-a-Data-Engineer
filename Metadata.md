**Batch / Job / Proc / Proc‑Param (illustrative DDL)**

```sql
CREATE TABLE IF NOT EXISTS meta.batch (
  batch_nm STRING, framework STRING, dag_nm STRING, schedule STRING
);
CREATE TABLE IF NOT EXISTS meta.job (
  job_nm STRING, batch_nm STRING, layer STRING, module STRING, class STRING
);
CREATE TABLE IF NOT EXISTS meta.proc (
  proc_nm STRING, job_nm STRING, lower_bound_ts TIMESTAMP, high_val_ts TIMESTAMP
);
CREATE TABLE IF NOT EXISTS meta.proc_param (
  proc_nm STRING, parm_key STRING, parm_val STRING
);
```

