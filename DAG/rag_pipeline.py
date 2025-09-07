
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
              "notebook_task": {"notebook_path": "/Repos/niloomid/20_silver_cleaning.py"}}
    )
    gold = DatabricksSubmitRunOperator(
        task_id="gold",
        json={"new_cluster": new_cluster,
              "notebook_task": {"notebook_path": "/Repos/niloomid/30_gold_kpis.sql"}}
    )
    embed = DatabricksSubmitRunOperator(
        task_id="embed",
        json={"new_cluster": new_cluster,
              "notebook_task": {"notebook_path": "/Repos/niloomid/embed_index.py"}}
    )
    silver >> gold >> embed
```
