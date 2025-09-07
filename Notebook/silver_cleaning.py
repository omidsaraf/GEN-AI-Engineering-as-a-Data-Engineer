```python
from pyspark.sql import functions as F
src = spark.table("niloomid_ai.raw.events_bronze")
clean = (src.dropDuplicates(["event_id"])
            .filter("event_type is not null")
            .withColumn("event_dt", F.to_date("event_ts")))
clean.write.mode("overwrite").saveAsTable("niloomid_ai.clean.events_silver")
```
