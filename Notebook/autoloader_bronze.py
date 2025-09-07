
```python
from pyspark.sql.functions import col, input_file_name, current_timestamp
spark.conf.set("cloudFiles.inferColumnTypes", "true")
(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", "/mnt/lake/_schemas/events")
  .option("cloudFiles.maxFilesPerTrigger", 1000)
  .load("/mnt/lake/landing/events/")
  .withColumn("src_file", input_file_name())
  .withColumn("ingest_ts", current_timestamp())
  .writeStream
  .option("checkpointLocation", "/mnt/lake/_chk/bronze/events")
  .toTable("niloomid_ai.raw.events_bronze"))
```
