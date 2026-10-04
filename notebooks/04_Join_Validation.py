# Databricks notebook source
# MAGIC %md
# MAGIC # Join the hourly datasets and audit unmatched records
# MAGIC A full outer join keeps every timestamp from every source. Presence flags reveal unmatched records instead of allowing an inner join to hide them.
# MAGIC Uniqueness is checked before joining, preventing duplicate timestamps from multiplying records. We use UTC keys and block promotion if any source is absent at any hour.
# MAGIC Generation and consumption remain useful analytical context. Their contemporaneous actual values will not be forecast predictors.

# COMMAND ----------
import json
import hashlib
import re
import math
from functools import reduce
from pathlib import Path
from datetime import datetime, timedelta
from pyspark.sql import functions as F, types as T, Window
spark.conf.set("spark.sql.session.timeZone", "UTC")
dbutils.widgets.text("config_path", "/Volumes/<catalog>/<schema>/<volume>/config.json")
config_path = dbutils.widgets.get("config_path")
with open(config_path, encoding="utf-8") as stream:
    config = json.load(stream)
volume = config["output_root"]
schema = config["table_prefix"]

dbutils.widgets.text("run_id", "manual")
run_id = dbutils.widgets.get("run_id")
assert re.fullmatch(r"[A-Za-z0-9_-]+", run_id), "Unsafe run id"

manifest = spark.table(f"{schema}.pipeline_manifest").first()
assert manifest.run_id == run_id, "Data belongs to another run"
assert manifest.config_sha256 == hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest(), "Configuration changed during run"

def current_table(name):
    frame = spark.table(f"{schema}.{name}")
    ids = [r[0] for r in frame.select("_pipeline_run_id").distinct().collect()]
    assert ids == [run_id], f"Stale or mixed run IDs in {name}"
    return frame

def save_table(frame, name):
    frame.withColumn("_pipeline_run_id", F.lit(run_id)).write.format("delta").mode("overwrite").saveAsTable(f"{schema}.{name}")

def save_audit(report, name):
    save_table(spark.createDataFrame([(json.dumps(report, sort_keys=True),)], "report_json string").withColumn("checked_at", F.current_timestamp()), name)

# COMMAND ----------
def join_datasets(datasets):
    """Full join retains unmatched records for auditing instead of hiding them."""
    marked = []
    for name, df in datasets.items():
        business = [c for c in df.columns if not c.startswith("_") and c != "timestamp_end_utc"]
        marked.append(df.select(*business).withColumn(f"_{name}_present", F.lit(True)))
    joined = reduce(lambda left, right: left.join(right, "timestamp_utc", "full"), marked)
    unmatched = {name: joined.filter(F.col(f"_{name}_present").isNull()).count() for name in datasets}
    return joined, unmatched



# COMMAND ----------

quality = spark.table(f"{schema}.cleaned_quality_report").collect()
assert len(quality) == 3
assert all(json.loads(r.report_json)["passed"] and json.loads(r.report_json)["run_id"] == run_id for r in quality)
datasets = {name: current_table(f"cleaned_{name}") for name in config["datasets"]}
for name, df in datasets.items():
    assert df.filter(F.col("timestamp_utc").isNull()).count() == 0
    assert df.groupBy("timestamp_utc").count().filter("count > 1").count() == 0
joined, unmatched = join_datasets(datasets)
report = {"unmatched": unmatched, "rows": joined.count(), "passed": not any(unmatched.values()), "run_id": run_id}
save_audit(report, "join_quality_report")
assert report["passed"], "Unmatched records: inspect join_quality_report"
assert report["rows"] == 53256
save_table(joined.withColumn("timestamp_end_utc", F.col("timestamp_utc") + F.expr("INTERVAL 1 HOUR")), "cleaned_joined")
display(current_table("cleaned_joined").orderBy("timestamp_utc").limit(3))
dbutils.notebook.exit(json.dumps(report))
