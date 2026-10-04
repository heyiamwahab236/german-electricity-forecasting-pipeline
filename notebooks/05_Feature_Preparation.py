# Databricks notebook source
# MAGIC %md
# MAGIC # Calendar, cyclical hour and exact price lags
# MAGIC Calendar features reflect German delivery time; lag arithmetic uses elapsed UTC hours.
# MAGIC Sine and cosine encode hour on a circle, so 23:00 and 00:00 are close. The 24/48-hour lags join on exact timestamps, rather than shifting 24/48 rows.
# MAGIC The first 48 rows have insufficient history. They remain in the feature table with null lags and are excluded from model evaluation; no future backfill is allowed.
# MAGIC The checks compare actual lag values to timestamp joins, including clock changes.

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
def prepare_features(joined):
    # All interval math uses UTC; calendar reflects the German delivery hour.
    df = joined.withColumn("_local_time", F.from_utc_timestamp("timestamp_utc", "Europe/Berlin"))
    df = (df.withColumn("hour", F.hour("_local_time"))
          .withColumn("day_of_week", F.pmod(F.dayofweek("_local_time") + F.lit(5), F.lit(7)))
          .withColumn("month", F.month("_local_time"))
          .withColumn("is_weekend", (F.col("day_of_week") >= 5).cast("int"))
          .withColumn("hour_sin", F.sin(F.col("hour") * 2 * math.pi / 24))
          .withColumn("hour_cos", F.cos(F.col("hour") * 2 * math.pi / 24)))
    for hours in (24, 48):
        lag = joined.select((F.col("timestamp_utc") + F.expr(f"INTERVAL {hours} HOURS")).alias("timestamp_utc"),
                            F.col("price_eur_mwh").alias(f"price_lag_{hours}h"))
        df = df.join(lag, "timestamp_utc", "left")
    return df.drop("_local_time")



# COMMAND ----------

joined = current_table("cleaned_joined")
frame = prepare_features(joined)
save_table(frame, "features_hourly")
saved = current_table("features_hourly")
last = joined.agg(F.max("timestamp_utc")).first()[0]
report = {"rows": saved.count(), "missing_24h_lags": saved.filter(F.col("price_lag_24h").isNull()).count(), "missing_48h_lags": saved.filter(F.col("price_lag_48h").isNull()).count(), "run_id": run_id}
for hours in (24, 48):
    expected = joined.select((F.col("timestamp_utc") + F.expr(f"INTERVAL {hours} HOURS")).alias("timestamp_utc"), F.col("price_eur_mwh").alias("expected_lag"))
    comparison = saved.join(expected, "timestamp_utc", "left")
    failures = comparison.filter(~F.col(f"price_lag_{hours}h").eqNullSafe(F.col("expected_lag"))).count()
    report[f"lag_{hours}h_mismatches"] = failures
    assert failures == 0
norm_failures = saved.filter(F.abs(F.pow("hour_sin", 2) + F.pow("hour_cos", 2) - 1) > 1e-10).count()
assert norm_failures == 0
assert report["missing_24h_lags"] == 24 and report["missing_48h_lags"] == 48
assert saved.filter((F.col("timestamp_utc") >= F.lit(last) - F.expr("INTERVAL 24 HOURS")) & (F.col("price_lag_24h").isNull() | F.col("price_lag_48h").isNull())).count() == 0
report["passed"] = True
save_audit(report, "feature_quality_report")
display(saved.orderBy("timestamp_utc").limit(5))
dbutils.notebook.exit(json.dumps(report))
