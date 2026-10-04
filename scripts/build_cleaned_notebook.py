"""Produce a self-contained learning notebook from the same tested project functions.

Embedding definitions keeps the learning stage runnable without installing a wheel.
The source functions remain the authoritative implementation.
"""
import json
from pathlib import Path

base = Path(__file__).resolve().parents[1]
parsing = (base / "src/smard_engineering/parsing.py").read_text(encoding="utf-8")
engineering = (base / "src/smard_engineering/engineering.py").read_text(encoding="utf-8")
engineering = engineering.replace("from .parsing import parse_number, parse_timestamp\n", "")
transforms = engineering[engineering.index("def column("):engineering.index("def read_csv(")]
transforms += engineering[engineering.index("def normalize("):engineering.index("def join_datasets(")]
header = '''# Databricks notebook source
# MAGIC %md
# MAGIC # Cleaned layer: parsing, UTC timestamps and quality gates
# MAGIC This stage reads the raw Delta tables. It preserves all 53,256 delivery hours per dataset and creates typed cleaned tables only after validation succeeds.
# MAGIC
# MAGIC **Numeric parsing:** the original exports use English separators, so `4,815.25` becomes `4815.25`. A missing marker `-` becomes null; malformed numbers get an explicit error. Negative electricity prices and residual loads are legitimate observations.
# MAGIC
# MAGIC **Timestamps:** delivery times are in Europe/Berlin; storage and interval checks use UTC. The two autumn 02:00 rows are distinguished using their preserved source order, with the first assigned the earlier UTC instant. This assumption is checked against continuous source chronology. Lone or extra ambiguous occurrences fail validation. Spring times that do not exist are rejected.
# MAGIC
# MAGIC **Nuclear null policy:** nuclear output has 8,724 missing values in the source. We preserve these as nulls and report them through an explicit nullable contract. They are not silently changed into zero or used as model predictors.

# COMMAND ----------
import json
import hashlib
import re
from datetime import timedelta
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
assert manifest.run_id == run_id, "Raw data belongs to a different run"
assert manifest.config_sha256 == hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest(), "Configuration changed during run"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parsing functions
# MAGIC The following functions are shared with the local project's unit tests. Each parser returns both a value and a possible error, so a failed conversion cannot disappear into an unexplained null.

# COMMAND ----------
'''
checks = '''
# COMMAND ----------
# MAGIC %md
# MAGIC ## Deliberately corrupted sample: verify the gate detects failures
# MAGIC This test contains one duplicate hour, one missing hour, a malformed number, a missing price and an invalid date. Its valid negative price must survive parsing.

# COMMAND ----------
test_spec = dict(timestamp_columns=["Start date"], timestamp_format="%b %d, %Y %I:%M %p", locale="en", numeric_columns={"Price": "price_eur_mwh"}, nonnegative=[])
corrupt = spark.createDataFrame([
    ("Jan 1, 2024 12:00 AM", "-15.00"),
    ("Jan 1, 2024 1:00 AM", "20.00"),
    ("Jan 1, 2024 1:00 AM", "21.00"),
    ("Jan 1, 2024 3:00 AM", "broken"),
    ("Jan 1, 2024 4:00 AM", "-"),
    ("not-a-date", "25.00"),
], "`Start date` string, Price string")
corrupt_report = quality_report(normalize(corrupt, test_spec), test_spec)
assert not corrupt_report["passed"]
assert corrupt_report["duplicate_extra_rows"] == 1
assert corrupt_report["missing_hourly_intervals"] == 1
assert corrupt_report["nulls"]["price_eur_mwh"] == 2
assert corrupt_report["conversion_errors"] == {"price_eur_mwh:invalid_number": 1, "price_eur_mwh:missing": 1, "invalid_timestamp": 1}
assert corrupt_report["observed_negatives"]["price_eur_mwh"] == 1
assert parse_timestamp("2024-03-31 02:00", "%Y-%m-%d %H:%M")[1] == "nonexistent_local_time"
assert parse_timestamp("2024-10-27 02:00", "%Y-%m-%d %H:%M")[1] == "ambiguous_local_time"
print("Corrupted sample assertions passed", json.dumps(corrupt_report))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Normalize the original raw tables
# MAGIC The raw header mapping restores original column names for the shared parser. Results are materialized in staging Delta tables to avoid rerunning Python parsing for every check. No cleaned table is promoted unless every dataset passes.

# COMMAND ----------
reports = {}
for dataset, spec in config["datasets"].items():
    raw = spark.table(f"{schema}.raw_{dataset}")
    assert raw.filter(F.col("_pipeline_run_id") != run_id).count() == 0, "Stale raw data"
    original_columns = spec["expected_columns"]
    mapping_values = raw.select("_original_headers_json").distinct().collect()
    assert len(mapping_values) == 1, "Inconsistent raw schema metadata"
    mapping = json.loads(mapping_values[0][0])
    assert list(mapping.values()) == original_columns, "Raw schema drift detected"
    assert set(c for c in raw.columns if not c.startswith("_")) == set(mapping), "Unexpected raw columns"
    restored = raw.select(*[F.col(safe).alias(original) for safe, original in mapping.items()], "_source_file", "_source_row", "_ingested_at")
    normalized = normalize(restored, spec).withColumn("timestamp_end_utc", F.col("timestamp_utc") + F.expr("INTERVAL 1 HOUR")).withColumn("_pipeline_run_id", F.lit(run_id))
    # End timestamps describe an hourly interval inferred from the export resolution;
    # source End date labels remain preserved in the raw tables.
    stage_table = f"{schema}.staging_cleaned_{dataset}"
    normalized.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(stage_table)
    candidate = spark.table(stage_table)
    report = quality_report(candidate, spec)
    order = Window.partitionBy("_source_file").orderBy("_source_row")
    chronology = candidate.withColumn("_previous_epoch", F.lag(F.col("timestamp_utc").cast("long")).over(order))
    report["source_chronology_failures"] = chronology.filter(F.col("_previous_epoch").isNotNull() & ((F.col("timestamp_utc").cast("long") - F.col("_previous_epoch")) != 3600)).count()
    report["passed"] = report["passed"] and report["source_chronology_failures"] == 0
    report["nullable_contract"] = spec.get("nullable", [])
    report["run_id"] = run_id
    reports[dataset] = report
    print(dataset, json.dumps(report, indent=2))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Persist the audit and promote passed datasets
# MAGIC Checks cover duplicate UTC timestamps, missing hourly intervals, off-hour timestamps, nulls, failed conversions, forbidden negative values and source chronology.
# MAGIC Audit records remain available even when validation fails. The staging records with conversion issues are retained for investigation. Expected nuclear nulls are included in that audit; they are not fatal.
# MAGIC The three-table promotion is gated as a group, but Delta transactions are per table, not one cross-table transaction.

# COMMAND ----------
audit = spark.createDataFrame([(name, json.dumps(report, sort_keys=True)) for name, report in reports.items()], "dataset string, report_json string").withColumn("checked_at", F.current_timestamp())
audit.write.format("delta").mode("overwrite").saveAsTable(f"{schema}.cleaned_quality_report")
for dataset in reports:
    spark.table(f"{schema}.staging_cleaned_{dataset}").filter(F.size("_errors") > 0).write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{schema}.cleaned_conversion_audit_{dataset}")
assert all(report["passed"] for report in reports.values()), "Quality gate failed: inspect cleaned_quality_report and staging tables"
for dataset in reports:
    spark.table(f"{schema}.staging_cleaned_{dataset}").write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{schema}.cleaned_{dataset}")
    saved = spark.table(f"{schema}.cleaned_{dataset}")
    assert saved.count() == reports[dataset]["rows"]
    assert isinstance(saved.schema["timestamp_utc"].dataType, T.TimestampType)
    for numeric in config["datasets"][dataset]["numeric_columns"].values():
        assert isinstance(saved.schema[numeric].dataType, T.DoubleType)
    display(saved.orderBy("timestamp_utc").limit(3))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Review before the next stage
# MAGIC Find `cleaned_generation`, `cleaned_consumption`, `cleaned_price` and `cleaned_quality_report` in Catalog.
# MAGIC Joining the three datasets and checking unmatched timestamps is the next workflow stage.

# COMMAND ----------
dbutils.notebook.exit(json.dumps({"datasets": reports, "corrupted_sample_tests": "passed", "stage": "cleaned_only"}, sort_keys=True))
'''
destination = base / "notebooks/03_Cleaned_Validation.py"
destination.write_text(header + parsing + "\n# COMMAND ----------\n" + transforms + checks, encoding="utf-8")
print(destination)
