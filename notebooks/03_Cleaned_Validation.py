# Databricks notebook source
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
"""Strict parsing shared by Spark UDFs and local unit tests."""
import math
import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

MISSING = {"", "-", "—", "n/a", "na", "null"}


def parse_number(value, locale="de"):
    if value is None or str(value).strip().lower() in MISSING:
        return None, "missing"
    value = str(value).strip().replace("\u00a0", "").replace(" ", "")
    patterns = {
        "de": r"[+-]?(?:\d+|\d{1,3}(?:\.\d{3})+)(?:,\d+)?",
        "en": r"[+-]?(?:\d+|\d{1,3}(?:,\d{3})+)(?:\.\d+)?",
    }
    if locale not in patterns:
        raise ValueError("locale must be de or en")
    if not re.fullmatch(patterns[locale], value):
        return None, "invalid_number"
    normalized = value.replace(".", "").replace(",", ".") if locale == "de" else value.replace(",", "")
    result = float(normalized)
    return (result, None) if math.isfinite(result) else (None, "invalid_number")


def parse_timestamp(value, fmt, zone="Europe/Berlin", fold=None):
    """Return UTC epoch seconds. Never guess a DST fold or repair a nonexistent time."""
    if value is None or not str(value).strip():
        return None, "missing_timestamp"
    try:
        dt = datetime.strptime(str(value).strip(), fmt)
    except ValueError:
        return None, "invalid_timestamp"
    if dt.tzinfo is not None:
        return int(dt.timestamp()), None
    local_zone = ZoneInfo(zone)
    instants = set()
    for candidate_fold in (0, 1):
        candidate = dt.replace(tzinfo=local_zone, fold=candidate_fold)
        utc = candidate.astimezone(timezone.utc)
        if utc.astimezone(local_zone).replace(tzinfo=None) == dt:
            instants.add(int(utc.timestamp()))
    if not instants:
        return None, "nonexistent_local_time"
    if len(instants) > 1:
        if fold in (0, 1):
            return int(dt.replace(tzinfo=local_zone, fold=fold).timestamp()), None
        return None, "ambiguous_local_time"
    return instants.pop(), None

# COMMAND ----------
def column(name):
    return F.col("`" + name.replace("`", "``") + "`")


def normalize(raw, spec):
    numeric_type = T.StructType([T.StructField("value", T.DoubleType()), T.StructField("error", T.StringType())])
    time_type = T.StructType([T.StructField("epoch", T.LongType()), T.StructField("error", T.StringType())])
    locale = spec.get("locale", "de")
    fmt, zone = spec["timestamp_format"], spec.get("timezone", "Europe/Berlin")
    number_udf = F.udf(lambda value: parse_number(value, locale), numeric_type)
    time_udf = F.udf(lambda value: parse_timestamp(value, fmt, zone), time_type)
    # concat (not concat_ws) deliberately propagates a null timestamp component.
    timestamp_parts = []
    for i, name in enumerate(spec["timestamp_columns"]):
        if i:
            timestamp_parts.append(F.lit(" "))
        timestamp_parts.append(column(name))
    df = raw.withColumn("_parsed_time", time_udf(F.concat(*timestamp_parts)))
    if spec.get("dst_policy") == "ordered_pair":
        if not {"_source_file", "_source_row"} <= set(raw.columns):
            raise ValueError("ordered_pair requires stable source file and row metadata")
        df = df.withColumn("_wall_time", F.concat(*timestamp_parts))
        window = Window.partitionBy("_source_file", "_wall_time")
        df = df.withColumn("_pair_count", F.count("_wall_time").over(window))
        df = df.withColumn("_first_row", F.min("_source_row").over(window))
        fold_udf = F.udf(lambda value, fold: parse_timestamp(value, fmt, zone, fold), time_type)
        df = df.withColumn("_parsed_time", F.when((F.col("_parsed_time.error") == "ambiguous_local_time") & (F.col("_pair_count") == 2),
                           fold_udf("_wall_time", F.when(F.col("_source_row") == F.col("_first_row"), F.lit(0)).otherwise(F.lit(1))))
                           .otherwise(F.col("_parsed_time")))
    df = df.withColumn("timestamp_utc", F.col("_parsed_time.epoch").cast("timestamp"))
    errors = [F.col("_parsed_time.error")]
    for i, (source, alias) in enumerate(spec["numeric_columns"].items()):
        parsed = f"_parsed_{i}"
        df = df.withColumn(parsed, number_udf(column(source))).withColumn(alias, F.col(f"{parsed}.value"))
        errors.append(F.when(F.col(f"{parsed}.error").isNotNull(), F.concat(F.lit(alias + ":"), F.col(f"{parsed}.error"))))
    df = df.withColumn("_errors", F.filter(F.array(*errors), lambda x: x.isNotNull()))
    selected = ["timestamp_utc", *spec["numeric_columns"].values(), "_errors"]
    selected += [c for c in ("_source_file", "_source_row", "_ingested_at") if c in df.columns]
    return df.select(*selected)


def quality_report(df, spec):
    numeric = list(spec["numeric_columns"].values())
    null_columns = ["timestamp_utc", *numeric]
    nonnegative = spec.get("nonnegative", [])
    aggregates = [F.count("*").alias("rows"), F.min("timestamp_utc").alias("min_time"), F.max("timestamp_utc").alias("max_time"),
                  F.sum(F.when((F.col("timestamp_utc").cast("long") % 3600) != 0, 1).otherwise(0)).alias("off_hour")]
    aggregates += [F.sum(F.when(F.col(name).isNull(), 1).otherwise(0)).alias("null_" + name) for name in null_columns]
    aggregates += [F.sum(F.when(F.col(name) < 0, 1).otherwise(0)).alias("negative_" + name) for name in numeric]
    stats = df.agg(*aggregates).first().asDict()
    total = stats["rows"]
    valid_times = df.filter(F.col("timestamp_utc").isNotNull())
    duplicates = valid_times.groupBy("timestamp_utc").count().filter("count > 1")
    duplicate_extra = duplicates.select(F.sum(F.col("count") - 1)).first()[0] or 0
    bounds = stats["min_time"], stats["max_time"]
    missing = 0
    if bounds[0] is not None:
        expected = df.sparkSession.range(1).select(F.explode(F.sequence(F.lit(bounds[0]), F.lit(bounds[1]), F.expr("INTERVAL 1 HOUR"))).alias("timestamp_utc"))
        missing = expected.join(valid_times.select("timestamp_utc").distinct(), "timestamp_utc", "left_anti").count()
    errors = {r["error"]: r["count"] for r in df.select(F.explode("_errors").alias("error")).groupBy("error").count().collect()}
    nulls = {name: int(stats["null_" + name] or 0) for name in null_columns}
    negatives = {name: int(stats["negative_" + name] or 0) for name in nonnegative}
    observed_negatives = {name: int(stats["negative_" + name] or 0) for name in numeric}
    off_hour = int(stats["off_hour"] or 0)
    report = dict(rows=total, duplicate_extra_rows=int(duplicate_extra), missing_hourly_intervals=missing,
                  off_hour_rows=off_hour, nulls=nulls, conversion_errors=errors, forbidden_negatives=negatives,
                  observed_negatives=observed_negatives, start_utc=str(bounds[0]), end_utc=str(bounds[1]))
    nullable = set(spec.get("nullable", []))
    blocking_errors = {k: v for k, v in errors.items() if k not in {name + ":missing" for name in nullable}}
    report["passed"] = bool(total and not duplicate_extra and not missing and not off_hour and not any(v for k, v in nulls.items() if k not in nullable) and not blocking_errors and not any(negatives.values()))
    return report



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
