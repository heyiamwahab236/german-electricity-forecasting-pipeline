# Databricks notebook source
# MAGIC %md
# MAGIC # Raw ingestion: original SMARD CSVs to Delta Lake
# MAGIC **Goal:** create `raw_generation`, `raw_consumption`, and `raw_price` in the configured catalog and schema.
# MAGIC Raw means original business values remain strings. We add source metadata and preserve a mapping to the original CSV headers.
# MAGIC
# MAGIC **Why preserve source row numbers?** Your SMARD exports repeat the 02:00 delivery hour when daylight saving ends. A Spark table has no inherent row order. An explicit source record number lets the next stage distinguish the first occurrence from the second without deleting real observations.
# MAGIC
# MAGIC Spark distributes files and parses each export as a bounded file. This historical-export approach has a 64 MiB per-file limit; it is not intended for multi-GB streaming files.

# COMMAND ----------
import csv
import io
import json
import re
import hashlib
from pyspark.sql import functions as F, types as T

dbutils.widgets.text("config_path", "/Volumes/<catalog>/<schema>/<volume>/config.json")
config_path = dbutils.widgets.get("config_path")
with open(config_path, encoding="utf-8") as stream:
    config = json.load(stream)
volume = config["output_root"]
schema = config["table_prefix"]


dbutils.widgets.text("run_id", "manual")
run_id = dbutils.widgets.get("run_id")
assert re.fullmatch(r"[A-Za-z0-9_-]+", run_id), "Unsafe run id"
config_hash = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Read records and check the CSV structure
# MAGIC We compare headers with the inspected original schema. Renamed, added, removed, or reordered columns stop ingestion rather than silently changing the data.
# MAGIC Numeric values, missing-value markers, and timestamps are kept as text. Parsing them belongs in the cleaned layer.

# COMMAND ----------
def read_raw(spec):
    original_headers = spec["expected_columns"]
    safe_headers = [re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_") for name in original_headers]
    if len(safe_headers) != len(set(safe_headers)):
        raise ValueError("Column-name collision after normalization")
    header_mapping = dict(zip(safe_headers, original_headers))
    record_schema = T.StructType(
        [T.StructField(name, T.StringType(), True) for name in safe_headers]
        + [T.StructField("_source_row", T.LongType(), False)]
    )

    def parse_file(content):
        if len(content) > 64 * 1024 * 1024:
            raise ValueError("Historical CSV exceeds the 64 MiB per-file limit")
        reader = csv.reader(io.StringIO(bytes(content).decode("utf-8-sig")), delimiter=";", strict=True)
        if next(reader, []) != original_headers:
            raise ValueError("Schema drift: CSV headers differ from the original contract")
        records = []
        for source_row, values in enumerate(reader, start=2):
            if len(values) != len(original_headers):
                raise ValueError(f"Malformed CSV record at source row {source_row}")
            records.append(tuple(values) + (source_row,))
        return records

    parse_udf = F.udf(parse_file, T.ArrayType(record_schema))
    source_files = spark.read.format("binaryFile").load(spec["path"])
    raw = source_files.select(
        F.col("path").alias("_source_file"),
        F.explode(parse_udf("content")).alias("record")
    ).select("record.*", "_source_file")
    return (
        raw.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_pipeline_run_id", F.lit(run_id))
        .withColumn("_original_headers_json", F.lit(json.dumps(header_mapping, ensure_ascii=False)))
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Write Delta tables
# MAGIC A DataFrame is Spark's table-like representation of data. Spark delays much of the work until an action, such as a write or count.
# MAGIC `format("delta")` requests transactional Delta storage. `saveAsTable` registers a managed table in Unity Catalog.
# MAGIC `overwrite` replaces this project's raw snapshots on a rerun. It does not modify the original CSVs or notebooks.

# COMMAND ----------
results = {}
for dataset, spec in config["datasets"].items():
    raw = read_raw(spec)
    table = f"{schema}.raw_{dataset}"
    raw.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(table)
    saved = spark.table(table)
    rows = saved.count()
    unique_source_records = saved.select("_source_file", "_source_row").distinct().count()
    assert rows == 53256, f"Unexpected number of rows in {dataset}: {rows}"
    assert unique_source_records == rows, f"Duplicate source record identifiers in {dataset}"
    business_columns = [c for c in saved.columns if not c.startswith("_")]
    assert all(isinstance(saved.schema[c].dataType, T.StringType) for c in business_columns)
    assert saved.filter(F.col("_ingested_at").isNull() | F.col("_source_file").isNull()).count() == 0
    details = spark.sql(f"DESCRIBE DETAIL {table}").first()
    assert details["format"] == "delta"
    results[dataset] = {
        "table": table, "rows": rows, "business_columns": len(business_columns),
        "unique_source_records": unique_source_records, "format": details["format"]
    }
    if dataset == "price":
        target = next(c for c in business_columns if c.startswith("germany_luxembourg"))
        # Diagnostic only: the persisted prices are still the original strings.
        results[dataset]["negative_price_rows"] = saved.filter(F.regexp_replace(F.col(target), ",", "").cast("double") < 0).count()
    print(json.dumps(results[dataset], indent=2))
    display(saved.orderBy("_source_file", "_source_row").limit(3))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Ingestion guarantees
# MAGIC The raw layer preserves source values and record identifiers for daylight-saving normalization. Schema contracts and row counts are checked before cleaning. Delta provides transactional storage; Unity Catalog registers the tables.
# MAGIC
# MAGIC These checks verify ingestion, not business-data quality or forecast accuracy. Numeric parsing, UTC normalization, duplicate-hour checks, joins and model evaluation are the next stages.

# COMMAND ----------
manifest = spark.createDataFrame([(run_id, config_hash, json.dumps(config, sort_keys=True))], "run_id string, config_sha256 string, config_json string").withColumn("ingested_at", F.current_timestamp())
manifest.write.format("delta").mode("overwrite").saveAsTable(f"{schema}.pipeline_manifest")
dbutils.notebook.exit(json.dumps(results, sort_keys=True))
