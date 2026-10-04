# Databricks notebook source
# MAGIC %md
# MAGIC # Electricity project: setup check
# MAGIC This notebook checks access to the original SMARD files. It does not train a model or replace the original notebook.
# MAGIC
# MAGIC **Catalogs** group data resources, **schemas** separate projects, and **Volumes** store files.
# MAGIC PySpark reads the CSVs into DataFrames: tables of rows and columns that Spark can process across workers.

# COMMAND ----------
from pyspark.sql import functions as F
import hashlib
import json

dbutils.widgets.text("config_path", "/Volumes/<catalog>/<schema>/<volume>/config.json")
config_path = dbutils.widgets.get("config_path")
with open(config_path, encoding="utf-8") as stream:
    config = json.load(stream)
volume = config["output_root"]
schema = config["table_prefix"]
source = volume + "/source"
files = {
    "generation": "Actual_generation_201901010000_202501280000_Hour.csv",
    "consumption": "Actual_consumption_201901010000_202501280000_Hour.csv",
    "price": "Day-ahead_prices_201901010000_202501280000_Hour.csv",
}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Read the original values
# MAGIC `header=True` uses the first line for column names. The separator is a semicolon. We leave schema inference off so the raw values remain strings.
# MAGIC `count()` actually asks Spark to read the data. A DataFrame definition alone does not execute all processing: Spark is lazy.

# COMMAND ----------
results = {}
for dataset, filename in files.items():
    path = f"{source}/{filename}"
    frame = spark.read.option("header", True).option("sep", ";").option("mode", "FAILFAST").csv(path)
    with open(path, "rb") as original:
        digest = hashlib.sha256(original.read()).hexdigest()
    results[dataset] = {"rows": frame.count(), "columns": len(frame.columns), "sha256": digest}
    if dataset == "price":
        price_column = next(c for c in frame.columns if c.startswith("Germany/Luxembourg"))
        results[dataset]["negative_price_rows"] = frame.filter(F.regexp_replace(F.col(price_column), ",", "").cast("double") < 0).count()
    print(dataset, results[dataset])
    display(frame.limit(3))

# COMMAND ----------
# MAGIC %md
# MAGIC ## What this verifies
# MAGIC A successful run proves serverless Spark can read the uploaded CSVs and access Volume files. SHA-256 checksums allow comparison with local originals.
# MAGIC Negative prices are legitimate electricity-market observations; they must not be removed just because they are below zero.
# MAGIC This is only the setup check. Timestamp normalization, Delta layers, quality gates, forecasting and their tests are separate project stages.

# COMMAND ----------
dbutils.notebook.exit(json.dumps(results, sort_keys=True))
