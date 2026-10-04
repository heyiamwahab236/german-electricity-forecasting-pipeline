"""Generate runnable teaching notebooks and a reproducible serverless workflow."""
import json
from pathlib import Path
import yaml

base = Path(__file__).resolve().parents[1]
engine = (base / "src/smard_engineering/engineering.py").read_text(encoding="utf-8").replace("from .parsing import parse_number, parse_timestamp\n", "")
model = (base / "src/smard_engineering/modeling.py").read_text(encoding="utf-8")
parsing = (base / "src/smard_engineering/parsing.py").read_text(encoding="utf-8")
join = engine[engine.index("def join_datasets("):engine.index("def prepare_features(")]
features = engine[engine.index("def prepare_features("):engine.index("def write_delta(")]
common = '''import json
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
'''

def notebook(name, title, explanation, definitions, body):
    content = "# Databricks notebook source\n# MAGIC %md\n# MAGIC # " + title + "\n"
    content += "\n".join("# MAGIC " + line for line in explanation.splitlines())
    content += "\n\n# COMMAND ----------\n" + common + "\n# COMMAND ----------\n" + definitions + "\n# COMMAND ----------\n" + body
    (base / "notebooks" / (name + ".py")).write_text(content, encoding="utf-8")

notebook("04_Join_Validation", "Join the hourly datasets and audit unmatched records", """A full outer join keeps every timestamp from every source. Presence flags reveal unmatched records instead of allowing an inner join to hide them.
Uniqueness is checked before joining, preventing duplicate timestamps from multiplying records. We use UTC keys and block promotion if any source is absent at any hour.
Generation and consumption remain useful analytical context. Their contemporaneous actual values will not be forecast predictors.""", join, '''
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
''')

notebook("05_Feature_Preparation", "Calendar, cyclical hour and exact price lags", """Calendar features reflect German delivery time; lag arithmetic uses elapsed UTC hours.
Sine and cosine encode hour on a circle, so 23:00 and 00:00 are close. The 24/48-hour lags join on exact timestamps, rather than shifting 24/48 rows.
The first 48 rows have insufficient history. They remain in the feature table with null lags and are excluded from model evaluation; no future backfill is allowed.
The checks compare actual lag values to timestamp joins, including clock changes.""", features, '''
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
''')

notebook("06_Training_Evaluation", "Chronological XGBoost evaluation", """The earliest 60% of eligible rows form training, the next 20% validation, and the latest 20% test. A 24-hour purge at each boundary keeps training labels earlier than the first validation forecast origin, and validation labels earlier than the first test origin.
Median imputation is fitted only on training. Two XGBoost candidates are compared only on validation MAE; the selected model is evaluated once on the untouched test partition. We report euro-per-MWh errors, including a previous-day-price baseline. Tree models do not require scaling or target normalization.
Generation/consumption actuals are excluded because they would not be available for future delivery hours. The evaluation assumes historical lag prices are available at origin t-24h; these source files do not contain publication timestamps.
Modeling is bounded to 200,000 rows and runs on the Python driver after Spark feature preparation; this is not distributed XGBoost training.""", model, '''
feature_report = json.loads(current_table("feature_quality_report").first().report_json)
assert feature_report["passed"]
frame = current_table("features_hourly")
assert frame.count() <= config["model"]["max_rows"], "Driver-side modeling limit exceeded"
columns = list(dict.fromkeys(["timestamp_utc", "price_eur_mwh", "price_lag_24h", "price_lag_48h", *config["model"]["features"]]))
artifact_dir = f"{volume}/artifacts/runs/{run_id}"
report, output = evaluate(frame.select(*columns).toPandas(), config["model"], artifact_dir)
import platform
import importlib.metadata
report["environment"] = {name: importlib.metadata.version(name) for name in ["xgboost", "scikit-learn", "pandas", "numpy", "joblib"]}
report["python_version"] = platform.python_version()
report["run_id"] = run_id
report["artifact_dir"] = artifact_dir
assert report["warmup_rows_dropped"] == 48
save_audit(report, "model_evaluation")
test_predictions = spark.createDataFrame(output).withColumn("forecast_origin_utc", F.col("timestamp_utc") - F.expr("INTERVAL 24 HOURS"))
save_table(test_predictions, "predictions_test")
Path(artifact_dir, "metrics.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
print(json.dumps(report, indent=2))
display(current_table("predictions_test").orderBy("timestamp_utc").limit(10))
dbutils.notebook.exit(json.dumps(report))
''')

notebook("07_Prediction", "Predict the next 24 historical delivery hours", """The source history ends on 27 January 2025. This produces a historical demonstration for the next 24 elapsed hours, not a live forecast for today.
We use the exact same saved preprocessing and selected XGBoost model. Calendar features and lag prices are built from known history; no future actual generation, consumption or target price is required.
The training-only evaluation model stays frozen for transparency; a separately governed production model could later be refitted on all history after evaluation.""", features + "\n" + model, '''
evaluation = json.loads(current_table("model_evaluation").first().report_json)
history = current_table("cleaned_price").select("timestamp_utc", "price_eur_mwh")
last = history.agg(F.max("timestamp_utc")).first()[0]
future = spark.range(1, 25).select((F.lit(last).cast("long") + F.col("id") * 3600).cast("timestamp").alias("timestamp_utc")).withColumn("price_eur_mwh", F.lit(None).cast("double"))
prepared = prepare_features(history.unionByName(future)).filter(F.col("timestamp_utc") > F.lit(last))
pdf = prepared.select("timestamp_utc", *config["model"]["features"]).orderBy("timestamp_utc").toPandas()
assert len(pdf) == 24
output = predict(pdf, evaluation["artifact_dir"])
assert np.isfinite(output.predicted_price_eur_mwh).all()
save_table(spark.createDataFrame(output).withColumn("as_of_utc", F.lit(last)).withColumn("forecast_type", F.lit("historical_next_24h_demo")), "predictions_next24h")
output.to_csv(Path(evaluation["artifact_dir"]) / "predictions_next24h.csv", index=False)
report = {"rows": len(output), "as_of_utc": str(last), "start_utc": str(output.timestamp_utc.min()), "end_utc": str(output.timestamp_utc.max()), "run_id": run_id, "forecast_type": "historical_next_24h_demo"}
save_audit(report, "prediction_report")
display(current_table("predictions_next24h").orderBy("timestamp_utc"))
dbutils.notebook.exit(json.dumps(report))
''')

# Embed the exact source functions and tests into an executable cloud test notebook.
test_engine = (base / "tests/test_engineering.py").read_text(encoding="utf-8")
test_model = (base / "tests/test_modeling.py").read_text(encoding="utf-8")
for text_name in ("test_engine", "test_model"):
    text = locals()[text_name]
    text = "\n".join(line for line in text.splitlines() if not line.startswith("from smard_engineering") and not line.startswith("@pytest.mark.spark"))
    text = text.replace(".cache()", "")
    if text_name == "test_engine":
        test_engine = text
    else:
        test_model = text
notebook("08_Project_Tests", "Execute corrupted-data and leakage-prevention tests", """These are the same meaningful tests as the local project, executed with Databricks Spark. They cover schema drift, conversion failures, missing/duplicate hours, negative prices, explicit nullable exceptions, unmatched joins, exact lags over gaps, DST ambiguity, chronological splitting and training-only imputation.
Corrupted fixtures are separate from original sources. Synthetic model test errors are not project performance metrics.""", parsing + "\n" + engine + "\n" + model + "\n" + test_engine + "\n" + test_model, '''
test_root = Path(volume) / "test-artifacts" / run_id
test_root.mkdir(parents=True, exist_ok=True)
__file__ = str(Path(volume) / "project/tests/test_engineering.py")
cases = [
    ("corrupted_csv", lambda: test_corrupted_csv_quality_gate(spark)),
    ("schema_drift", lambda: test_schema_drift_fails(spark, test_root)),
    ("exact_lags_over_gap", lambda: test_exact_lags_do_not_shift_over_a_missing_hour(spark)),
    ("unmatched_join", lambda: test_full_join_reports_unmatched(spark)),
    ("dst_pair_and_lone_ambiguity", lambda: test_ordered_dst_pair_and_unexplained_duplicates(spark)),
    ("negative_price_nullable_contract", lambda: test_negative_price_passes_and_nullable_is_explicit(spark)),
    ("chronological_duplicates", test_split_orders_rows_and_rejects_duplicate_times),
    ("train_only_imputer_and_model_reload", lambda: test_imputer_fit_uses_training_only_and_saved_model_predicts(test_root / "imputer")),
    ("warmup_no_backfill", lambda: test_warmup_dropped_instead_of_future_backfill(test_root / "warmup")),
    ("purged_forecast_boundaries", test_purged_boundaries_respect_forecast_origin),
]
results = {}
for name, case in cases:
    case()
    results[name] = "passed"
    print(name, "passed")
report = {"cases": results, "passed": True, "run_id": run_id}
save_audit(report, "project_test_results")
dbutils.notebook.exit(json.dumps(report))
''')

stages = [
    ("ingestion", "02_Raw_Ingestion"),
    ("validation", "03_Cleaned_Validation"),
    ("joins", "04_Join_Validation"),
    ("features", "05_Feature_Preparation"),
    ("training_evaluation", "06_Training_Evaluation"),
    ("prediction", "07_Prediction"),
    ("tests", "08_Project_Tests"),
]
tasks = []
for index, (key, name) in enumerate(stages):
    task = dict(task_key=key, notebook_task=dict(notebook_path="${var.notebook_root}/" + name, source="WORKSPACE", base_parameters={"run_id": "{{job.run_id}}", "config_path": "${var.config_path}"}), timeout_seconds=1200)
    if index:
        task["depends_on"] = [{"task_key": stages[index - 1][0]}]
    if index >= 4:
        task["environment_key"] = "modeling"
    tasks.append(task)
settings = dict(name="German Electricity Forecasting Pipeline", description="Original SMARD CSVs to validated raw, cleaned and feature Delta tables, chronological XGBoost evaluation and historical next-24-hour predictions. Includes corrupted-data tests.", max_concurrent_runs=1, timeout_seconds=5400, tasks=tasks,
    environments=[dict(environment_key="modeling", spec=dict(environment_version="2", dependencies=["xgboost==2.1.4", "scikit-learn==1.5.2", "pandas==2.2.3", "numpy==1.26.4", "joblib==1.4.2", "pytest==8.3.5"]))])
bundle = dict(bundle={"name": "german-electricity-forecasting-pipeline"}, variables={"notebook_root": {"description": "Folder containing imported learning notebooks", "default": "/Users/${workspace.current_user.userName}/Electricity Portfolio"}, "config_path": {"description": "Absolute Volume path to your configured config.json"}}, resources={"jobs": {"smard_pipeline": settings}}, targets={"dev": {"default": True, "mode": "development", "workspace": {}}})
(base / "databricks.yml").write_text(yaml.safe_dump(bundle, sort_keys=False), encoding="utf-8")
print("Built four stages, tests notebook and serverless bundle")
