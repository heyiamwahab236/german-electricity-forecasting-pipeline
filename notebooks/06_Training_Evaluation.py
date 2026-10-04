# Databricks notebook source
# MAGIC %md
# MAGIC # Chronological XGBoost evaluation
# MAGIC The earliest 60% of eligible rows form training, the next 20% validation, and the latest 20% test. A 24-hour purge at each boundary keeps training labels earlier than the first validation forecast origin, and validation labels earlier than the first test origin.
# MAGIC Median imputation is fitted only on training. Two XGBoost candidates are compared only on validation MAE; the selected model is evaluated once on the untouched test partition. We report euro-per-MWh errors, including a previous-day-price baseline. Tree models do not require scaling or target normalization.
# MAGIC Generation/consumption actuals are excluded because they would not be available for future delivery hours. The evaluation assumes historical lag prices are available at origin t-24h; these source files do not contain publication timestamps.
# MAGIC Modeling is bounded to 200,000 rows and runs on the Python driver after Spark feature preparation; this is not distributed XGBoost training.

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
"""Bounded driver-side XGBoost. No preprocessing sees validation or test rows."""
import json
from pathlib import Path
import joblib
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.pipeline import Pipeline
from xgboost import XGBRegressor


def chronological_split(frame, train_fraction=0.6, validation_fraction=0.2, purge_hours=0):
    if not (0 < train_fraction < 1 and 0 < validation_fraction < 1 and train_fraction + validation_fraction < 1):
        raise ValueError("Invalid chronological split fractions")
    frame = frame.sort_values("timestamp_utc").reset_index(drop=True)
    if frame.timestamp_utc.isna().any() or frame.timestamp_utc.duplicated().any():
        raise ValueError("Split requires non-null unique timestamps")
    a, b = int(len(frame) * train_fraction), int(len(frame) * (train_fraction + validation_fraction))
    parts = frame.iloc[:a].copy(), frame.iloc[a:b].copy(), frame.iloc[b:].copy()
    if purge_hours < 0:
        raise ValueError("Purge horizon must be nonnegative")
    if purge_hours:
        train, validation, test = parts
        train = train[train.timestamp_utc < validation.timestamp_utc.min() - pd.Timedelta(hours=purge_hours)]
        validation = validation[validation.timestamp_utc < test.timestamp_utc.min() - pd.Timedelta(hours=purge_hours)]
        parts = train, validation, test
    if any(len(part) < 2 for part in parts):
        raise ValueError("At least two rows required per chronological partition")
    return parts


def metrics(y, prediction):
    return {"mae_eur_mwh": float(mean_absolute_error(y, prediction)),
            "rmse_eur_mwh": float(np.sqrt(mean_squared_error(y, prediction)))}


def evaluate(frame, spec, artifact_dir):
    features = spec["features"]
    if "price_eur_mwh" in features:
        raise ValueError("Target cannot be a predictor")
    if frame.price_eur_mwh.isna().any():
        raise ValueError("Null targets cannot be scored")
    # Initial lag warm-up has no previous observations; do not invent them.
    input_rows = len(frame)
    frame = frame.dropna(subset=["price_lag_24h", "price_lag_48h"])
    purge_hours = spec.get("purge_hours", 24)
    train, validation, test = chronological_split(frame, spec.get("train_fraction", .6), spec.get("validation_fraction", .2), purge_hours)
    if any(train[c].isna().all() for c in features):
        raise ValueError("All-null training feature")
    candidates = spec.get("candidates", [{"n_estimators": 100, "max_depth": 3}])
    if not candidates:
        raise ValueError("At least one model candidate required")
    fitted = []
    for params in candidates:
        model = Pipeline([("imputer", SimpleImputer(strategy="median", add_indicator=True)),
                          ("xgb", XGBRegressor(objective="reg:squarederror", random_state=42, n_jobs=2,
                                                learning_rate=.05, **params))])
        model.fit(train[features], train.price_eur_mwh)
        fitted.append((metrics(validation.price_eur_mwh, model.predict(validation[features]))["mae_eur_mwh"], model, params))
    score, model, params = min(fitted, key=lambda item: item[0])
    prediction = model.predict(test[features])
    report = {"selected_parameters": params, "validation_mae_eur_mwh": score,
              "input_rows": input_rows, "warmup_rows_dropped": input_rows - len(frame),
              "purge_hours": purge_hours,
              "validation_candidates": [{"parameters": p, "mae_eur_mwh": s} for s, _, p in fitted],
              "test": metrics(test.price_eur_mwh, prediction),
              "baseline_24h_test": metrics(test.price_eur_mwh, test.price_lag_24h),
              "splits": {name: {"rows": len(part), "start": str(part.timestamp_utc.min()), "end": str(part.timestamp_utc.max())}
                         for name, part in zip(("train", "validation", "test"), (train, validation, test))},
              "evaluation_protocol": "rolling 24h horizon; assumes lagged prices available by origin t-24h; train-only fit, purged boundaries, validation selection, test once"}
    directory = Path(artifact_dir)
    directory.mkdir(parents=True, exist_ok=True)
    joblib.dump({"pipeline": model, "features": features}, directory / "model.joblib")
    (directory / "metrics.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    output = test[["timestamp_utc", "price_eur_mwh"]].copy()
    output["predicted_price_eur_mwh"] = prediction.astype(float)
    output["baseline_24h_eur_mwh"] = test.price_lag_24h.to_numpy()
    return report, output


def predict(frame, artifact_dir):
    bundle = joblib.load(Path(artifact_dir) / "model.joblib")
    if frame[bundle["features"]].isna().any().any():
        raise ValueError("Prediction rows must have complete features, including exact lags")
    result = frame[["timestamp_utc"]].copy()
    result["predicted_price_eur_mwh"] = bundle["pipeline"].predict(frame[bundle["features"]]).astype(float)
    return result

# COMMAND ----------

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
