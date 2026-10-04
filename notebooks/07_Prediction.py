# Databricks notebook source
# MAGIC %md
# MAGIC # Predict the next 24 historical delivery hours
# MAGIC The source history ends on 27 January 2025. This produces a historical demonstration for the next 24 elapsed hours, not a live forecast for today.
# MAGIC We use the exact same saved preprocessing and selected XGBoost model. Calendar features and lag prices are built from known history; no future actual generation, consumption or target price is required.
# MAGIC The training-only evaluation model stays frozen for transparency; a separately governed production model could later be refitted on all history after evaluation.

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
