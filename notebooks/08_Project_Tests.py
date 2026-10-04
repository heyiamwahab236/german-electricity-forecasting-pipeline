# Databricks notebook source
# MAGIC %md
# MAGIC # Execute corrupted-data and leakage-prevention tests
# MAGIC These are the same meaningful tests as the local project, executed with Databricks Spark. They cover schema drift, conversion failures, missing/duplicate hours, negative prices, explicit nullable exceptions, unmatched joins, exact lags over gaps, DST ambiguity, chronological splitting and training-only imputation.
# MAGIC Corrupted fixtures are separate from original sources. Synthetic model test errors are not project performance metrics.

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

"""Spark transforms; all persisted business layers use Delta."""
import json
import math
import csv
import io
from functools import reduce
from pyspark.sql import functions as F, types as T, Window


def column(name):
    return F.col("`" + name.replace("`", "``") + "`")


def read_csv(spark, spec):
    if spec.get("dst_policy") == "ordered_pair":
        return read_ordered_csv(spark, spec)
    # FAILFAST prevents silent truncation of malformed records; inferSchema remains off.
    df = (spark.read.option("header", True).option("sep", spec.get("delimiter", ";"))
          .option("encoding", "UTF-8").option("mode", "FAILFAST").csv(spec["path"]))
    expected, actual = spec["expected_columns"], df.columns
    if actual != expected or len(actual) != len(set(actual)):
        raise ValueError(f"Schema drift: expected {expected!r}; observed {actual!r}")
    # Force CSV parse now so an invalid input cannot be promoted.
    df.cache()
    df.count()
    return df.withColumn("_source_file", F.input_file_name()).withColumn("_ingested_at", F.current_timestamp())


def read_ordered_csv(spark, spec):
    """Bounded per-file CSV parser preserves physical row order for SMARD DST pairs.

    Spark distributes files; CSV records become string columns, never inferred types.
    This mode is for modest historical exports, not multi-GB streaming files.
    """
    headers = spec["expected_columns"]
    row_type = T.StructType([T.StructField(h, T.StringType()) for h in headers] + [T.StructField("_source_row", T.LongType())])

    def decode(content):
        if len(content) > spec.get("max_file_bytes", 64 * 1024 * 1024):
            raise ValueError("CSV exceeds ordered ingestion per-file memory limit")
        reader = csv.reader(io.StringIO(bytes(content).decode("utf-8-sig")), delimiter=spec.get("delimiter", ";"), strict=True)
        actual = next(reader, [])
        if actual != headers or len(actual) != len(set(actual)):
            raise ValueError(f"Schema drift: expected {headers!r}; observed {actual!r}")
        rows = []
        for index, row in enumerate(reader, 2):
            if len(row) != len(headers):
                raise ValueError(f"Malformed record at source row {index}")
            rows.append(tuple(row) + (index,))
        return rows

    decode_udf = F.udf(decode, T.ArrayType(row_type))
    files = spark.read.format("binaryFile").load(spec["path"])
    raw = files.select(F.col("path").alias("_source_file"), F.explode(decode_udf("content")).alias("record"))
    return raw.select("record.*", "_source_file").withColumn("_ingested_at", F.current_timestamp())


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


def join_datasets(datasets):
    """Full join retains unmatched records for auditing instead of hiding them."""
    marked = []
    for name, df in datasets.items():
        business = [c for c in df.columns if not c.startswith("_") and c != "timestamp_end_utc"]
        marked.append(df.select(*business).withColumn(f"_{name}_present", F.lit(True)))
    joined = reduce(lambda left, right: left.join(right, "timestamp_utc", "full"), marked)
    unmatched = {name: joined.filter(F.col(f"_{name}_present").isNull()).count() for name in datasets}
    return joined, unmatched


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


def write_delta(df, path):
    # Full-snapshot reruns are idempotent. No schema merging: contracts must change explicitly.
    df.write.format("delta").option("delta.columnMapping.mode", "name").mode("overwrite").save(path)


def write_report(spark, report, path):
    write_delta(spark.createDataFrame([(json.dumps(report, sort_keys=True),)], "report_json string")
                .withColumn("checked_at", F.current_timestamp()), path)

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

from datetime import datetime, timedelta
from pathlib import Path
import pytest

SPEC = dict(expected_columns=["Start date", "End date", "Price"], timestamp_columns=["Start date"],
            timestamp_format="%b %d, %Y %I:%M %p", locale="en", numeric_columns={"Price": "price_eur_mwh"}, nonnegative=[])


def test_corrupted_csv_quality_gate(spark):
    spec = dict(SPEC, path=str(Path(__file__).parent / "fixtures/corrupt_price.csv"), dst_policy="ordered_pair")
    df = normalize(read_csv(spark, spec), spec)
    report = quality_report(df, spec)
    assert report["rows"] == 6
    assert report["duplicate_extra_rows"] == 1
    assert report["missing_hourly_intervals"] == 1
    assert report["nulls"]["price_eur_mwh"] == 2
    assert report["conversion_errors"] == {"price_eur_mwh:invalid_number": 1, "price_eur_mwh:missing": 1, "invalid_timestamp": 1}
    assert not report["passed"]
    assert df.filter("price_eur_mwh < 0").count() == 1


def test_schema_drift_fails(spark, tmp_path):
    path = tmp_path / "drift.csv"
    path.write_text("Start date;End date;Renamed Price\n2024;2025;1\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Schema drift"):
        read_csv(spark, dict(SPEC, path=str(path)))


def test_exact_lags_do_not_shift_over_a_missing_hour(spark):
    start = datetime(2024, 1, 1)
    rows = [(start + timedelta(hours=h), float(h)) for h in range(75) if h != 1]
    df = spark.createDataFrame(rows, "timestamp_utc timestamp, price_eur_mwh double")
    features = prepare_features(df)
    at25 = features.filter("timestamp_utc = timestamp '2024-01-02 01:00:00'").first()
    assert at25.price_lag_24h is None
    at72 = features.filter("timestamp_utc = timestamp '2024-01-04 00:00:00'").first()
    assert at72.price_lag_24h == 48.0 and at72.price_lag_48h == 24.0
    assert at72.hour == 1  # Berlin is UTC+1 in January


def test_full_join_reports_unmatched(spark):
    t = datetime(2024, 1, 1)
    a = spark.createDataFrame([(t, 1.0)], "timestamp_utc timestamp, generation_mwh double")
    b = spark.createDataFrame([(t, 2.0), (t + timedelta(hours=1), 3.0)], "timestamp_utc timestamp, consumption_mwh double")
    p = spark.createDataFrame([(t, -5.0)], "timestamp_utc timestamp, price_eur_mwh double")
    joined, unmatched = join_datasets(dict(generation=a, consumption=b, price=p))
    assert joined.count() == 2
    assert unmatched == {"generation": 1, "consumption": 0, "price": 1}


def test_ordered_dst_pair_and_unexplained_duplicates(spark):
    spec = dict(SPEC, dst_policy="ordered_pair")
    df = spark.createDataFrame([
        ("Oct 27, 2024 2:00 AM", "Oct 27, 2024 3:00 AM", "10", "source.csv", 2),
        ("Oct 27, 2024 2:00 AM", "Oct 27, 2024 3:00 AM", "11", "source.csv", 3),
    ], "`Start date` string, `End date` string, Price string, _source_file string, _source_row long")
    result = normalize(df, spec).orderBy("timestamp_utc").collect()
    assert result[1].timestamp_utc - result[0].timestamp_utc == timedelta(hours=1)
    assert result[0].price_eur_mwh == 10
    lone = normalize(df.limit(1), spec).first()
    assert lone.timestamp_utc is None and "ambiguous_local_time" in lone._errors


def test_negative_price_passes_and_nullable_is_explicit(spark):
    df = spark.createDataFrame([(datetime(2024,1,1), -4.08, [])], "timestamp_utc timestamp, price_eur_mwh double, _errors array<string>")
    assert quality_report(df, SPEC)["passed"]
    energy = spark.createDataFrame([(datetime(2024,1,1), None, ["nuclear_mwh:missing"])], "timestamp_utc timestamp, nuclear_mwh double, _errors array<string>")
    assert quality_report(energy, dict(numeric_columns={"Nuclear": "nuclear_mwh"}, nullable=["nuclear_mwh"]))["passed"]
    assert not quality_report(energy, dict(numeric_columns={"Nuclear": "nuclear_mwh"}))["passed"]
import joblib
import numpy as np
import pandas as pd
import pytest


def test_split_orders_rows_and_rejects_duplicate_times():
    frame = pd.DataFrame({"timestamp_utc": pd.date_range("2024-01-01", periods=20, freq="h")[::-1]})
    train, validation, test = chronological_split(frame)
    assert train.timestamp_utc.max() < validation.timestamp_utc.min() < test.timestamp_utc.min()
    with pytest.raises(ValueError, match="unique"):
        chronological_split(pd.concat([frame, frame.iloc[:1]]))


def test_imputer_fit_uses_training_only_and_saved_model_predicts(tmp_path):
    n = 100
    frame = pd.DataFrame(dict(timestamp_utc=pd.date_range("2024-01-01", periods=n, freq="h"),
                             price_eur_mwh=np.sin(np.arange(n)) * 20,
                             price_lag_24h=np.arange(n, dtype=float), price_lag_48h=np.arange(n, dtype=float)))
    frame["hour"] = [1.0] * 60 + [999.0] * 40
    frame.loc[5, "hour"] = np.nan
    spec = dict(features=["hour", "price_lag_24h", "price_lag_48h"], purge_hours=0, candidates=[dict(n_estimators=5, max_depth=2)])
    report, output = evaluate(frame, spec, tmp_path)
    bundle = joblib.load(tmp_path / "model.joblib")
    assert bundle["pipeline"].named_steps["imputer"].statistics_[0] == 1.0
    assert report["splits"]["test"]["rows"] == 20
    assert len(output) == 20 and np.isfinite(output.predicted_price_eur_mwh).all()
    assert len(predict(frame.iloc[-24:], tmp_path)) == 24


def test_warmup_dropped_instead_of_future_backfill(tmp_path):
    frame = pd.DataFrame(dict(timestamp_utc=pd.date_range("2024-01-01", periods=80, freq="h"), price_eur_mwh=10., price_lag_24h=5., price_lag_48h=5.))
    frame.loc[:47, "price_lag_48h"] = np.nan
    report, _ = evaluate(frame, dict(features=["price_lag_24h", "price_lag_48h"], purge_hours=0, candidates=[dict(n_estimators=2)]), tmp_path)
    assert report["splits"]["train"]["start"] == str(frame.iloc[48].timestamp_utc)


def test_purged_boundaries_respect_forecast_origin():
    frame = pd.DataFrame({"timestamp_utc": pd.date_range("2024-01-01", periods=500, freq="h")})
    train, validation, test = chronological_split(frame, purge_hours=24)
    assert train.timestamp_utc.max() < validation.timestamp_utc.min() - pd.Timedelta(hours=24)
    assert validation.timestamp_utc.max() < test.timestamp_utc.min() - pd.Timedelta(hours=24)
# COMMAND ----------

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
