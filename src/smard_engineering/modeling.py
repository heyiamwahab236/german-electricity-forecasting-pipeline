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
