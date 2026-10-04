import joblib
import numpy as np
import pandas as pd
import pytest
from smard_engineering.modeling import chronological_split, evaluate, predict


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
