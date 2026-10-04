from datetime import datetime, timedelta
from pathlib import Path
import pytest
from smard_engineering.engineering import read_csv, normalize, quality_report, join_datasets, prepare_features

SPEC = dict(expected_columns=["Start date", "End date", "Price"], timestamp_columns=["Start date"],
            timestamp_format="%b %d, %Y %I:%M %p", locale="en", numeric_columns={"Price": "price_eur_mwh"}, nonnegative=[])


@pytest.mark.spark
def test_corrupted_csv_quality_gate(spark):
    spec = dict(SPEC, path=str(Path(__file__).parent / "fixtures/corrupt_price.csv"), dst_policy="ordered_pair")
    df = normalize(read_csv(spark, spec), spec).cache()
    report = quality_report(df, spec)
    assert report["rows"] == 6
    assert report["duplicate_extra_rows"] == 1
    assert report["missing_hourly_intervals"] == 1
    assert report["nulls"]["price_eur_mwh"] == 2
    assert report["conversion_errors"] == {"price_eur_mwh:invalid_number": 1, "price_eur_mwh:missing": 1, "invalid_timestamp": 1}
    assert not report["passed"]
    assert df.filter("price_eur_mwh < 0").count() == 1


@pytest.mark.spark
def test_schema_drift_fails(spark, tmp_path):
    path = tmp_path / "drift.csv"
    path.write_text("Start date;End date;Renamed Price\n2024;2025;1\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Schema drift"):
        read_csv(spark, dict(SPEC, path=str(path)))


@pytest.mark.spark
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


@pytest.mark.spark
def test_full_join_reports_unmatched(spark):
    t = datetime(2024, 1, 1)
    a = spark.createDataFrame([(t, 1.0)], "timestamp_utc timestamp, generation_mwh double")
    b = spark.createDataFrame([(t, 2.0), (t + timedelta(hours=1), 3.0)], "timestamp_utc timestamp, consumption_mwh double")
    p = spark.createDataFrame([(t, -5.0)], "timestamp_utc timestamp, price_eur_mwh double")
    joined, unmatched = join_datasets(dict(generation=a, consumption=b, price=p))
    assert joined.count() == 2
    assert unmatched == {"generation": 1, "consumption": 0, "price": 1}


@pytest.mark.spark
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


@pytest.mark.spark
def test_negative_price_passes_and_nullable_is_explicit(spark):
    df = spark.createDataFrame([(datetime(2024,1,1), -4.08, [])], "timestamp_utc timestamp, price_eur_mwh double, _errors array<string>")
    assert quality_report(df, SPEC)["passed"]
    energy = spark.createDataFrame([(datetime(2024,1,1), None, ["nuclear_mwh:missing"])], "timestamp_utc timestamp, nuclear_mwh double, _errors array<string>")
    assert quality_report(energy, dict(numeric_columns={"Nuclear": "nuclear_mwh"}, nullable=["nuclear_mwh"]))["passed"]
    assert not quality_report(energy, dict(numeric_columns={"Nuclear": "nuclear_mwh"}))["passed"]
