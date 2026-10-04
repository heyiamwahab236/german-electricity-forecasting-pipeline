from datetime import datetime, timezone
import pytest
from smard_engineering.parsing import parse_number, parse_timestamp


@pytest.mark.parametrize("value,locale,expected", [("1.234,56", "de", 1234.56), ("4,815.25", "en", 4815.25), ("-4.08", "en", -4.08), ("-23,50", "de", -23.5), ("0", "de", 0)])
def test_valid_numbers(value, locale, expected):
    assert parse_number(value, locale) == (expected, None)


@pytest.mark.parametrize("value", ["NaN", "Infinity", "12watts", "1,23.50", "1.2.3", "1e309"])
def test_corrupted_numbers_are_detected(value):
    assert parse_number(value, "en") == (None, "invalid_number")


def test_missing_is_not_zero():
    assert parse_number("-") == (None, "missing")


def test_dst_nonexistent_and_ambiguous_are_not_guessed():
    fmt = "%Y-%m-%d %H:%M"
    assert parse_timestamp("2024-03-31 02:00", fmt)[1] == "nonexistent_local_time"
    assert parse_timestamp("2024-10-27 02:00", fmt)[1] == "ambiguous_local_time"
    early = parse_timestamp("2024-10-27 02:00", fmt, fold=0)[0]
    late = parse_timestamp("2024-10-27 02:00", fmt, fold=1)[0]
    assert late - early == 3600


def test_offset_times_and_spring_gap_are_contiguous_utc():
    fmt = "%Y-%m-%d %H:%M"
    a = parse_timestamp("2024-03-31 01:00", fmt)[0]
    b = parse_timestamp("2024-03-31 03:00", fmt)[0]
    assert b - a == 3600
    assert parse_timestamp("2024-10-27T02:00+0200", "%Y-%m-%dT%H:%M%z")[0] == int(datetime(2024, 10, 27, tzinfo=timezone.utc).timestamp())
