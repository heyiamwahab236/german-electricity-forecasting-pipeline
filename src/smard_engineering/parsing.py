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
