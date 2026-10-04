import json
import re
from pathlib import Path


def load_config(path):
    config = json.loads(Path(path).read_text(encoding="utf-8"))
    if set(config["datasets"]) != {"generation", "consumption", "price"}:
        raise ValueError("Exactly generation, consumption and price are required")
    aliases = []
    for name, spec in config["datasets"].items():
        required = set(spec["timestamp_columns"]) | set(spec["numeric_columns"])
        if not required <= set(spec["expected_columns"]):
            raise ValueError(f"{name}: parsing columns absent from schema contract")
        if spec.get("locale", "de") not in {"de", "en"}:
            raise ValueError("Unsupported numeric locale")
        aliases.extend(spec["numeric_columns"].values())
    if len(aliases) != len(set(aliases)) or any(not re.fullmatch(r"[a-z][a-z0-9_]*", a) for a in aliases):
        raise ValueError("Numeric aliases must be unique safe identifiers")
    if "price_eur_mwh" not in config["datasets"]["price"]["numeric_columns"].values():
        raise ValueError("Price dataset must map its target to price_eur_mwh")
    if not set(config["model"]["features"]) <= {"hour", "day_of_week", "month", "is_weekend", "hour_sin", "hour_cos", "price_lag_24h", "price_lag_48h"}:
        raise ValueError("Only forecast-time available features are supported")
    return config
