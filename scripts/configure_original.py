"""Generate the committed original schema contracts from the inspected source export.

This is an explicit onboarding tool, NEVER invoked automatically by the pipeline:
regenerating contracts on each run would conceal schema drift.
"""
import argparse
import csv
import hashlib
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--source", default="../dsc-group-5-source/Submission/Datasets")
parser.add_argument("--output", default="config.local.json")
parser.add_argument("--output-root", default="local-output")
args = parser.parse_args()
base = Path(__file__).resolve().parents[1]
config = json.loads((base / "config.example.json").read_text(encoding="utf-8"))
config["description"] = "Exact schema from original SMARD exports at Git revision 8ef4b6a. Start date is Europe/Berlin delivery time."
config["output_root"] = str(Path(args.output_root).resolve()).replace("\\", "/")
files = {"generation": "Actual_generation", "consumption": "Actual_consumption", "price": "Day-ahead_prices"}
generation_aliases = ["biomass_mwh", "hydropower_mwh", "wind_offshore_mwh", "wind_onshore_mwh", "photovoltaics_mwh", "other_renewable_mwh", "nuclear_mwh", "lignite_mwh", "hard_coal_mwh", "fossil_gas_mwh", "generation_pumped_storage_mwh", "other_conventional_mwh"]
manifest = {}
for name, prefix in files.items():
    path = (Path(args.source) / (prefix + "_201901010000_202501280000_Hour.csv")).resolve()
    with path.open(encoding="utf-8-sig", newline="") as stream:
        headers = next(csv.reader(stream, delimiter=";"))
    if name == "generation":
        mapping = dict(zip(headers[2:], generation_aliases, strict=True))
    elif name == "consumption":
        mapping = dict(zip(headers[2:], ["consumption_mwh", "residual_load_mwh", "consumption_pumped_storage_mwh"], strict=True))
    else:
        mapping = {headers[2]: "price_eur_mwh"}
    config["datasets"][name] = dict(path=str(path).replace("\\", "/"), delimiter=";", expected_columns=headers,
        timestamp_columns=["Start date"], timestamp_format="%b %d, %Y %I:%M %p", timezone="Europe/Berlin", locale="en",
        dst_policy="ordered_pair", numeric_columns=mapping,
        nonnegative=[v for v in mapping.values() if v not in {"price_eur_mwh", "residual_load_mwh"}],
        nullable=["nuclear_mwh"] if name == "generation" else [])
    manifest[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
Path(args.output).write_text(json.dumps(config, indent=2, ensure_ascii=False), encoding="utf-8")
(base / "reports").mkdir(exist_ok=True)
(base / "reports/source_checksums.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
print(f"Wrote {args.output}; schema must be reviewed before production use")
