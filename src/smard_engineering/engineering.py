"""Spark transforms; all persisted business layers use Delta."""
import json
import math
import csv
import io
from functools import reduce
from pyspark.sql import functions as F, types as T, Window
from .parsing import parse_number, parse_timestamp


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
