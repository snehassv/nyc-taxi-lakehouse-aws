"""AWS Glue job: load raw NYC taxi Parquet files into two Apache Iceberg tables.

* ``trips_unpartitioned`` -- no partitions.
* ``trips_partitioned``   -- hidden partitioning by month of pickup time.

Both tables get the same rows. Each raw file is read on its own, cleaned, and
fitted to the table with ``schema_utils.plan_changes`` (Iceberg schema
evolution: add columns, promote types). Files already in the table are skipped,
so the job is safe to re-run.

Before loading, the job also tries the "naive" way -- one Spark read over all
files -- and records what goes wrong. That is the problem this project solves.

Job arguments (set by Terraform / Airflow):
  --bucket        S3 bucket name
  --database      Glue Data Catalog database
  --raw_prefix    where raw files live, e.g. raw/yellow/
  --reset         "true" to drop both tables first
  --naive_check   "true" to run the naive-read experiment
"""
import json
import sys
import time
import traceback

import boto3
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import functions as F

import schema_utils as su

CATALOG = "glue_catalog"
TABLES = {"unpartitioned": "trips_unpartitioned", "partitioned": "trips_partitioned"}
PARTITION_COL = "tpep_pickup_datetime"


def build_spark(bucket: str):
    conf = SparkConf()
    conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    conf.set(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    conf.set(f"spark.sql.catalog.{CATALOG}.warehouse", f"s3://{bucket}/warehouse/")
    conf.set(f"spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    conf.set(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    conf.set("spark.sql.session.timeZone", "UTC")
    # Some old Parquet files may store times in nanoseconds; read them as longs
    # and convert in schema_utils.normalize instead of failing.
    conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")
    sc = SparkContext(conf=conf)
    return GlueContext(sc).spark_session


def list_raw_files(bucket: str, prefix: str) -> list[str]:
    s3 = boto3.client("s3")
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".parquet")]
    # sort by file name (yellow_tripdata_YYYY-MM.parquet) so years load in order
    return [f"s3://{bucket}/{k}" for k in sorted(keys, key=lambda k: k.rsplit("/", 1)[-1])]


def first_line(exc: Exception) -> str:
    msg = str(exc).strip().splitlines()
    return (msg[0] if msg else type(exc).__name__)[:400]


def naive_check(spark, paths: list[str]) -> dict:
    """Try to read all files as one dataset, the way many first pipelines do."""
    out = {}

    # 1) mergeSchema=true: Spark tries to merge all file schemas.
    t = time.time()
    try:
        df = spark.read.option("mergeSchema", "true").parquet(*paths)
        df.agg(F.count(F.lit(1))).collect()
        out["merge_schema"] = {"ok": True, "columns": df.columns}
    except Exception as e:  # noqa: BLE001 - we want to record any failure
        out["merge_schema"] = {"ok": False, "error": first_line(e)}
    out["merge_schema"]["seconds"] = round(time.time() - t, 1)

    # 2) default read: Spark takes the schema from ONE file and applies it to all.
    t = time.time()
    try:
        df = spark.read.parquet(*paths)
        cols = [c for c in df.columns]
        sums = [F.count(F.lit(1)).alias("rows")]
        sums += [F.count(F.col(c)).alias(f"nonnull_{c}") for c in cols]
        row = df.agg(*sums).collect()[0].asDict()
        out["default_read"] = {"ok": True, "columns": cols, "counts": row}
    except Exception as e:  # noqa: BLE001
        out["default_read"] = {"ok": False, "error": first_line(e)}
    out["default_read"]["seconds"] = round(time.time() - t, 1)
    return out


def table_cols(spark, full_name: str) -> dict[str, str] | None:
    if not spark.catalog.tableExists(full_name):
        return None
    return su.spark_types(spark.table(full_name))


def apply_ddl(spark, full_name: str, changes: list[su.Change]) -> None:
    for ch in changes:
        if ch.action == "add_column":
            spark.sql(f"ALTER TABLE {full_name} ADD COLUMN {ch.column} {ch.to_type}")
        elif ch.action == "promote_table_column":
            spark.sql(f"ALTER TABLE {full_name} ALTER COLUMN {ch.column} TYPE {ch.to_type}")


def loaded_files(spark, full_name: str) -> set[str]:
    if not spark.catalog.tableExists(full_name):
        return set()
    return {r[0] for r in spark.table(full_name).select("source_file").distinct().collect()}


def run(spark, args: dict) -> dict:
    bucket, db = args["bucket"], args["database"]
    names = {k: f"{CATALOG}.{db}.{v}" for k, v in TABLES.items()}
    report = {"started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "files": []}

    paths = list_raw_files(bucket, args["raw_prefix"])
    if not paths:
        raise RuntimeError(f"No Parquet files under s3://{bucket}/{args['raw_prefix']}")
    report["raw_files"] = len(paths)

    if args.get("naive_check", "true").lower() == "true":
        report["naive_check"] = naive_check(spark, paths)

    if args.get("reset", "false").lower() == "true":
        for n in names.values():
            spark.sql(f"DROP TABLE IF EXISTS {n} PURGE")

    done = loaded_files(spark, names["unpartitioned"]) & loaded_files(spark, names["partitioned"])

    for path in paths:
        fname = path.rsplit("/", 1)[-1]
        if fname in done:
            report["files"].append({"file": fname, "skipped": "already loaded"})
            continue
        t = time.time()
        raw = spark.read.parquet(path)
        raw_types = su.spark_types(raw)
        df, empty_cols = su.mark_empty_columns(su.normalize(raw))
        df = df.withColumn("source_file", F.lit(fname))
        incoming = su.spark_types(df)

        # Both tables always have the same columns, so one plan fits both.
        current = table_cols(spark, names["unpartitioned"])
        changes = su.plan_changes(current, incoming)
        su.check_whole_numbers(df, changes)

        if current is None:
            keep = [c for c, t_ in incoming.items() if t_ != "void"]
            df = df.select(*keep)
            for kind, n in names.items():
                w = df.writeTo(n).using("iceberg").tableProperty("format-version", "2")
                if kind == "partitioned":
                    w = w.partitionedBy(F.months(PARTITION_COL))
                w.create()
        else:
            for n in names.values():
                apply_ddl(spark, n, changes)
            target = table_cols(spark, names["unpartitioned"])
            out = su.conform(df, target)
            for n in names.values():
                out.writeTo(n).append()

        rows = spark.table(names["unpartitioned"]).where(F.col("source_file") == fname).count()
        report["files"].append({
            "file": fname,
            "rows_loaded": rows,
            "raw_columns": raw_types,
            "empty_columns": empty_cols,
            "changes": [c.to_dict() for c in changes],
            "seconds": round(time.time() - t, 1),
        })
        print(f"[load] {fname}: {rows} rows, {len(changes)} schema actions")

    report["final_schema"] = table_cols(spark, names["unpartitioned"])
    report["table_rows"] = {k: spark.table(n).count() for k, n in names.items()}
    report["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    return report


def main():
    args = getResolvedOptions(sys.argv, ["bucket", "database", "raw_prefix", "reset", "naive_check"])
    spark = build_spark(args["bucket"])
    s3 = boto3.client("s3")
    try:
        report = run(spark, args)
    except Exception as e:
        report = {"failed": True, "error": first_line(e), "trace": traceback.format_exc()[-4000:]}
        s3.put_object(Bucket=args["bucket"], Key="results/load_report.json",
                      Body=json.dumps(report, indent=2, default=str).encode())
        raise
    s3.put_object(Bucket=args["bucket"], Key="results/load_report.json",
                  Body=json.dumps(report, indent=2, default=str).encode())


if __name__ == "__main__":
    main()
