"""Spark test: three small Parquet files that drift the way the TLC files do.

We can't run Iceberg locally in this test, so we keep a plain dict as "the
table schema" and apply the same plan/conform steps the Glue job uses. This
checks the Spark side: names, timestamps, casts, nulls and row counts.
"""
import sys
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "glue_jobs"))
import schema_utils as su  # noqa: E402

pyspark = pytest.importorskip("pyspark")


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    s = (SparkSession.builder.master("local[1]")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.legacy.parquet.nanosAsLong", "true")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    yield s
    s.stop()


def write_files(tmp: Path):
    t = [datetime(2011, 1, 1, 8, 30), datetime(2011, 1, 2, 9, 0)]
    old = pa.table({  # like 2011: int64 vendor, double passengers, empty fee columns
        "VendorID": pa.array([1, 2], pa.int64()),
        "tpep_pickup_datetime": pa.array(t, pa.timestamp("ns")),
        "passenger_count": pa.array([1.0, 2.0]),
        "fare_amount": pa.array([5.5, 7.0]),
        "congestion_surcharge": pa.nulls(2),
        "airport_fee": pa.nulls(2),
    })
    mid = pa.table({  # like 2023: int32 vendor, int64 passengers, Airport_fee renamed
        "VendorID": pa.array([2], pa.int32()),
        "tpep_pickup_datetime": pa.array([datetime(2023, 1, 1, 0, 5)], pa.timestamp("us")),
        "passenger_count": pa.array([3], pa.int64()),
        "fare_amount": pa.array([12.0]),
        "congestion_surcharge": pa.array([2.5]),
        "Airport_fee": pa.array([1.75]),
    })
    new = pa.table({  # like 2025: new cbd_congestion_fee column
        "VendorID": pa.array([1, 1, 2], pa.int32()),
        "tpep_pickup_datetime": pa.array([datetime(2025, 1, 6, 12)] * 3, pa.timestamp("us")),
        "passenger_count": pa.array([1, None, 1], pa.int64()),
        "fare_amount": pa.array([10.0, 11.0, 12.0]),
        "congestion_surcharge": pa.array([2.5, 2.5, 0.0]),
        "Airport_fee": pa.array([0.0, 0.0, 1.75]),
        "cbd_congestion_fee": pa.array([0.75, 0.75, 0.0]),
    })
    paths = []
    for name, tbl in [("2011-01", old), ("2023-01", mid), ("2025-01", new)]:
        p = tmp / f"yellow_tripdata_{name}.parquet"
        pq.write_table(tbl, p)
        paths.append(str(p))
    return paths


def test_drifting_files_load_into_one_schema(spark, tmp_path):
    paths = write_files(tmp_path)
    table = None
    frames = []
    log = []
    for p in paths:
        df, _ = su.mark_empty_columns(su.normalize(spark.read.parquet(p)))
        incoming = su.spark_types(df)
        changes = su.plan_changes(table, incoming)
        su.check_whole_numbers(df, changes)
        log += [(Path(p).name[16:23], c.column, c.action) for c in changes]
        if table is None:
            table = {c: t for c, t in incoming.items() if t != "void"}
        else:
            for c in changes:
                if c.action in ("add_column", "promote_table_column"):
                    table[c.column] = c.to_type
        frames.append(df)

    out = None
    for df in frames:
        part = su.conform(df, table)
        out = part if out is None else out.unionByName(part)

    assert table == {
        "vendorid": "bigint",
        "tpep_pickup_datetime": "timestamp_ntz",
        "passenger_count": "double",
        "fare_amount": "double",
        "congestion_surcharge": "double",
        "airport_fee": "double",
        "cbd_congestion_fee": "double",
    }
    assert out.count() == 6
    rows = {r["vendorid"]: r for r in out.where("year(tpep_pickup_datetime) = 2023").collect()}
    assert rows[2]["airport_fee"] == 1.75          # Airport_fee -> airport_fee kept its value
    first = out.orderBy("tpep_pickup_datetime").first()
    assert first["tpep_pickup_datetime"] == datetime(2011, 1, 1, 8, 30)  # nanos read, no shift
    assert first["cbd_congestion_fee"] is None     # old rows read NULL for new column
    assert ("2025-01", "cbd_congestion_fee", "add_column") in log
    assert ("2011-01", "airport_fee", "skip_empty_column") in log
    assert ("2023-01", "airport_fee", "add_column") in log
    assert ("2023-01", "vendorid", "cast_incoming") in log
