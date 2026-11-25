"""Fit each raw taxi file into one Iceberg table, even when its columns differ.

The NYC TLC files are not all the same shape. Over the years columns were added
(congestion_surcharge, airport_fee, cbd_congestion_fee), a column changed its
name's letter case (airport_fee -> Airport_fee), and number types changed
(for example int64 in old files, int32 in newer ones).

This module has two parts:

* ``plan_changes`` -- pure Python. Given the table's columns and a new file's
  columns, it decides what to do with each column. No Spark needed, so it is
  easy to unit test.
* ``normalize`` / ``conform`` -- small Spark helpers that clean names and
  timestamps, then make a DataFrame match the table exactly.

Rules (Iceberg only allows safe type changes, so we follow the same idea):

* New column in the file      -> ADD COLUMN to the table (old rows read NULL).
* Column missing from file    -> write NULL for it.
* Table int, file bigint      -> PROMOTE the table column to bigint (Iceberg allows this).
* Table float, file double    -> PROMOTE the table column to double.
* File type narrower than table (int -> bigint, int/bigint -> double, float -> double)
                              -> CAST the file's column up. No data is lost.
* Table bigint, file double   -> cast only if every value is a whole number
                                 (checked at run time), else stop.
* Column that is empty (all NULL, made void by ``mark_empty_columns``)
                              -> skip it if the table doesn't have it yet,
                                 else cast NULLs to the table's type.
* Anything else (e.g. string vs number) -> stop with a clear error.
  We never silently change data.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass

# Table column type a -> b that Iceberg can change in place (type promotion).
TABLE_PROMOTIONS = {("int", "bigint"), ("float", "double")}

# File column type a -> table type b that we can cast without losing data.
SAFE_UPCASTS = {
    ("int", "bigint"),
    ("int", "double"),
    ("bigint", "double"),
    ("float", "double"),
    ("smallint", "int"),
    ("smallint", "bigint"),
    ("tinyint", "int"),
    ("tinyint", "bigint"),
    ("timestamp", "timestamp_ntz"),
}


class SchemaConflict(Exception):
    """Raised when a column cannot be loaded without changing its meaning."""


@dataclass
class Change:
    column: str
    action: str  # create_table | add_column | promote_table_column | cast_incoming |
    #              cast_if_whole_numbers | fill_null | skip_empty_column
    from_type: str | None = None
    to_type: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)


def plan_changes(table_cols: dict[str, str] | None, incoming: dict[str, str]) -> list[Change]:
    """Return the list of changes needed to load ``incoming`` into the table.

    ``table_cols`` and ``incoming`` map lower-case column name -> Spark type name
    (``DataType.simpleString()``, e.g. ``"bigint"``). ``table_cols`` is ``None``
    when the table does not exist yet.
    """
    changes: list[Change] = []

    if table_cols is None:
        for col, typ in incoming.items():
            if typ == "void":
                changes.append(Change(col, "skip_empty_column", typ, None))
        changes.insert(0, Change("*", "create_table"))
        return changes

    for col, new in incoming.items():
        old = table_cols.get(col)
        if old is None:
            if new == "void":
                changes.append(Change(col, "skip_empty_column", new, None))
            else:
                changes.append(Change(col, "add_column", None, new))
        elif old == new:
            continue
        elif new == "void":
            changes.append(Change(col, "cast_incoming", new, old))
        elif (old, new) in TABLE_PROMOTIONS:
            changes.append(Change(col, "promote_table_column", old, new))
        elif (new, old) in SAFE_UPCASTS:
            changes.append(Change(col, "cast_incoming", new, old))
        elif old == "bigint" and new in ("double", "float"):
            changes.append(Change(col, "cast_if_whole_numbers", new, old))
        else:
            raise SchemaConflict(
                f"Column '{col}': table has {old}, file has {new}. "
                "This is not a safe change, so the load stops here."
            )

    for col, old in table_cols.items():
        if col not in incoming:
            changes.append(Change(col, "fill_null", None, old))

    return changes


# ---------------------------------------------------------------- Spark helpers
# Imported lazily so the pure planner above can be tested without Spark.

def spark_types(df) -> dict[str, str]:
    return {f.name: f.dataType.simpleString() for f in df.schema.fields}


def normalize(df):
    """Lower-case column names and make every *_datetime column TIMESTAMP_NTZ.

    TLC times are New York local clock times with no time zone, so
    "timestamp without time zone" is the honest type. The Spark session must use
    spark.sql.session.timeZone=UTC so the cast does not shift the clock.
    """
    from pyspark.sql import functions as F

    lowered = [c.lower() for c in df.columns]
    dupes = {c for c in lowered if lowered.count(c) > 1}
    if dupes:
        raise SchemaConflict(f"Columns clash after lower-casing: {sorted(dupes)}")
    df = df.toDF(*lowered)

    for name, typ in spark_types(df).items():
        if not name.endswith("datetime"):
            continue
        if typ == "timestamp":
            df = df.withColumn(name, F.col(name).cast("timestamp_ntz"))
        elif typ == "bigint":
            # Parquet TIMESTAMP(NANOS) read as a long (spark.sql.legacy.parquet.nanosAsLong).
            df = df.withColumn(
                name, F.timestamp_micros(F.floor(F.col(name) / 1000)).cast("timestamp_ntz")
            )
        elif typ != "timestamp_ntz":
            raise SchemaConflict(f"Column '{name}' looks like a time but has type {typ}")
    return df


def mark_empty_columns(df):
    """Turn columns that are NULL in every row into untyped (void) columns.

    An all-NULL column carries no data, but Parquet still gives it a type
    (Spark often reads it as int). If we let that type into the table, a later
    file with real double values would clash. Making it void lets the first
    file that really has values decide the type.

    Returns (df, list of emptied column names). Costs one scan of the file.
    """
    from pyspark.sql import functions as F

    counts = df.agg(*[F.count(F.col(c)).alias(c) for c in df.columns]).collect()[0].asDict()
    empty = [c for c, n in counts.items() if n == 0]
    for c in empty:
        df = df.withColumn(c, F.lit(None))
    return df, empty


def check_whole_numbers(df, changes: list[Change]) -> None:
    """For double -> bigint casts, stop if any value has a fraction."""
    from pyspark.sql import functions as F

    for ch in changes:
        if ch.action != "cast_if_whole_numbers":
            continue
        c = F.col(ch.column)
        bad = df.filter(c.isNotNull() & (c != F.floor(c))).limit(1).count()
        if bad:
            raise SchemaConflict(
                f"Column '{ch.column}' has fractional values; can't store them as {ch.to_type}"
            )


def conform(df, table_cols: dict[str, str]):
    """Return ``df`` with exactly the table's columns, in order, with the table's types."""
    from pyspark.sql import functions as F

    have = spark_types(df)
    out = []
    for col, typ in table_cols.items():
        if col in have:
            expr = F.col(col) if have[col] == typ else F.col(col).cast(typ)
        else:
            expr = F.lit(None).cast(typ)
        out.append(expr.alias(col))
    return df.select(*out)
