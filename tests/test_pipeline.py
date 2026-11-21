"""Small tests for the helper scripts (no AWS calls)."""
from pathlib import Path

from pipeline import athena_bench as ab
from pipeline import raw

ROOT = Path(__file__).resolve().parent.parent


def test_query_cost_uses_10mb_minimum_and_rounds_up():
    assert ab.query_cost(0) == ab.query_cost(10 * ab.MB)
    assert ab.query_cost(10 * ab.MB + 1) == 11 * ab.MB / ab.TB * 5
    assert round(ab.query_cost(ab.TB), 6) == 5.0


def test_benchmark_queries_parse_and_use_table_placeholder():
    qs = ab.load_queries(ROOT / "sql" / "benchmark_queries.sql")
    assert [q["id"] for q in qs] == ["q1_one_month", "q2_three_years",
                                     "q3_new_column_2025", "q4_no_time_filter"]
    for q in qs:
        assert "{table}" in q["sql"] and not q["sql"].endswith(";")


def test_parse_years():
    assert raw.parse_years("2011-2013") == [2011, 2012, 2013]
    assert raw.parse_years("2019,2025") == [2019, 2025]


def test_schema_diff_shows_case_change_and_missing_column():
    md = raw.schema_diff_markdown([
        {"file": "yellow_tripdata_2011-01.parquet", "columns": {"VendorID": "int64", "airport_fee": "null"}},
        {"file": "yellow_tripdata_2025-01.parquet",
         "columns": {"VendorID": "int32", "Airport_fee": "double", "cbd_congestion_fee": "double"}},
    ])
    assert "| airport_fee | null | double (`Airport_fee`) |" in md
    assert "| cbd_congestion_fee | — | double |" in md
