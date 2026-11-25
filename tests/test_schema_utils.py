"""Unit tests for the schema planner (no Spark needed)."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "glue_jobs"))
import schema_utils as su  # noqa: E402


def actions(changes):
    return {(c.column, c.action) for c in changes}


def test_first_file_creates_table_and_skips_empty_columns():
    ch = su.plan_changes(None, {"vendorid": "bigint", "airport_fee": "void"})
    assert ch[0].action == "create_table"
    assert ("airport_fee", "skip_empty_column") in actions(ch)


def test_new_column_is_added():
    ch = su.plan_changes({"a": "bigint"}, {"a": "bigint", "cbd_congestion_fee": "double"})
    assert actions(ch) == {("cbd_congestion_fee", "add_column")}


def test_missing_column_is_filled_with_null():
    ch = su.plan_changes({"a": "bigint", "airport_fee": "double"}, {"a": "bigint"})
    assert actions(ch) == {("airport_fee", "fill_null")}


def test_narrower_incoming_type_is_cast_up():
    ch = su.plan_changes({"vendorid": "bigint", "passenger_count": "double"},
                         {"vendorid": "int", "passenger_count": "bigint"})
    assert actions(ch) == {("vendorid", "cast_incoming"), ("passenger_count", "cast_incoming")}


def test_wider_incoming_type_promotes_table():
    ch = su.plan_changes({"vendorid": "int"}, {"vendorid": "bigint"})
    assert actions(ch) == {("vendorid", "promote_table_column")}


def test_double_into_bigint_needs_whole_number_check():
    ch = su.plan_changes({"passenger_count": "bigint"}, {"passenger_count": "double"})
    assert actions(ch) == {("passenger_count", "cast_if_whole_numbers")}


def test_empty_column_matching_existing_column_is_cast():
    ch = su.plan_changes({"airport_fee": "double"}, {"airport_fee": "void"})
    assert actions(ch) == {("airport_fee", "cast_incoming")}


def test_unsafe_change_stops():
    with pytest.raises(su.SchemaConflict):
        su.plan_changes({"store_and_fwd_flag": "string"}, {"store_and_fwd_flag": "bigint"})


def test_same_schema_needs_nothing():
    assert su.plan_changes({"a": "bigint"}, {"a": "bigint"}) == []
