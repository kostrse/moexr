from __future__ import annotations

import json
from datetime import date
from typing import Any, cast

import pytest
from conftest import FIXTURES_DIR
from moexr.client import MoexIndexedTable, MoexTable

JsonObject = dict[str, Any]


def load_json(file_name: str, table_name: str) -> JsonObject:
    path = FIXTURES_DIR / file_name
    with path.open("r", encoding="utf-8") as f:
        j = json.load(f)
        return j[table_name]


class TestMoexTable:
    @pytest.fixture(scope="class")
    def history_json(self) -> JsonObject:
        return load_json("history.json", "history")

    @pytest.fixture(scope="class")
    def table(self, history_json: JsonObject) -> MoexTable:
        return MoexTable.from_result(history_json)

    def test_row_count(self, history_json: JsonObject, table: MoexTable):
        assert table.row_count() == len(history_json["data"])

    def test_columns(self, history_json: JsonObject, table: MoexTable):
        assert len(table.columns) == len(history_json["columns"])

        # Spot check a few expected columns
        for column in ("BOARDID", "TRADEDATE", "SECID", "CLOSE"):
            assert table.has_column(column)

    def test_value_types(self, table: MoexTable):
        row = table.get_row(10)
        assert row is not None

        date_value = table.get_value(10, "TRADEDATE")
        assert date_value is not None and isinstance(date_value, date)
        assert date_value == date(2015, 5, 8)

        int_value = table.get_value(10, "TRADINGSESSION")
        assert int_value is None or isinstance(int_value, int)
        assert int_value == 3

        float_value = table.get_value(10, "NUMTRADES")
        assert float_value is None or isinstance(float_value, float)
        assert float_value == 12.0

        price_value = table.get_value(10, "CLOSE")
        assert price_value is not None and isinstance(price_value, float)
        assert price_value == 73.22

    def test_value_nulls(self, table: MoexTable):
        price_value = table.get_value(0, "CLOSE")
        assert price_value is None

    def test_get_row_out_of_bounds(self, table: MoexTable):
        with pytest.raises(IndexError):
            table.get_row(9999)

    def test_get_rows_iterates_all(self, history_json: JsonObject, table: MoexTable):
        rows = list(table.get_rows())
        assert len(rows) == len(history_json["data"])


class TestMoexIndexedTable:
    @pytest.fixture(scope="class")
    def history_json(self) -> JsonObject:
        return load_json("history.json", "history")

    @pytest.fixture(scope="class")
    def table(self, history_json: JsonObject) -> MoexTable:
        return MoexTable.from_result(history_json)

    @pytest.fixture(scope="class")
    def indexed(self, table: MoexTable) -> MoexIndexedTable[date]:
        return MoexIndexedTable(table, "TRADEDATE")

    def test_index_column(self, indexed: MoexIndexedTable[date]):
        assert indexed.index_column == "TRADEDATE"

    def test_columns_delegated(self, table: MoexTable, indexed: MoexIndexedTable[date]):
        assert indexed.columns == table.columns

    def test_row_count_delegated(self, table: MoexTable, indexed: MoexIndexedTable[date]):
        assert indexed.row_count() == table.row_count()
        assert len(indexed) == len(table)

    def test_table_property(self, table: MoexTable, indexed: MoexIndexedTable[date]):
        assert indexed.table is table

    def test_invalid_index_column(self, table: MoexTable):
        with pytest.raises(ValueError, match="index column"):
            MoexIndexedTable(table, "NONEXISTENT")

    def test_get_row_exact(self, indexed: MoexIndexedTable[date]):
        lookup_date = date(2015, 5, 8)
        row = indexed.get_row(lookup_date)
        assert row is not None
        col_pos = indexed.get_column_position("TRADEDATE")
        assert row[col_pos] == lookup_date

    def test_get_row_not_found(self, indexed: MoexIndexedTable[date]):
        lookup_date = date(2015, 5, 10)  # weekend, no trading
        row = indexed.get_row(lookup_date)
        assert row is None

    def test_get_value_exact(self, indexed: MoexIndexedTable[date]):
        lookup_date = date(2015, 5, 8)
        value = indexed.get_value(lookup_date, "TRADEDATE")
        assert value == lookup_date

    def test_get_value_not_found(self, indexed: MoexIndexedTable[date]):
        lookup_date = date(2015, 5, 10)
        value = indexed.get_value(lookup_date, "CLOSE")
        assert value is None

    def test_get_rows_full_range(self, indexed: MoexIndexedTable[date]):
        rows = list(indexed.get_rows())
        assert len(rows) == indexed.row_count()

    def test_get_rows_with_range(self, indexed: MoexIndexedTable[date]):
        rows = list(indexed.get_rows(range_from=date(2015, 5, 6), range_to=date(2015, 5, 8)))
        assert len(rows) > 0
        col_pos = indexed.get_column_position("TRADEDATE")
        for row in rows:
            row_date = cast(date, row[col_pos])
            assert date(2015, 5, 6) <= row_date <= date(2015, 5, 8)

    def test_get_rows_range_from_only(self, indexed: MoexIndexedTable[date]):
        rows = list(indexed.get_rows(range_from=date(2015, 5, 8)))
        assert len(rows) > 0
        col_pos = indexed.get_column_position("TRADEDATE")
        for row in rows:
            row_date = cast(date, row[col_pos])
            assert row_date >= date(2015, 5, 8)

    def test_get_rows_range_to_only(self, indexed: MoexIndexedTable[date]):
        rows = list(indexed.get_rows(range_to=date(2015, 5, 6)))
        assert len(rows) > 0
        col_pos = indexed.get_column_position("TRADEDATE")
        for row in rows:
            row_date = cast(date, row[col_pos])
            assert row_date <= date(2015, 5, 6)

    def test_get_rows_past_end(self, indexed: MoexIndexedTable[date]):
        rows = list(indexed.get_rows(range_from=date(2099, 1, 1)))
        assert len(rows) == 0

    def test_get_rows_non_inclusive_to(self, indexed: MoexIndexedTable[date]):
        rows_inclusive = list(indexed.get_rows(range_to=date(2015, 5, 8), inclusive_to=True))
        rows_exclusive = list(indexed.get_rows(range_to=date(2015, 5, 8), inclusive_to=False))
        # exclusive should have fewer or equal rows
        assert len(rows_exclusive) <= len(rows_inclusive)
        col_pos = indexed.get_column_position("TRADEDATE")
        for row in rows_exclusive:
            row_date = cast(date, row[col_pos])
            assert row_date < date(2015, 5, 8)
