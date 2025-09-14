from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from moexr.client.result import MoexTableResult

DATA_DIR = Path(__file__).parent / "data"


def load_json(file_name: str, table_name: str) -> dict:
    path = DATA_DIR / file_name
    with path.open("r", encoding="utf-8") as f:
        j = json.load(f)
        return j[table_name]


class TestMoexTableResult:
    @pytest.fixture(scope="class")
    def history_json(self) -> dict:
        return load_json("history.json", "history")

    @pytest.fixture(scope="class")
    def table(self, history_json: dict) -> MoexTableResult:
        return MoexTableResult.from_result(history_json)  # type: ignore[arg-type]

    def test_row_count(self, history_json: dict, table: MoexTableResult):
        assert table.row_count() == len(history_json["data"])

    def test_columns(self, history_json: dict, table: MoexTableResult):
        assert len(table.columns) == len(history_json["columns"])  # columns

        # Spot check a few expected columns
        for column in ("BOARDID", "TRADEDATE", "SECID", "CLOSE"):
            assert table.has_column(column)

    def test_value_types(self, history_json: dict, table: MoexTableResult):
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

    def test_value_nulls(self, table: MoexTableResult):
        price_value = table.get_value(0, "CLOSE")
        assert price_value is None


class TestBinarySearch:
    @pytest.fixture(scope="class")
    def history_json(self) -> dict:
        return load_json("history.json", "history")

    @pytest.fixture(scope="class")
    def table(self, history_json: dict) -> MoexTableResult:
        return MoexTableResult.from_result(history_json)  # type: ignore[arg-type]

    def test_bisect_left_exact(self, table: MoexTableResult):
        column = 'TRADEDATE'
        lookup_date = date(2015, 5, 8)

        idx = table.bisect_left(lookup_date, column, exact_match=True)
        assert idx is not None
        assert table.get_value(idx, column) == lookup_date

    def test_bisect_left_exact_not_found(self, table: MoexTableResult):
        column = 'TRADEDATE'
        lookup_date = date(2015, 5, 10)

        idx = table.bisect_left(lookup_date, column, exact_match=True)
        assert idx is None

    def test_bisect_left_inexact(self, table: MoexTableResult):
        column = 'TRADEDATE'
        lookup_date = date(2015, 5, 10)

        idx = table.bisect_left(lookup_date, column, exact_match=False)
        assert idx is not None
        assert table.get_value(idx, column) >= lookup_date

    def test_bisect_left_early(self, table: MoexTableResult):
        column = 'TRADEDATE'
        lookup_date = date(2015, 4, 15)

        idx = table.bisect_left(lookup_date, column, exact_match=False)
        assert idx is not None
        assert table.get_value(idx, column) >= table.get_value(0, column)

    def test_bisect_left_late(self, table: MoexTableResult):
        column = 'TRADEDATE'
        lookup_date = date(2015, 6, 15)

        idx = table.bisect_left(lookup_date, column, exact_match=False)
        assert idx is None
