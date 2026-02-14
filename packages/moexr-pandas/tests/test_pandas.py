import json
from datetime import date
from typing import cast

import numpy as np
import pytest
from conftest import FIXTURES_DIR
from moexr.client import MoexIndexedTable, MoexTable
from moexr.pandas import to_dataframe


def _create_table() -> MoexTable:
    result = {
        "metadata": {
            "ID": {"type": "int32", "bytes": 4, "max_size": None},
            "SECID": {"type": "string", "bytes": None, "max_size": 16},
        },
        "columns": ["ID", "SECID"],
        "data": [
            [1, "SBER"],
            [2, "GAZP"],
        ],
    }
    return MoexTable.from_result(result)


def test_to_dataframe_accepts_moex_table() -> None:
    table = _create_table()

    result = to_dataframe(table)

    assert list(result.columns) == ["ID", "SECID"]
    assert result.shape == (2, 2)
    assert result.iloc[0].to_list() == [1, "SBER"]
    assert result.iloc[1].to_list() == [2, "GAZP"]


def test_to_dataframe_accepts_moex_indexed_table() -> None:
    table = _create_table()
    indexed: MoexIndexedTable[int] = MoexIndexedTable(table, "ID")

    result = to_dataframe(indexed)

    assert list(result.columns) == ["ID", "SECID"]
    assert result.index.to_list() == [1, 2]
    assert result.iloc[0].to_list() == [1, "SBER"]
    assert result.iloc[1].to_list() == [2, "GAZP"]


def test_to_dataframe_rejects_index_column_for_moex_indexed_table() -> None:
    table = _create_table()
    indexed: MoexIndexedTable[int] = MoexIndexedTable(table, "ID")

    with pytest.raises(ValueError, match="index_column must not be provided for MoexIndexedTable"):
        to_dataframe(indexed, index_column="ID")


def test_to_dataframe_excludes_index_column_for_moex_indexed_table() -> None:
    table = _create_table()
    indexed: MoexIndexedTable[int] = MoexIndexedTable(table, "ID")

    result = to_dataframe(indexed, exclude_index_column=True)

    assert list(result.columns) == ["SECID"]
    assert result.index.to_list() == [1, 2]


class TestToDataframeWithHistoryFixture:
    @staticmethod
    def _load_history_table() -> MoexTable:
        path = FIXTURES_DIR / "history.json"
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return MoexTable.from_result(data["history"])

    def test_shape_matches_source(self) -> None:
        table = self._load_history_table()
        df = to_dataframe(table)

        assert df.shape[0] == table.row_count()
        assert list(df.columns) == table.columns

    def test_date_column_type(self) -> None:
        table = self._load_history_table()
        df = to_dataframe(table)

        assert df["TRADEDATE"].iloc[0] == date(2015, 5, 5)

    def test_float_column_with_nulls(self) -> None:
        table = self._load_history_table()
        df = to_dataframe(table)

        # Row 0 has CLOSE=null, row 2 has a real value
        assert np.isnan(cast(float, df["CLOSE"].iloc[0]))
        assert df["CLOSE"].iloc[2] == 79.4

    def test_string_column(self) -> None:
        table = self._load_history_table()
        df = to_dataframe(table)

        assert df["SECID"].iloc[0] == "MOEX"

    def test_int_column(self) -> None:
        table = self._load_history_table()
        df = to_dataframe(table)

        assert df["TRADINGSESSION"].iloc[0] == 3

    def test_with_index_column(self) -> None:
        table = self._load_history_table()
        df = to_dataframe(table, index_column="TRADEDATE")

        assert df.index[0] == date(2015, 5, 5)
        assert "TRADEDATE" in df.columns  # not excluded

    def test_indexed_table_matches_plain_table(self) -> None:
        table = self._load_history_table()
        indexed: MoexIndexedTable[date] = MoexIndexedTable(table, "TRADEDATE")

        df_plain = to_dataframe(table, index_column="TRADEDATE")
        df_indexed = to_dataframe(indexed)

        assert df_indexed.equals(df_plain)
