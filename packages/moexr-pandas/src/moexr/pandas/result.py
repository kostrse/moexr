import inspect
from datetime import date
from typing import Any, Optional

import numpy as np
from moexr.client import MoexTableResult

import pandas as pd


def to_dataframe(table: MoexTableResult, index_column: Optional[str] = None,
                 exclude_index_column: bool = False) -> pd.DataFrame:
    if table is None: # type: ignore
        raise TypeError("table must be provided")

    if not isinstance(table, MoexTableResult): # type: ignore
        if inspect.isawaitable(table):
            raise TypeError("expected MoexTableResult, got awaitable (did you forget to await?)")
        else:
            raise TypeError(f"table must be MoexTableResult, not {type(table).__name__}")

    if index_column is not None:
        if not isinstance(index_column, str): # type: ignore
            raise TypeError(f"index_column must be str, not {type(index_column).__name__}")
        if index_column not in table.columns:
            raise ValueError(f"index column '{index_column}' not found in table")

    row_count = table.row_count()

    col_type: list[tuple[str, str]] = []
    col_arr: list[np.ndarray] = []
    for column_name in table.columns:
        metadata = table.get_column_metadata(column_name)
        column_type = metadata['type']
        numpy_type = _get_column_numpy_type(column_name, column_type)
        col_type.append((column_type, numpy_type))
        col_arr.append(np.empty(row_count, dtype=numpy_type))

    col_count = len(col_arr)
    for row_index, row in enumerate(table.get_rows()):
        for col_index in range(col_count):
            column_type, numpy_type = col_type[col_index]
            col_arr[col_index][row_index] = _get_formatted_value(row[col_index], column_type, numpy_type)

    col_dict = dict(zip(table.columns, col_arr, strict=True))

    idx_col: Optional[np.ndarray] = None
    if index_column is not None:
        if exclude_index_column:
            idx_col = col_dict.pop(index_column)
        else:
            idx_col = col_dict[index_column]

    return pd.DataFrame(col_dict, index=idx_col)


def _get_column_numpy_type(column_name: str, column_type: str) -> str:
    if column_type == 'string':
        return 'O'
    elif column_type == 'int32':
        return 'i4'
    elif column_type == 'int64':
        return 'i8'
    elif column_type == 'double':
        return 'f8'
    elif column_type == 'date':
        return 'O'
    elif column_type == 'time':
        return 'O'
    elif column_type == 'datetime':
        return 'datetime64[ns]'
    elif column_type == 'undefined':
        return 'O'
    else:
        raise ValueError(f"column '{column_name}' has unknown type '{column_type}'")


def _get_formatted_value(value: Any, column_type: str, numpy_type: str) -> Any:
    if column_type == 'date':
        if value is None or value == '0000-00-00':
            return None
        else:
            return date.fromisoformat(value)
    elif column_type == 'datetime':
        if numpy_type == 'datetime64[ns]':
            if value is None:
                return np.datetime64('NaT')
            else:
                return value
        else:
            return value
    else:
        if numpy_type in ['i4', 'i8'] and value is None:
            return -1
        elif numpy_type == 'f8' and value is None:
            return np.nan
        else:
            return value
