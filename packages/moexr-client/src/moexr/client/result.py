import bisect
import itertools
from collections.abc import Iterator
from datetime import date, datetime, time
from typing import Any, Optional, TypedDict, TypeVar, cast

RawValue = str | int | float | None
RawRow = list[RawValue]

Value = str | int | float | date | time | datetime | None

TLookupValue = TypeVar('TLookupValue', str, int, float, date, time, datetime)


class ColumnMetadataEntry(TypedDict):
    type: str
    bytes: int | None
    max_size: int | None


class _MoexTableResultState(TypedDict):
    metadata: dict[str, ColumnMetadataEntry]
    columns: list[str]
    data: list[RawRow]


class MoexTableResult:
    _metadata: dict[str, ColumnMetadataEntry]
    _columns: list[str]
    _column_index: dict[str, int]
    _data_partitions: list[list[RawRow]]
    _data_offsets: list[int]

    def __init__(self, metadata: dict[str, ColumnMetadataEntry], columns: list[str], partitions: list[list[RawRow]]) -> None:
        self._metadata = metadata
        self._columns = columns
        self._data_partitions = partitions
        self._column_index = dict(zip(self._columns, range(len(self._columns)), strict=True))
        self._rebuild_data_offsets()

    @classmethod
    def from_result(cls, result: dict[str, Any]) -> 'MoexTableResult':
        return cls(result['metadata'], result['columns'], [result['data']])

    @property
    def columns(self) -> list[str]:
        return self._columns

    def has_column(self, column: str) -> bool:
        return column in self._columns

    def get_column_index(self, column: str) -> int:
        if not self.has_column(column):
            raise ValueError(f"table doesn't have column '{column}'")
        return self._column_index[column]

    def get_column_metadata(self, column: str) -> ColumnMetadataEntry:
        if not self.has_column(column):
            raise ValueError(f"table doesn't have column '{column}'")
        return self._metadata[column]

    def get_column(self, column: str) -> list[Value]:
        return list(self.iter_column(column))

    def iter_column(self, column: str) -> Iterator[Value]:
        column_index = self.get_column_index(column)
        column_metadata = self.get_column_metadata(column)
        for row in self.get_rows():
            yield _coerce_value(row[column_index], column, column_metadata)

    def row_count(self) -> int:
        if len(self._data_offsets) == 0:
            return 0
        return self._data_offsets[-1] + len(self._data_partitions[-1])

    def __len__(self) -> int:
        return self.row_count()

    def get_rows(self, index_from: int = 0) -> Iterator[RawRow]:
        if index_from < 0:
            index_from = 0
        partition_index, local_index = self._get_local_index(index_from)
        if partition_index == -1:
            return
        while partition_index < len(self._data_partitions):
            partition = self._data_partitions[partition_index]
            for i in range(local_index, len(partition)):
                yield partition[i]
            partition_index += 1
            local_index = 0

    def get_row(self, row_index: int) -> RawRow:
        partition_index, local_index = self._get_local_index(row_index)
        if partition_index == -1:
            raise ValueError(f"table doesn't have row {row_index}")
        partition = self._data_partitions[partition_index]
        return partition[local_index]

    def get_value(self, row_index: int, column: str) -> Value:
        column_index = self.get_column_index(column)
        column_metadata = self.get_column_metadata(column)
        raw_value = self.get_row(row_index)[column_index]
        return _coerce_value(raw_value, column, column_metadata)

    def bisect_left(self, lookup_value: TLookupValue, column: str, exact_match: bool) -> Optional[int]:
        """Lower-bound binary search over the given column.

        Semantics:
        - exact_match=True: return the index of the row whose value equals the lookup value,
          otherwise None if not found.
        - exact_match=False: return the index of the first row whose value is >= the lookup value,
          otherwise None if the lookup value is after the last value.
        """
        column_index = self.get_column_index(column)
        column_metadata = self.get_column_metadata(column)

        def get_value_by_index(row_index: int) -> TLookupValue:
            row = self.get_row(row_index)
            raw_value = row[column_index]
            if raw_value is None:
                raise ValueError(f"column '{column}' contains null values, cannot perform binary search")
            return cast(TLookupValue, _coerce_value(raw_value, column, column_metadata))

        count = self.row_count()
        if count == 0:
            return None

        # Search range is [lo, hi) over indices [0, count)
        lo, hi = 0, count
        while lo < hi:
            mid = (lo + hi) // 2
            mid_value = get_value_by_index(mid)
            if mid_value < lookup_value:
                lo = mid + 1
            else:
                hi = mid

        insertion = lo  # the first index with value >= lookup value, may be == count (past the end)

        if insertion == count:
            # No value >= lookup value exists (lookup value is after the last available value)
            return None

        found_value = get_value_by_index(insertion)
        if exact_match:
            return insertion if found_value == lookup_value else None

        # Inexact: return the first index with value >= lookup value
        return insertion

    def extend(self, other: 'MoexTableResult'):
        for partition in other._data_partitions:
            self._data_partitions.append(partition)
        self._rebuild_data_offsets()

    def concat(self, other: 'MoexTableResult') -> 'MoexTableResult':
        return MoexTableResult(self._metadata, self._columns, [
            *self._data_partitions,
            *other._data_partitions,
        ])

    def take(self, n: int) -> 'MoexTableResult':
        if n < 0:
            raise ValueError("n must be positive")

        if n >= self.row_count():
            return self

        remaining = n
        partitions: list[list[RawRow]] = []
        for partition in self._data_partitions:
            if remaining <= 0:
                break

            partition_len = len(partition)
            if partition_len == 0:
                continue

            if partition_len > remaining:
                partitions.append(partition[:remaining])
                break
            else:
                partitions.append(partition)
                remaining -= partition_len

        return MoexTableResult(self._metadata, self._columns, partitions)

    def __getstate__(self) -> _MoexTableResultState:
        self._flatten_data()
        state: _MoexTableResultState = {
            'metadata': self._metadata,
            'columns': self._columns,
            'data': self._data_partitions[0],
        }
        return state

    def __setstate__(self, state: _MoexTableResultState):
        self._metadata = state['metadata']
        self._columns = state['columns']
        self._data_partitions = [state['data']]
        self._column_index = dict(zip(self._columns, range(len(self._columns)), strict=True))
        self._rebuild_data_offsets()

    def _flatten_data(self):
        if len(self._data_partitions) > 1:
            self._data_partitions = [list(itertools.chain.from_iterable(self._data_partitions))]
            self._rebuild_data_offsets()

    def _rebuild_data_offsets(self):
        offsets: list[int] = []
        total_count = 0
        for partition in self._data_partitions:
            offsets.append(total_count)
            total_count += len(partition)
        self._data_offsets = offsets

    def _get_local_index(self, row_index: int) -> tuple[int, int]:
        partition_index = bisect.bisect_right(self._data_offsets, row_index) - 1
        if partition_index < 0:
            return -1, -1
        local_index = row_index - self._data_offsets[partition_index]
        if local_index >= len(self._data_partitions[partition_index]):
            return -1, -1
        return partition_index, local_index


def _coerce_value(raw_value: RawValue, column: str, metadata: ColumnMetadataEntry) -> Value:
    # All columns can have null values
    if raw_value is None:
        return None

    column_type = metadata['type']

    if column_type == 'string':
        if type(raw_value) is str:
            return raw_value
    elif column_type == 'int32' or column_type == 'int64':
        if type(raw_value) is int:
            return raw_value
        elif type(raw_value) is float:
            return int(raw_value)
    elif column_type == 'double':
        if type(raw_value) is float:
            return raw_value
        elif type(raw_value) is int:
            return float(raw_value)
    elif column_type == 'date':
        if type(raw_value) is str:
            return date.fromisoformat(raw_value)
    elif column_type == 'time':
        if type(raw_value) is str:
            return time.fromisoformat(raw_value)
    elif column_type == 'datetime':
        if type(raw_value) is str:
            return datetime.fromisoformat(raw_value)
    else:
        raise ValueError(f"column '{column}' has unknown type '{column_type}'")

    # Catch all unexpected values
    raise ValueError(f"column '{column}' of type '{column_type}' does not allow value '{raw_value}' ({type(raw_value).__name__})")
