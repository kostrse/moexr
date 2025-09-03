import bisect
import itertools
from collections.abc import Iterator
from datetime import date
from typing import Any, TypedDict

RowValue = Any
Row = list[RowValue]


class ColumnMetadataEntry(TypedDict):
    type: str
    bytes: int | None
    max_size: int | None


class MoexTableResultState(TypedDict):
    metadata: dict[str, ColumnMetadataEntry]
    columns: list[str]
    data: list[Row]


class MoexTableResult:
    _metadata: dict[str, ColumnMetadataEntry]
    _columns: list[str]
    _column_index: dict[str, int]
    _data_partitions: list[list[Row]]
    _data_offsets: list[int]

    def __init__(self, metadata: dict[str, ColumnMetadataEntry], columns: list[str], partitions: list[list[Row]]) -> None:
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
            raise ValueError(f"The table doesn't have column '{column}'.")
        return self._column_index[column]

    def get_column_metadata(self, column: str) -> ColumnMetadataEntry:
        if not self.has_column(column):
            raise ValueError(f"The table doesn't have column '{column}'.")
        return self._metadata[column]

    def get_column(self, column: str) -> list[Any]:
        return list(self.iter_column(column))

    def iter_column(self, column: str) -> Iterator[Any]:
        column_index = self.get_column_index(column)
        for row in self.get_rows():
            yield row[column_index]

    def row_count(self) -> int:
        if len(self._data_offsets) == 0:
            return 0
        return self._data_offsets[-1] + len(self._data_partitions[-1])

    def __len__(self) -> int:
        return self.row_count()

    def get_rows(self, index_from: int = 0) -> Iterator[Row]:
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

    def get_row(self, row_index: int) -> Row:
        partition_index, local_index = self._get_local_index(row_index)
        if partition_index == -1:
            raise ValueError(f"The table doesn't have row {row_index}.")
        partition = self._data_partitions[partition_index]
        return partition[local_index]

    def get_value(self, row_index: int, column: str) -> RowValue:
        column_index = self.get_column_index(column)
        return self.get_row(row_index)[column_index]

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
        partitions: list[list[Row]] = []
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

    def __getstate__(self) -> MoexTableResultState:
        self._flatten_data()
        state: MoexTableResultState = {
            'metadata': self._metadata,
            'columns': self._columns,
            'data': self._data_partitions[0],
        }
        return state

    def __setstate__(self, state: MoexTableResultState):
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


def to_properties(table: MoexTableResult) -> dict[str, Any]:
    property_name_index = table.get_column_index('name')
    property_value_index = table.get_column_index('value')
    property_type_index = table.get_column_index('type')
    property_precision_index = table.get_column_index('precision')

    properties: dict[str, RowValue] = {}

    for row in table.get_rows():
        property_name = row[property_name_index]
        property_value = row[property_value_index]
        property_type = row[property_type_index]
        property_precision = row[property_precision_index]

        if property_type == 'string':
            pass
        elif property_type == 'number':
            if property_precision == 0:
                property_value = int(property_value)
            else:
                property_value = float(property_value)
        elif property_type == 'boolean':
            property_value = property_value == '1'
        elif property_type == 'date':
            property_value = date.fromisoformat(property_value)
        else:
            raise ValueError(f"The property '{property_name}' has unknown type '{property_type}'")

        properties[property_name] = property_value

    return properties
