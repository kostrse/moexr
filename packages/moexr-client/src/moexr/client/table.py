import bisect
import itertools
import sys
from collections.abc import Iterator
from datetime import date, datetime, time
from typing import Any, Generic, Self, TypedDict, TypeVar, cast

Value = str | int | float | date | time | datetime | None
Row = list[Value]

if sys.version_info >= (3, 12):
    IndexT = TypeVar("IndexT", str, int, float, date, time, datetime, default=int)
else:
    IndexT = TypeVar("IndexT", str, int, float, date, time, datetime)


class ColumnMetadataEntry(TypedDict):
    """Column type and size information from the MOEX ISS metadata block."""

    type: str
    bytes: int | None
    max_size: int | None


class _MoexTableState(TypedDict):
    metadata: dict[str, ColumnMetadataEntry]
    columns: list[str]
    data: list[Row]


class MoexTable:
    """Columnar table of MOEX ISS API results.

    Stores rows in partitions for efficient concatenation and supports
    positional access by row index and column name. Values are automatically
    coerced to native Python types (``date``, ``time``, ``datetime``, etc.)
    when constructed via :meth:`from_result`.
    """

    _metadata: dict[str, ColumnMetadataEntry]
    _columns: list[str]
    _columns_pos: dict[str, int]
    _data_partitions: list[list[Row]]
    _data_offsets: list[int]

    def __init__(self, metadata: dict[str, ColumnMetadataEntry], columns: list[str], partitions: list[list[Row]]) -> None:
        self._metadata = metadata
        self._columns = columns
        self._columns_pos = dict(zip(self._columns, range(len(self._columns)), strict=True))
        self._data_partitions = partitions
        self._rebuild_data_offsets()

    @classmethod
    def from_result(cls, result: dict[str, Any]) -> Self:
        """Construct a table from a raw MOEX ISS JSON result block.

        *result* must contain ``metadata``, ``columns``, and ``data`` keys.
        Values are coerced to native Python types according to column metadata.
        """
        metadata = result["metadata"]
        columns = result["columns"]
        rows = result["data"]
        _coerce_result(metadata, columns, rows)
        return cls(metadata, columns, [rows])

    @property
    def columns(self) -> list[str]:
        """Return the list of column names."""
        return self._columns

    def has_column(self, column: str) -> bool:
        """Return whether the table contains a column with the given name."""
        return column in self._columns

    def get_column_position(self, column: str) -> int:
        """Return the zero-based index of *column*.

        Raises:
            ValueError: If the column does not exist.
        """
        if not self.has_column(column):
            raise ValueError(f"table doesn't have column '{column}'")
        return self._columns_pos[column]

    def get_column_metadata(self, column: str) -> ColumnMetadataEntry:
        """Return the metadata entry for *column*.

        Raises:
            ValueError: If the column does not exist.
        """
        if not self.has_column(column):
            raise ValueError(f"table doesn't have column '{column}'")
        return self._metadata[column]

    def row_count(self) -> int:
        """Return the total number of rows across all partitions."""
        if len(self._data_offsets) == 0:
            return 0
        return self._data_offsets[-1] + len(self._data_partitions[-1])

    def __len__(self) -> int:
        return self.row_count()

    def get_row(self, position: int) -> Row:
        """Return the row at the given zero-based *position*.

        Raises:
            IndexError: If *position* is out of range.
        """
        partition_index, local_index = self._get_local_index(position)
        if partition_index == -1:
            raise IndexError(f"row position {position} is out of range (0..{self.row_count() - 1})")
        return self._data_partitions[partition_index][local_index]

    def get_value(self, position: int, column: str) -> Value:
        """Return the value at the given row *position* and *column* name."""
        row = self.get_row(position)
        col_pos = self.get_column_position(column)
        return row[col_pos]

    def get_rows(self) -> Iterator[Row]:
        """Return an iterator over all rows in order."""
        for partition in self._data_partitions:
            yield from partition

    def extend(self, other: Self) -> None:
        """Append all rows from *other* into this table in place."""
        for partition in other._data_partitions:
            self._data_partitions.append(partition)
        self._rebuild_data_offsets()

    def concat(self, other: Self) -> Self:
        """Return a new table with rows from this table followed by rows from *other*."""
        return type(self)(
            self._metadata,
            self._columns,
            [
                *self._data_partitions,
                *other._data_partitions,
            ],
        )

    def take(self, n: int) -> Self:
        """Return a new table containing at most the first *n* rows.

        Raises:
            ValueError: If *n* is negative.
        """
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

        return type(self)(self._metadata, self._columns, partitions)

    def __getstate__(self) -> _MoexTableState:
        self._flatten_data()
        state: _MoexTableState = {
            "metadata": self._metadata,
            "columns": self._columns,
            "data": self._data_partitions[0],
        }
        return state

    def __setstate__(self, state: _MoexTableState) -> None:
        self._metadata = state["metadata"]
        self._columns = state["columns"]
        self._data_partitions = [state["data"]]
        self._columns_pos = dict(zip(self._columns, range(len(self._columns)), strict=True))
        self._rebuild_data_offsets()

    def _get_local_index(self, position: int) -> tuple[int, int]:
        partition_index = bisect.bisect_right(self._data_offsets, position) - 1
        if partition_index < 0:
            return -1, -1
        local_index = position - self._data_offsets[partition_index]
        if local_index >= len(self._data_partitions[partition_index]):
            return -1, -1
        return partition_index, local_index

    def _flatten_data(self) -> None:
        if len(self._data_partitions) > 1:
            self._data_partitions = [list(itertools.chain.from_iterable(self._data_partitions))]
            self._rebuild_data_offsets()

    def _rebuild_data_offsets(self) -> None:
        offsets: list[int] = []
        total_count = 0
        for partition in self._data_partitions:
            offsets.append(total_count)
            total_count += len(partition)
        self._data_offsets = offsets


class _MoexIndexedTableState(TypedDict):
    table: _MoexTableState
    index_column: str


class MoexIndexedTable(Generic[IndexT]):
    """Typed, key-based view over a :class:`MoexTable` with a sorted index column.

    Provides key lookup and range queries via binary search on the
    *index_column*. The underlying table is available via the :attr:`table`
    property for positional access.

    Example::

        table = await client.req_table(path, 'history')
        indexed: MoexIndexedTable[date] = MoexIndexedTable(table, 'TRADEDATE')
        row = indexed.get_row(date(2024, 6, 15))
    """

    _table: MoexTable
    _index_column: str
    _index_column_pos: int

    def __init__(self, table: MoexTable, index_column: str) -> None:
        self._table = table
        self._index_column = index_column
        try:
            self._index_column_pos = table.get_column_position(index_column)
        except ValueError:
            raise ValueError(f"table doesn't have index column '{index_column}'") from None

    @property
    def table(self) -> MoexTable:
        """Return the underlying table for positional access."""
        return self._table

    @property
    def columns(self) -> list[str]:
        """Return the list of column names."""
        return self._table.columns

    @property
    def index_column(self) -> str:
        """Return the name of the index column."""
        return self._index_column

    def has_column(self, column: str) -> bool:
        """Return whether the table contains a column with the given name."""
        return self._table.has_column(column)

    def get_column_position(self, column: str) -> int:
        """Return the zero-based index of *column*.

        Raises:
            ValueError: If the column does not exist.
        """
        return self._table.get_column_position(column)

    def get_column_metadata(self, column: str) -> ColumnMetadataEntry:
        """Return the metadata entry for *column*.

        Raises:
            ValueError: If the column does not exist.
        """
        return self._table.get_column_metadata(column)

    def row_count(self) -> int:
        """Return the total number of rows."""
        return self._table.row_count()

    def __len__(self) -> int:
        return self._table.row_count()

    def get_row(self, key: IndexT) -> Row | None:
        """Return the row matching *key*, or ``None`` if not found."""
        position = self._bisect_left(key, exact_match=True)
        if position is None:
            return None
        return self._table.get_row(position)

    def get_value(self, key: IndexT, column: str) -> Value | None:
        """Return the value at *key* and *column*, or ``None`` if not found."""
        row = self.get_row(key)
        if row is None:
            return None
        col_pos = self._table.get_column_position(column)
        return row[col_pos]

    def get_rows(self, range_from: IndexT | None = None, range_to: IndexT | None = None, inclusive_to: bool = True) -> Iterator[Row]:
        """Return an iterator over rows within the given index key range.

        Args:
            range_from: Lower bound (inclusive). ``None`` starts from the first row.
            range_to: Upper bound. ``None`` iterates to the last row.
            inclusive_to: If ``True`` (default), *range_to* is inclusive.
        """
        count = self._table.row_count()
        if count == 0:
            return

        # Determine start position
        if range_from is not None:
            start = self._bisect_left(range_from, exact_match=False)
            if start is None:
                return
        else:
            start = 0

        # Determine end position (exclusive)
        if range_to is not None:
            end = self._bisect_right(range_to)
            if not inclusive_to:
                # For non-inclusive upper bound, use bisect_left instead
                end_left = self._bisect_left(range_to, exact_match=False)
                if end_left is None:
                    end = count
                else:
                    end = end_left
        else:
            end = count

        for position in range(start, end):
            yield self._table.get_row(position)

    def _bisect_left(self, value: IndexT, exact_match: bool) -> int | None:
        """Lower-bound binary search over the index column.

        - exact_match=True: return the position of the row whose index value equals the
          lookup value, or None if not found.
        - exact_match=False: return the position of the first row whose index value
          is >= the lookup value, or None if the lookup value is past the last row.
        """
        count = self._table.row_count()
        if count == 0:
            return None

        lo, hi = 0, count
        while lo < hi:
            mid = (lo + hi) // 2
            mid_value = self._get_index_value(mid)
            if mid_value < value:
                lo = mid + 1
            else:
                hi = mid

        insertion = lo

        if insertion == count:
            return None

        if exact_match:
            found_value = self._get_index_value(insertion)
            return insertion if found_value == value else None

        return insertion

    def _bisect_right(self, value: IndexT) -> int:
        """Upper-bound binary search: return the position after the last row
        whose index value is <= the lookup value."""
        count = self._table.row_count()
        if count == 0:
            return 0

        lo, hi = 0, count
        while lo < hi:
            mid = (lo + hi) // 2
            mid_value = self._get_index_value(mid)
            if mid_value <= value:
                lo = mid + 1
            else:
                hi = mid

        return lo

    def _get_index_value(self, position: int) -> IndexT:
        row = self._table.get_row(position)
        value = row[self._index_column_pos]
        if value is None:
            raise ValueError(f"index column contains null value at row {position}, cannot perform binary search")
        return cast(IndexT, value)

    def __getstate__(self) -> _MoexIndexedTableState:
        return {
            "table": self._table.__getstate__(),
            "index_column": self._index_column,
        }

    def __setstate__(self, state: _MoexIndexedTableState) -> None:
        table = MoexTable.__new__(MoexTable)
        table.__setstate__(state["table"])
        self._table = table
        self._index_column = state["index_column"]
        self._index_column_pos = table.get_column_position(state["index_column"])


_UNCHANGED = object()


def _coerce_result(metadata: dict[str, ColumnMetadataEntry], columns: list[str], rows: list[Row]) -> None:
    if type(metadata) is not dict:
        raise ValueError(f"metadata should be a dict, not '{type(metadata).__name__}'")
    if type(columns) is not list:
        raise ValueError(f"columns should be a list, not '{type(columns).__name__}'")
    if type(rows) is not list:
        raise ValueError(f"rows should be a list, not '{type(rows).__name__}'")

    col_count = len(columns)
    if col_count == 0:
        return

    row_count = len(rows)
    if row_count == 0:
        return

    for row_index in range(row_count):
        row = rows[row_index]

        row_changed = False

        for column_index in range(col_count):
            column_name = columns[column_index]
            column_metadata = metadata[column_name]
            raw_value = row[column_index]

            value = _coerce_value(raw_value, column_name, column_metadata)
            if value is _UNCHANGED:
                continue

            row[column_index] = cast(Value, value)
            row_changed = True

        if row_changed:
            rows[row_index] = row


def _coerce_value(raw_value: Value, column: str, metadata: ColumnMetadataEntry) -> Value | object:
    # All columns can have null values
    if raw_value is None:
        return _UNCHANGED

    column_type = metadata["type"]

    if column_type == "string":
        if type(raw_value) is str:
            return _UNCHANGED
    elif column_type == "int32" or column_type == "int64":
        if type(raw_value) is int:
            return _UNCHANGED
    elif column_type == "double":
        if type(raw_value) is float:
            return _UNCHANGED
        elif type(raw_value) is int:
            return float(raw_value)
    elif column_type == "date":
        if type(raw_value) is date:
            return _UNCHANGED
        if type(raw_value) is str:
            if raw_value == "0000-00-00":
                return None
            return date.fromisoformat(raw_value)
    elif column_type == "time":
        if type(raw_value) is time:
            return _UNCHANGED
        if type(raw_value) is str:
            return time.fromisoformat(raw_value)
    elif column_type == "datetime":
        if type(raw_value) is datetime:
            return _UNCHANGED
        if type(raw_value) is str:
            return datetime.fromisoformat(raw_value)
    else:
        raise ValueError(f"column '{column}' has unknown type '{column_type}'")

    # Catch all unexpected values
    raise ValueError(f"column '{column}' of type '{column_type}' does not allow value '{raw_value}' ({type(raw_value).__name__})")
