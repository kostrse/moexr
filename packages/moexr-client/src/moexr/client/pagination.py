from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LimitOnly:
    limit_sizes: list[int]

    def __post_init__(self) -> None:
        if len(self.limit_sizes) == 0:
            raise ValueError("limit_sizes must be a non-empty list")
        if any(value <= 0 for value in self.limit_sizes):
            raise ValueError("limit_sizes values must be positive integers")
        if self.limit_sizes != sorted(self.limit_sizes):
            raise ValueError("limit_sizes must be sorted in ascending order")


@dataclass(frozen=True)
class OffsetPagination:
    limit_sizes: list[int] | None = None

    def __post_init__(self) -> None:
        if self.limit_sizes is None:
            return

        if len(self.limit_sizes) == 0:
            raise ValueError("limit_sizes must be a non-empty list")
        if any(value <= 0 for value in self.limit_sizes):
            raise ValueError("limit_sizes values must be positive integers")
        if self.limit_sizes != sorted(self.limit_sizes):
            raise ValueError("limit_sizes must be sorted in ascending order")


@dataclass(frozen=True)
class DatePagination:
    date_column: str

    def __post_init__(self) -> None:
        if not self.date_column:
            raise ValueError("date_column must be a non-empty string")


Pagination = LimitOnly | OffsetPagination | DatePagination
