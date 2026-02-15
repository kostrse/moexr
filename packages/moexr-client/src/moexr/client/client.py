import asyncio
import bisect
from datetime import date, datetime, timedelta
from types import TracebackType
from typing import Any, Self, cast

import aiohttp

from .error import MoexClientError, PaginationError
from .pagination import DatePagination, LimitOnly, OffsetPagination, Pagination
from .table import MoexTable

_MAX_PAGES = 10_000


class MoexClient:
    def __init__(self, access_token: str | None = None, lang: str = "ru"):
        if access_token is None:
            self._client_session = aiohttp.ClientSession(base_url="https://iss.moex.com/")
        else:
            headers = {
                "Authorization": "Bearer " + access_token,
            }
            self._client_session = aiohttp.ClientSession(base_url="https://apim.moex.com/", headers=headers)

        self._req_semaphore = asyncio.Semaphore(4)
        self._closed = False
        self._lang = lang

    async def close(self) -> None:
        if not self._closed:
            self._closed = True
            await self._client_session.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def req(self, path: list[str], query: dict[str, Any] | None = None) -> dict[str, MoexTable]:
        return await self._req(path, query)

    async def req_table(
        self,
        path: list[str],
        table_name: str,
        query: dict[str, Any] | None = None,
        *,
        paginate: Pagination | None = None,
        limit: int | None = None,
    ) -> MoexTable:
        if limit is not None and limit <= 0:
            raise ValueError("limit must be a positive integer")

        if paginate is None:
            return await self._req_table(path, table_name, query, limit)
        if isinstance(paginate, LimitOnly):
            return await self._req_limit_only(path, table_name, query, paginate, limit)
        if isinstance(paginate, OffsetPagination):
            return await self._req_offset_paginated(path, table_name, query, paginate, limit)

        pagination = cast(object, paginate)
        if isinstance(pagination, DatePagination):
            return await self._req_date_paginated(path, table_name, query, pagination, limit)

        raise TypeError(f"paginate must be LimitOnly, OffsetPagination, DatePagination, or None; got {type(pagination).__name__}")

    async def _req_table(
        self, path: list[str], table_name: str, query: dict[str, Any] | None = None, limit: int | None = None
    ) -> MoexTable:
        query_params: dict[str, Any] = {}
        if query:
            query_params.update({table_name + "." + key: value for key, value in query.items()})

        query_params["iss.only"] = table_name

        result = await self._req(path, query_params)
        table = result[table_name]

        if limit is not None and len(table) > limit:
            return table.take(limit)

        return table

    async def _req_limit_only(
        self,
        path: list[str],
        table_name: str,
        query: dict[str, Any] | None,
        strategy: LimitOnly,
        limit: int | None,
    ) -> MoexTable:
        _validate_pagination_query(query, {"limit"})

        limit_sizes = strategy.limit_sizes
        max_limit = limit_sizes[-1]

        req_limit = max_limit
        if limit is not None and limit < max_limit:
            req_limit = _snap_limit(limit_sizes, limit)

        query_params = dict(query or {})
        query_params["limit"] = req_limit

        return await self._req_table(path, table_name, query_params, limit)

    async def _req_offset_paginated(
        self,
        path: list[str],
        table_name: str,
        query: dict[str, Any] | None,
        strategy: OffsetPagination,
        limit: int | None,
    ) -> MoexTable:
        reserved_keys = {"start"}
        if strategy.limit_sizes is not None:
            reserved_keys.add("limit")
        _validate_pagination_query(query, reserved_keys)

        offset = 0
        remaining = limit
        limit_sizes = strategy.limit_sizes
        default_page_size = max(limit_sizes) if limit_sizes is not None else None

        merged_result: MoexTable | None = None

        for _ in range(_MAX_PAGES):
            query_params = dict(query or {})
            query_params["start"] = offset

            req_limit: int | None = None
            if limit_sizes is not None:
                assert default_page_size is not None
                req_limit = default_page_size
                if remaining is not None and remaining < default_page_size:
                    req_limit = _snap_limit(limit_sizes, remaining)
                query_params["limit"] = req_limit

            page = await self._req_table(path, table_name, query_params, limit=None)
            if merged_result is None:
                merged_result = page.take(0)

            page_count = len(page)
            if page_count == 0:
                break

            raw_page_count = page_count
            if remaining is not None and page_count > remaining:
                page = page.take(remaining)
                page_count = remaining

            merged_result.extend(page)

            if remaining is not None:
                remaining -= page_count
                if remaining == 0:
                    break

            if req_limit is not None and raw_page_count < req_limit:
                break

            next_offset = offset + raw_page_count
            if next_offset <= offset:
                raise PaginationError("offset pagination did not advance")
            offset = next_offset
        else:
            raise PaginationError(f"pagination exceeded maximum page count ({_MAX_PAGES})")

        assert merged_result is not None
        return merged_result

    async def _req_date_paginated(
        self,
        path: list[str],
        table_name: str,
        query: dict[str, Any] | None,
        strategy: DatePagination,
        limit: int | None,
    ) -> MoexTable:
        _validate_pagination_query(query, {"start", "limit"})

        boundary: date | None = None
        remaining = limit
        merged_result: MoexTable | None = None

        for _ in range(_MAX_PAGES):
            query_params = dict(query or {})
            if boundary is not None:
                query_params["from"] = boundary

            page = await self._req_table(path, table_name, query_params, limit=None)
            if merged_result is None:
                merged_result = page.take(0)

            page_count = len(page)
            if page_count == 0:
                break

            if remaining is not None and page_count > remaining:
                page = page.take(remaining)
                page_count = remaining

            merged_result.extend(page)

            if remaining is not None:
                remaining -= page_count
                if remaining == 0:
                    break

            max_date = _get_max_page_date(page, strategy.date_column)
            next_boundary = max_date + timedelta(days=1)
            if boundary is not None and next_boundary <= boundary:
                raise PaginationError("date pagination boundary did not advance")
            boundary = next_boundary
        else:
            raise PaginationError(f"pagination exceeded maximum page count ({_MAX_PAGES})")

        assert merged_result is not None
        return merged_result

    async def _req(self, path: list[str], query: dict[str, Any] | None) -> dict[str, MoexTable]:
        if self._closed:
            raise MoexClientError("client is closed")

        query_params: dict[str, Any] = {}
        if query:
            query_params.update({key: _format_query(value) for key, value in query.items() if value is not None})

        query_params["lang"] = self._lang

        async with self._req_semaphore:
            try:
                async with self._client_session.get("/iss/" + "/".join(path) + ".json", params=query_params) as resp:
                    if resp.status != 200:
                        raise MoexClientError(f"request failed with status code: {resp.status}")

                    result = await resp.json()
                    return {key: MoexTable.from_result(value) for key, value in result.items()}
            except Exception as e:
                raise MoexClientError(str(e)) from e


def _format_query(value: Any) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat(" ", timespec="seconds")
    else:
        return str(value)


def _validate_pagination_query(query: dict[str, Any] | None, reserved_keys: set[str]) -> None:
    if query is None:
        return

    conflicts = sorted(key for key in query if key in reserved_keys)
    if conflicts:
        conflict_keys = ", ".join(conflicts)
        raise ValueError(f"query contains reserved pagination keys: {conflict_keys}")


def _snap_limit(limit_sizes: list[int], remaining: int) -> int:
    if remaining <= 0:
        raise ValueError("remaining must be a positive integer")

    idx = bisect.bisect_left(limit_sizes, remaining)
    if idx >= len(limit_sizes):
        return limit_sizes[-1]
    return limit_sizes[idx]


def _get_max_page_date(page: MoexTable, date_column: str) -> date:
    try:
        date_pos = page.get_column_position(date_column)
    except ValueError as e:
        raise PaginationError(str(e)) from e

    max_date: date | None = None
    for row in page.get_rows():
        value = row[date_pos]
        if value is None:
            continue
        if type(value) is not date:
            raise PaginationError(f"column '{date_column}' must contain date values")

        row_date = value
        if max_date is None or row_date > max_date:
            max_date = row_date

    if max_date is None:
        raise PaginationError(f"column '{date_column}' contains no date values")

    return max_date
