from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from types import TracebackType
from typing import Any, Self

from moexr.client import MoexClient, Pagination

from .pagination import AutoPagination, auto

QueryScalar = str | int | float | bool | date | datetime
Query = Mapping[str, QueryScalar | None]


class Moex:
    def __init__(self, access_token: str | None = None, lang: str = "ru") -> None:
        self._client = MoexClient(access_token=access_token, lang=lang)
        self._closed = False

    async def close(self) -> None:
        if not self._closed:
            self._closed = True
            await self._client.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def table(
        self,
        path: list[str],
        table: str,
        query: Query | None = None,
        *,
        paginate: Pagination | AutoPagination | None = auto,
        limit: int | None = None,
    ) -> Any:
        if isinstance(paginate, AutoPagination):
            paginate = None
        return await self._client.req_table(path, table, query, paginate=paginate, limit=limit)
