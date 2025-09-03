import asyncio
import bisect
from datetime import date, datetime
from typing import Any, Optional

import aiohttp

from .error import MoexClientError
from .result import MoexTableResult

_REQ_LIMITS = [1, 5, 10, 20, 50, 100]


class MoexClient:
    def __init__(self, access_token: Optional[str] = None, lang: str = 'ru'):
        if access_token is None:
            self._client_session = aiohttp.ClientSession(base_url='https://iss.moex.com/')
        else:
            headers = {
                'Authorization': 'Bearer ' + access_token,
            }
            self._client_session = aiohttp.ClientSession(base_url='https://apim.moex.com/', headers=headers)

        self._req_semaphore = asyncio.Semaphore(4)
        self._lang = lang

    async def req(self, path: list[str], query: Optional[dict[str, Any]] = None) -> dict[str, MoexTableResult]:
        return await self._req(path, query)

    async def req_table(self, path: list[str], table_name: str, query: Optional[dict[str, Any]] = None) -> MoexTableResult:
        query_params: dict[str, Any] = {}
        if query:
            query_params.update({table_name + '.' + key: value for key, value in query.items()})

        query_params['iss.only'] = table_name

        result = await self._req(path, query_params)
        return result[table_name]

    async def req_table_paginated(self, path: list[str], table_name: str, query: Optional[dict[str, Any]] = None, limit: Optional[int] = None) -> MoexTableResult:
        if limit is not None and limit <= 0:
            raise ValueError("limit must be a positive")

        offset = 0
        remaining = limit

        merged_result: Optional[MoexTableResult] = None

        while remaining is None or remaining > 0:
            req_limit = _get_req_limit(remaining)

            query_params: dict[str, Any] = {}
            if query:
                query_params.update(query)

            query_params['start'] = offset
            query_params['limit'] = req_limit

            result = await self.req_table(path, table_name, query_params)
            resp_count = len(result)

            if remaining is not None:
                if resp_count > remaining:
                    result = result.take(remaining)
                    remaining = 0
                else:
                    remaining -= resp_count

            if merged_result is None:
                merged_result = result
            else:
                merged_result.extend(result)

            if resp_count < req_limit:
                break

            offset += resp_count

        assert merged_result is not None
        return merged_result

    async def _req(self, path: list[str], query: dict[str, Any] | None) -> dict[str, MoexTableResult]:
        query_params: dict[str, Any] = {}
        if query:
            query_params.update({key: _format_query(value) for key, value in query.items() if value is not None})

        query_params['lang'] = self._lang

        async with self._req_semaphore:
            try:
                async with self._client_session.get('/iss/' + '/'.join(path) + '.json', params=query_params) as resp:
                    if resp.status != 200:
                        raise MoexClientError(f"Request failed with status code: {resp.status}")

                    result = await resp.json()
                    return {key: MoexTableResult.from_result(value) for key, value in result.items()}
            except Exception as e:
                raise MoexClientError(str(e)) from e


def _format_query(value: Any) -> str:
    if isinstance(value, bool):
        return '1' if value else '0'
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat(' ', timespec='seconds')
    else:
        return str(value)


def _get_req_limit(limit: int | None) -> int:
    if limit is None:
        return _REQ_LIMITS[-1]

    if limit <= 0:
        raise ValueError("request limit must be greater than 0")

    idx = bisect.bisect_left(_REQ_LIMITS, limit)

    if idx >= len(_REQ_LIMITS):
        return _REQ_LIMITS[-1]

    return _REQ_LIMITS[idx]
