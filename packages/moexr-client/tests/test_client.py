from __future__ import annotations

import re
from datetime import date
from typing import Any, cast

import pytest
from aioresponses import CallbackResult, aioresponses
from moexr.client import (
    DatePagination,
    LimitOnly,
    MoexClient,
    OffsetPagination,
    PaginationError,
)
from moexr.client import client as client_module


def _table_payload(
    table_name: str,
    metadata: dict[str, dict[str, Any]],
    columns: list[str],
    data: list[list[Any]],
) -> dict[str, Any]:
    return {
        table_name: {
            "metadata": metadata,
            "columns": columns,
            "data": data,
        }
    }


def _int_table_payload(table_name: str, values: list[int]) -> dict[str, Any]:
    return _table_payload(
        table_name,
        metadata={"ID": {"type": "int32"}},
        columns=["ID"],
        data=[[value] for value in values],
    )


def _date_table_payload(table_name: str, rows: list[tuple[str, int]]) -> dict[str, Any]:
    return _table_payload(
        table_name,
        metadata={
            "TRADEDATE": {"type": "date", "bytes": 10, "max_size": 0},
            "ID": {"type": "int32"},
        },
        columns=["TRADEDATE", "ID"],
        data=[[trade_date, value] for trade_date, value in rows],
    )


def _register_sequence(
    mock: Any,
    url_pattern: re.Pattern[str],
    payloads: list[dict[str, Any]],
    captured_params: list[dict[str, str]],
) -> None:
    state = {"index": 0}

    def callback(_: Any, **kwargs: Any) -> CallbackResult:
        params: dict[str, Any] = kwargs["params"]
        captured_params.append({str(key): str(value) for key, value in params.items()})

        payload_index = state["index"]
        state["index"] += 1

        payload = payloads[payload_index]
        return CallbackResult(status=200, payload=payload)

    mock.get(url_pattern, callback=callback, repeat=True)


@pytest.mark.asyncio
async def test_moex_client_attributes() -> None:
    client = MoexClient()
    try:
        assert hasattr(client, "req")
        assert hasattr(client, "req_table")
        assert not hasattr(client, "req_table_paginated")
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_req_table_single_shot_and_query_scoping() -> None:
    table_name = "securities"
    url = re.compile(r"https://iss\.moex\.com/iss/securities\.json")
    captured_params: list[dict[str, str]] = []

    payload = _int_table_payload(table_name, [1, 2, 3])

    with aioresponses() as mock:
        _register_sequence(mock, url, [payload], captured_params)

        client = MoexClient()
        try:
            result = await client.req_table(["securities"], table_name, query={"group_by": "type"})
        finally:
            await client.close()

    assert len(result) == 3
    assert captured_params == [
        {
            "securities.group_by": "type",
            "iss.only": "securities",
            "lang": "ru",
        }
    ]


@pytest.mark.asyncio
async def test_req_table_limit_only_truncates_single_shot() -> None:
    table_name = "securities"
    url = re.compile(r"https://iss\.moex\.com/iss/securities\.json")
    captured_params: list[dict[str, str]] = []

    payload = _int_table_payload(table_name, [1, 2, 3, 4])

    with aioresponses() as mock:
        _register_sequence(mock, url, [payload], captured_params)

        client = MoexClient()
        try:
            result = await client.req_table(["securities"], table_name, limit=2)
        finally:
            await client.close()

    assert len(result) == 2
    assert result.get_value(0, "ID") == 1
    assert result.get_value(1, "ID") == 2
    assert captured_params[0]["iss.only"] == "securities"
    assert "securities.limit" not in captured_params[0]


@pytest.mark.asyncio
async def test_req_table_rejects_non_positive_limit() -> None:
    client = MoexClient()
    try:
        with pytest.raises(ValueError, match="positive integer"):
            await client.req_table(["securities"], "securities", limit=0)

        with pytest.raises(ValueError, match="positive integer"):
            await client.req_table(["securities"], "securities", limit=-1)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_req_table_rejects_unsupported_pagination_type() -> None:
    client = MoexClient()
    try:
        with pytest.raises(TypeError, match="paginate must be"):
            await client.req_table(["securities"], "securities", paginate=cast(Any, object()))
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_req_table_offset_paginated_without_limit_sizes() -> None:
    table_name = "securities"
    url = re.compile(r"https://iss\.moex\.com/iss/securities\.json")
    captured_params: list[dict[str, str]] = []

    payloads = [
        _int_table_payload(table_name, [1, 2]),
        _int_table_payload(table_name, [3, 4]),
        _int_table_payload(table_name, []),
    ]

    with aioresponses() as mock:
        _register_sequence(mock, url, payloads, captured_params)

        client = MoexClient()
        try:
            result = await client.req_table(["securities"], table_name, paginate=OffsetPagination())
        finally:
            await client.close()

    assert len(result) == 4
    assert [result.get_value(i, "ID") for i in range(4)] == [1, 2, 3, 4]

    assert captured_params[0]["securities.start"] == "0"
    assert captured_params[1]["securities.start"] == "2"
    assert captured_params[2]["securities.start"] == "4"
    assert "securities.limit" not in captured_params[0]
    assert "securities.limit" not in captured_params[1]
    assert "securities.limit" not in captured_params[2]


@pytest.mark.asyncio
async def test_req_table_offset_paginated_page_size_snap_down() -> None:
    table_name = "securities"
    url = re.compile(r"https://iss\.moex\.com/iss/securities\.json")
    captured_params: list[dict[str, str]] = []

    payloads = [
        _int_table_payload(table_name, list(range(1, 11))),
        _int_table_payload(table_name, [11, 12]),
    ]

    with aioresponses() as mock:
        _register_sequence(mock, url, payloads, captured_params)

        client = MoexClient()
        try:
            result = await client.req_table(
                ["securities"],
                table_name,
                paginate=OffsetPagination(limit_sizes=[1, 5, 10]),
                limit=12,
            )
        finally:
            await client.close()

    assert len(result) == 12
    assert captured_params[0]["securities.start"] == "0"
    assert captured_params[0]["securities.limit"] == "10"
    assert captured_params[1]["securities.start"] == "10"
    assert captured_params[1]["securities.limit"] == "5"


@pytest.mark.asyncio
async def test_req_table_offset_paginated_detects_query_conflicts() -> None:
    client = MoexClient()
    try:
        with pytest.raises(ValueError, match="start"):
            await client.req_table(["securities"], "securities", query={"start": 100}, paginate=OffsetPagination())

        with pytest.raises(ValueError, match="limit"):
            await client.req_table(
                ["securities"],
                "securities",
                query={"limit": 10},
                paginate=OffsetPagination(limit_sizes=[10]),
            )
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_req_table_date_paginated_progresses_from_boundary() -> None:
    table_name = "history"
    url = re.compile(r"https://iss\.moex\.com/iss/history\.json")
    captured_params: list[dict[str, str]] = []

    payloads = [
        _date_table_payload(table_name, [("2024-01-01", 1), ("2024-01-03", 2)]),
        _date_table_payload(table_name, [("2024-01-04", 3)]),
        _date_table_payload(table_name, []),
    ]

    with aioresponses() as mock:
        _register_sequence(mock, url, payloads, captured_params)

        client = MoexClient()
        try:
            result = await client.req_table(
                ["history"],
                table_name,
                query={"from": date(2024, 1, 1), "till": date(2024, 1, 31)},
                paginate=DatePagination(date_column="TRADEDATE"),
            )
        finally:
            await client.close()

    assert len(result) == 3
    assert [result.get_value(i, "ID") for i in range(3)] == [1, 2, 3]

    assert captured_params[0]["history.from"] == "2024-01-01"
    assert captured_params[1]["history.from"] == "2024-01-04"
    assert captured_params[2]["history.from"] == "2024-01-05"
    assert captured_params[0]["history.till"] == "2024-01-31"
    assert captured_params[1]["history.till"] == "2024-01-31"


@pytest.mark.asyncio
async def test_req_table_date_paginated_respects_limit() -> None:
    table_name = "history"
    url = re.compile(r"https://iss\.moex\.com/iss/history\.json")
    captured_params: list[dict[str, str]] = []

    payloads = [_date_table_payload(table_name, [("2024-01-01", 1), ("2024-01-02", 2), ("2024-01-03", 3)])]

    with aioresponses() as mock:
        _register_sequence(mock, url, payloads, captured_params)

        client = MoexClient()
        try:
            result = await client.req_table(
                ["history"],
                table_name,
                query={"from": date(2024, 1, 1)},
                paginate=DatePagination(date_column="TRADEDATE"),
                limit=2,
            )
        finally:
            await client.close()

    assert len(result) == 2
    assert captured_params[0]["history.from"] == "2024-01-01"
    assert len(captured_params) == 1


@pytest.mark.asyncio
async def test_req_table_date_paginated_detects_stalled_boundary() -> None:
    table_name = "history"
    url = re.compile(r"https://iss\.moex\.com/iss/history\.json")
    captured_params: list[dict[str, str]] = []

    payloads = [
        _date_table_payload(table_name, [("2024-01-01", 1)]),
        _date_table_payload(table_name, [("2024-01-01", 2)]),
    ]

    with aioresponses() as mock:
        _register_sequence(mock, url, payloads, captured_params)

        client = MoexClient()
        try:
            with pytest.raises(PaginationError, match="did not advance"):
                await client.req_table(
                    ["history"],
                    table_name,
                    query={"from": date(2024, 1, 1)},
                    paginate=DatePagination(date_column="TRADEDATE"),
                )
        finally:
            await client.close()

    assert captured_params[0]["history.from"] == "2024-01-01"
    assert captured_params[1]["history.from"] == "2024-01-02"


@pytest.mark.asyncio
async def test_req_table_respects_max_pages_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(client_module, "_MAX_PAGES", 2)

    table_name = "securities"
    url = re.compile(r"https://iss\.moex\.com/iss/securities\.json")
    captured_params: list[dict[str, str]] = []

    payloads = [
        _int_table_payload(table_name, [1]),
        _int_table_payload(table_name, [2]),
    ]

    with aioresponses() as mock:
        _register_sequence(mock, url, payloads, captured_params)

        client = MoexClient()
        try:
            with pytest.raises(PaginationError, match="maximum page count"):
                await client.req_table(
                    ["securities"],
                    table_name,
                    paginate=OffsetPagination(limit_sizes=[1]),
                )
        finally:
            await client.close()

    assert captured_params[0]["securities.start"] == "0"
    assert captured_params[1]["securities.start"] == "1"


@pytest.mark.asyncio
async def test_req_table_limit_only_snaps_up_to_supported_value() -> None:
    table_name = "security"
    url = re.compile(r"https://iss\.moex\.com/iss/statistics\.json")
    captured_params: list[dict[str, str]] = []

    payload = _int_table_payload(table_name, list(range(1, 101)))

    with aioresponses() as mock:
        _register_sequence(mock, url, [payload], captured_params)

        client = MoexClient()
        try:
            result = await client.req_table(
                ["statistics"],
                table_name,
                paginate=LimitOnly(limit_sizes=[1, 10, 20, 100, 1000]),
                limit=75,
            )
        finally:
            await client.close()

    assert len(result) == 75
    assert captured_params[0]["security.limit"] == "100"
    assert "security.start" not in captured_params[0]


@pytest.mark.asyncio
async def test_req_table_limit_only_uses_max_when_no_user_limit() -> None:
    table_name = "asset_volumes"
    url = re.compile(r"https://iss\.moex\.com/iss/statistics\.json")
    captured_params: list[dict[str, str]] = []

    payload = _int_table_payload(table_name, [1, 2, 3])

    with aioresponses() as mock:
        _register_sequence(mock, url, [payload], captured_params)

        client = MoexClient()
        try:
            result = await client.req_table(
                ["statistics"],
                table_name,
                paginate=LimitOnly(limit_sizes=[10, 20, 100, 1000]),
            )
        finally:
            await client.close()

    assert len(result) == 3
    assert captured_params[0]["asset_volumes.limit"] == "1000"


@pytest.mark.asyncio
async def test_req_table_limit_only_uses_max_when_limit_exceeds_all() -> None:
    table_name = "security"
    url = re.compile(r"https://iss\.moex\.com/iss/statistics\.json")
    captured_params: list[dict[str, str]] = []

    payload = _int_table_payload(table_name, list(range(1, 101)))

    with aioresponses() as mock:
        _register_sequence(mock, url, [payload], captured_params)

        client = MoexClient()
        try:
            result = await client.req_table(
                ["statistics"],
                table_name,
                paginate=LimitOnly(limit_sizes=[10, 20, 100]),
                limit=500,
            )
        finally:
            await client.close()

    assert len(result) == 100
    assert captured_params[0]["security.limit"] == "100"


@pytest.mark.asyncio
async def test_req_table_limit_only_detects_query_conflict() -> None:
    client = MoexClient()
    try:
        with pytest.raises(ValueError, match="limit"):
            await client.req_table(
                ["statistics"],
                "security",
                query={"limit": 10},
                paginate=LimitOnly(limit_sizes=[10, 20, 100]),
            )
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_req_table_limit_only_exact_match_no_truncation() -> None:
    table_name = "security"
    url = re.compile(r"https://iss\.moex\.com/iss/statistics\.json")
    captured_params: list[dict[str, str]] = []

    payload = _int_table_payload(table_name, list(range(1, 21)))

    with aioresponses() as mock:
        _register_sequence(mock, url, [payload], captured_params)

        client = MoexClient()
        try:
            result = await client.req_table(
                ["statistics"],
                table_name,
                paginate=LimitOnly(limit_sizes=[10, 20, 100]),
                limit=20,
            )
        finally:
            await client.close()

    assert len(result) == 20
    assert captured_params[0]["security.limit"] == "20"
