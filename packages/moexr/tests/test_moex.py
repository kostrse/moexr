from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from moexr import AutoPagination, Moex, auto


@pytest.fixture
def mock_client_class() -> MagicMock:
    mock = MagicMock()
    mock.req_table = AsyncMock()
    mock.close = AsyncMock()
    return mock


@pytest.fixture
def sample_table() -> object:
    return object()


@pytest.mark.asyncio
async def test_moex_context_manager(mock_client_class: MagicMock) -> None:
    with patch("moexr.moex._create_client", return_value=mock_client_class):
        moex = Moex()
        async with moex as m:
            assert m is moex

        mock_client_class.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_table_with_explicit_pagination(mock_client_class: MagicMock, sample_table: object) -> None:
    mock_client_class.req_table.return_value = sample_table
    with patch("moexr.moex._create_client", return_value=mock_client_class):
        moex = Moex()
        pagination = object()
        result = await moex.table(["securities"], "securities", paginate=pagination)

        assert result is sample_table
        mock_client_class.req_table.assert_awaited_once_with(
            ["securities"],
            "securities",
            None,
            paginate=pagination,
            limit=None,
        )


@pytest.mark.asyncio
async def test_table_with_auto_pagination(mock_client_class: MagicMock, sample_table: object) -> None:
    mock_client_class.req_table.return_value = sample_table
    with patch("moexr.moex._create_client", return_value=mock_client_class):
        moex = Moex()
        result = await moex.table(["securities"], "securities")

        assert result is sample_table
        mock_client_class.req_table.assert_awaited_once_with(
            ["securities"],
            "securities",
            None,
            paginate=None,
            limit=None,
        )


@pytest.mark.asyncio
async def test_table_with_none_pagination(mock_client_class: MagicMock, sample_table: object) -> None:
    mock_client_class.req_table.return_value = sample_table
    with patch("moexr.moex._create_client", return_value=mock_client_class):
        moex = Moex()
        result = await moex.table(["securities"], "securities", paginate=None)

        assert result is sample_table
        mock_client_class.req_table.assert_awaited_once_with(
            ["securities"],
            "securities",
            None,
            paginate=None,
            limit=None,
        )


@pytest.mark.asyncio
async def test_table_with_limit(mock_client_class: MagicMock, sample_table: object) -> None:
    mock_client_class.req_table.return_value = sample_table
    with patch("moexr.moex._create_client", return_value=mock_client_class):
        moex = Moex()
        result = await moex.table(["securities"], "securities", limit=10)

        assert result is sample_table
        mock_client_class.req_table.assert_awaited_once_with(
            ["securities"],
            "securities",
            None,
            paginate=None,
            limit=10,
        )


@pytest.mark.asyncio
async def test_close_is_idempotent(mock_client_class: MagicMock) -> None:
    with patch("moexr.moex._create_client", return_value=mock_client_class):
        moex = Moex()
        await moex.close()
        await moex.close()

        mock_client_class.close.assert_awaited_once()


def test_auto_sentinel_repr() -> None:
    assert repr(auto) == "auto"
    assert isinstance(auto, AutoPagination)
