import pytest
from aioresponses import aioresponses
from moexr.client import MoexClient


@pytest.mark.asyncio
async def test_basic_async_function():
    """Basic test to confirm async test functions run successfully."""
    assert True


@pytest.mark.asyncio
async def test_moex_client_import():
    """Test that MoexClient can be imported and instantiated."""
    async with MoexClient() as client:
        assert client is not None


@pytest.mark.asyncio
async def test_moex_client_context_manager():
    """Test that MoexClient works as an async context manager."""
    async with MoexClient() as client:
        assert client is not None
        assert hasattr(client, 'req')
        assert hasattr(client, 'req_table')
        assert hasattr(client, 'req_table_paginated')


@pytest.mark.asyncio
async def test_moex_client_with_aioresponses():
    """Test basic setup with aioresponses for future real API testing."""
    with aioresponses() as mock:
        async with MoexClient() as client:
            # This is a basic test showing how aioresponses can be used
            # In real tests, you would mock specific API endpoints
            assert client is not None
            assert mock is not None
