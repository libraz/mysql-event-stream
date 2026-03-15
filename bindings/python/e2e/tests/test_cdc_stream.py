"""E2E tests for CdcStream async iterator."""

from __future__ import annotations

import asyncio

import pytest
from lib.mysql_client import MysqlClient

from mysql_event_stream.stream import CdcStream

MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 13307
MYSQL_USER = "root"
MYSQL_PASSWORD = "test_root_password"


@pytest.mark.streaming
class TestCdcStream:
    """CdcStream async iterator tests."""

    @pytest.mark.asyncio
    async def test_receives_events(self, mysql: MysqlClient, lib_path: str) -> None:
        """CdcStream yields INSERT events via async for."""
        collected: list[tuple[str, str]] = []

        async def insert_later() -> None:
            await asyncio.sleep(1)
            mysql.insert("items", name="stream_test", value=42)

        task = asyncio.create_task(insert_later())

        async for event in CdcStream(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            server_id=210,
            lib_path=lib_path,
        ):
            if event.table == "items":
                collected.append((event.type.name, event.table))
                # Column name assertions (items: id, name, value)
                assert event.after is not None
                assert "id" in event.after
                assert "name" in event.after
                assert "value" in event.after
                break

        await task
        assert len(collected) == 1
        assert collected[0] == ("INSERT", "items")

    @pytest.mark.asyncio
    async def test_context_manager(self, mysql: MysqlClient, lib_path: str) -> None:
        """CdcStream works as async context manager."""
        collected: list[str] = []

        async def insert_later() -> None:
            await asyncio.sleep(1)
            mysql.insert("items", name="ctx_test", value=1)

        task = asyncio.create_task(insert_later())

        async with CdcStream(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            server_id=211,
            lib_path=lib_path,
        ) as stream:
            async for event in stream:
                if event.table == "items":
                    collected.append(event.table)
                    # Column name assertions (items: id, name, value)
                    assert event.after is not None
                    assert "id" in event.after
                    assert "name" in event.after
                    assert "value" in event.after
                    break

        await task
        assert len(collected) == 1

    @pytest.mark.asyncio
    async def test_current_gtid(self, mysql: MysqlClient, lib_path: str) -> None:
        """CdcStream exposes current_gtid after receiving events."""
        gtid = ""

        async def insert_later() -> None:
            await asyncio.sleep(1)
            mysql.insert("items", name="gtid_test", value=1)

        task = asyncio.create_task(insert_later())

        async with CdcStream(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            server_id=212,
            lib_path=lib_path,
        ) as stream:
            async for event in stream:
                if event.table == "items":
                    gtid = stream.current_gtid
                    # Column name assertions (items: id, name, value)
                    assert event.after is not None
                    assert "id" in event.after
                    assert "name" in event.after
                    assert "value" in event.after
                    break

        await task
        assert ":" in gtid, f"Expected GTID format uuid:gno, got: {gtid}"
