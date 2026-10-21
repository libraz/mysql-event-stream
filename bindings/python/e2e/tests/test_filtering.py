"""E2E tests for include/exclude filters across a forced reconnect."""

from __future__ import annotations

import asyncio

import pytest
from conftest import MYSQL_HOST, MYSQL_PORT
from lib.mysql_client import MysqlClient

from mysql_event_stream.stream import CdcStream

# A user of its own, so the forced reconnect kills only this test's dump thread.
STREAM_USER = "mes_filter_e2e"
STREAM_PASSWORD = "filter_e2e_password"


def _write_round(mysql: MysqlClient, tag: str) -> None:
    """One write to each allowed and filtered table, ending on an allowed sentinel."""
    mysql.insert("large_data", big_text=tag)  # excluded by wildcard
    mysql.insert("items", name=tag, value=1)  # qualified include
    mysql.insert("ddl_test", val=tag)  # matches no include entry
    mysql.insert("users", name=f"{tag}-sentinel")  # bare include


@pytest.mark.streaming
class TestStreamFilters:
    """Filters keep their effect on the connection that replaces a killed one."""

    @pytest.mark.asyncio
    async def test_delivers_exactly_the_allowed_tables_across_a_reconnect(
        self, mysql: MysqlClient, lib_path: str
    ) -> None:
        mysql.execute(
            f"CREATE USER IF NOT EXISTS '{STREAM_USER}'@'%' IDENTIFIED BY '{STREAM_PASSWORD}'"
        )
        mysql.execute(f"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{STREAM_USER}'@'%'")
        mysql.execute(f"GRANT SELECT ON mes_test.* TO '{STREAM_USER}'@'%'")
        start_gtid = mysql.get_current_gtid()
        delivered: list[str] = []
        killed: list[int] = []
        sentinels = 0

        async def first_round() -> None:
            await asyncio.sleep(1)
            _write_round(mysql, "first")

        task = asyncio.create_task(first_round())

        async with CdcStream(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=STREAM_USER,
            password=STREAM_PASSWORD,
            allow_public_key_retrieval=True,
            server_id=320,
            start_gtid=start_gtid,
            include_databases=["mes_test"],
            include_tables=["mes_test.items", "users", "large_data"],
            exclude_tables=["large_*"],
            lib_path=lib_path,
        ) as stream:
            async with asyncio.timeout(60):
                async for event in stream:
                    delivered.append(event.table)
                    name = str((event.after or {}).get("name", ""))
                    if event.table != "users" or not name.endswith("-sentinel"):
                        continue
                    sentinels += 1
                    if sentinels == 2:
                        break
                    threads = mysql.dump_thread_ids(STREAM_USER)
                    assert threads, "the stream's dump thread is not on the server"
                    for thread_id in threads:
                        mysql.kill_thread(thread_id)
                    killed.extend(threads)
                    _write_round(mysql, "second")

        await task
        assert delivered == ["items", "users", "items", "users"]
        assert killed
