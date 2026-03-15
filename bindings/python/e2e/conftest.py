"""E2E test fixtures for mysql-event-stream."""

from __future__ import annotations

import os
import sys
from collections.abc import Generator
from pathlib import Path

import pytest

# Add the e2e root to sys.path so that ``lib`` is importable.
sys.path.insert(0, str(Path(__file__).parent))

from lib.mysql_client import MysqlClient  # noqa: E402
from lib.streaming_collector import StreamingCollector  # noqa: E402
from lib.wait import wait_until  # noqa: E402

MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 13307
MYSQL_USER = "root"
MYSQL_PASSWORD = "test_root_password"
MYSQL_DATABASE = "mes_test"

# Incrementing server ID to avoid conflicts between concurrent tests
_server_id_counter = 100


def _next_server_id() -> int:
    global _server_id_counter
    _server_id_counter += 1
    return _server_id_counter


def _find_lib_path() -> str:
    """Find the libmes shared library with client support.

    Search order:
        1. MES_LIB_PATH environment variable
        2. build-client directory (client-enabled build)
        3. Regular build directory
    """
    env_path = os.environ.get("MES_LIB_PATH")
    if env_path and Path(env_path).exists():
        return env_path

    project_root = Path(__file__).parent.parent.parent.parent
    lib_name = "libmes.dylib" if sys.platform == "darwin" else "libmes.so"

    client_path = project_root / "build-client" / "core" / lib_name
    if client_path.exists():
        return str(client_path)

    build_path = project_root / "build" / "core" / lib_name
    if build_path.exists():
        return str(build_path)

    pytest.skip("libmes not found; install libmysqlclient and rebuild first")
    return ""  # unreachable, but keeps mypy happy


@pytest.fixture(scope="session")
def lib_path() -> str:
    """Path to the libmes shared library."""
    return _find_lib_path()


@pytest.fixture(scope="session")
def mysql() -> Generator[MysqlClient, None, None]:
    """Session-scoped MySQL client. Waits for MySQL to be ready."""
    client = MysqlClient(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )

    wait_until(
        client.ping,
        timeout=60,
        interval=2,
        description="MySQL to be ready",
    )

    yield client
    client.close()


@pytest.fixture(autouse=True)
def clean_tables(mysql: MysqlClient) -> None:
    """Clean test tables before each test."""
    mysql.truncate("items")
    mysql.truncate("users")


@pytest.fixture
def collector(lib_path: str) -> Generator[StreamingCollector, None, None]:
    """Per-test streaming collector.

    Starts a BinlogClient stream in a background thread.
    Use collector.wait_for_events() to collect events after DML operations.
    """
    c = StreamingCollector(lib_path, server_id=_next_server_id())
    c.start()
    yield c
    c.stop()
