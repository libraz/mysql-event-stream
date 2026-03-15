"""MySQL client for E2E tests."""

from __future__ import annotations

from typing import Any

import pymysql
from pymysql.cursors import DictCursor


class MysqlClient:
    """Simple MySQL client for test operations."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 13307,
        user: str = "root",
        password: str = "test_root_password",
        database: str = "mes_test",
    ) -> None:
        self._config = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": database,
            "charset": "utf8mb4",
            "autocommit": True,
            "cursorclass": DictCursor,
        }
        self._conn: pymysql.Connection[DictCursor] | None = None

    def _connect(self) -> pymysql.Connection[DictCursor]:
        """Get or create a connection."""
        if self._conn is None or not self._conn.open:
            self._conn = pymysql.connect(**self._config)
        return self._conn

    def close(self) -> None:
        """Close the connection."""
        if self._conn and self._conn.open:
            self._conn.close()
            self._conn = None

    def execute(self, sql: str, args: tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
        """Execute SQL and return results as list of dicts."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(sql, args)
            if cur.description:
                return list(cur.fetchall())
            return []

    def insert(self, table: str, **kwargs: Any) -> int:
        """Insert a row and return the auto-increment ID."""
        cols = ", ".join(kwargs.keys())
        placeholders = ", ".join(["%s"] * len(kwargs))
        sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(sql, tuple(kwargs.values()))
            return cur.lastrowid or 0

    def update(self, table: str, where: str, **kwargs: Any) -> int:
        """Update rows matching the WHERE clause. Returns affected row count."""
        sets = ", ".join(f"{k} = %s" for k in kwargs)
        sql = f"UPDATE {table} SET {sets} WHERE {where}"
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(sql, tuple(kwargs.values()))
            return cur.rowcount

    def delete(self, table: str, where: str) -> int:
        """Delete rows matching the WHERE clause. Returns affected row count."""
        sql = f"DELETE FROM {table} WHERE {where}"
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.rowcount

    def truncate(self, table: str) -> None:
        """Truncate a table."""
        self.execute(f"TRUNCATE TABLE {table}")

    def binlog_status(self) -> dict[str, Any]:
        """Get current binlog file and position via SHOW BINARY LOG STATUS."""
        rows = self.execute("SHOW BINARY LOG STATUS")
        if not rows:
            raise RuntimeError("SHOW BINARY LOG STATUS returned no rows")
        return rows[0]

    def ping(self) -> bool:
        """Check if MySQL is reachable."""
        try:
            conn = self._connect()
            conn.ping(reconnect=True)
            return True
        except Exception:
            self._conn = None
            return False
