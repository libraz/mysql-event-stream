#!/usr/bin/env python3
"""Cache Invalidation - Invalidate caches in real time via CDC.

Demonstrates a practical CDC use case: watching MySQL / MariaDB row changes
and invalidating/refreshing cached data. This pattern is commonly used with
Redis, Memcached, or any application-level cache.

Table setup (run in MySQL or MariaDB):

    CREATE TABLE sessions (
        id          CHAR(36) NOT NULL,
        user_id     INT NOT NULL,
        token       VARCHAR(255) NOT NULL,
        ip_address  VARCHAR(45),
        user_agent  TEXT,
        expires_at  DATETIME NOT NULL,
        created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        INDEX idx_user_id (user_id),
        INDEX idx_expires (expires_at),
        INDEX idx_token (token(64))
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

    CREATE TABLE user_settings (
        user_id     INT NOT NULL,
        theme       VARCHAR(20) NOT NULL DEFAULT 'light',
        language    VARCHAR(10) NOT NULL DEFAULT 'en',
        timezone    VARCHAR(50) NOT NULL DEFAULT 'UTC',
        updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (user_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

Sample DML to trigger events:

    -- New session
    INSERT INTO sessions (id, user_id, token, ip_address, expires_at)
    VALUES ('a1b2c3d4-...', 42, 'tok_abc123', '192.168.1.10',
            DATE_ADD(NOW(), INTERVAL 24 HOUR));

    -- User changes settings
    UPDATE user_settings SET theme = 'dark', language = 'ja'
    WHERE user_id = 42;

    -- Session expired / logout
    DELETE FROM sessions WHERE id = 'a1b2c3d4-...';

    -- Bulk cleanup
    DELETE FROM sessions WHERE expires_at < NOW();

Example output:
    Cache Invalidation Example
      Server: 127.0.0.1:13308 (MySQL or MariaDB)

    Watching for changes...

    [CACHE] sessions: new session a1b2c3d4-... for user 42 (192.168.1.10)
    [CACHE] user_settings: invalidate user:42:settings (theme: light -> dark)
    [CACHE] sessions: evict session a1b2c3d4-...
    [AUDIT] 3 events processed (cache_set=1, cache_invalidate=1, cache_evict=1)

Prerequisites:
    1a. Docker MySQL running:   cd e2e/docker && docker compose up -d
    1b. Or Docker MariaDB:      cd e2e/docker && docker compose -f docker-compose.mariadb.yml up -d
        (same port 13308 / database mes_test; flavor auto-detected)
    2. Build libmes with client support:
       cmake -B build-client
       cmake --build build-client --parallel
    3. Create the tables (DDL above)

Usage:
    MES_LIB_PATH=./build-client/core/libmes.dylib python examples/cache_invalidation.py
"""

from __future__ import annotations

import asyncio
import os
import signal
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from mysql_event_stream import ChangeEvent, EventType
from mysql_event_stream.stream import CdcStream

# --- Configuration ---

MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 13308
MYSQL_USER = "root"
MYSQL_PASSWORD = "test_root_password"

# --- Simulated cache (in production: Redis, Memcached, etc.) ---

cache: dict[str, Any] = {}
stats = {"cache_set": 0, "cache_invalidate": 0, "cache_evict": 0}


def cache_set(key: str, value: object) -> None:
    """Set a cache entry."""
    cache[key] = value
    stats["cache_set"] += 1


def cache_delete(key: str) -> bool:
    """Delete a cache entry. Returns True if the key existed."""
    if key in cache:
        del cache[key]
        return True
    return False


# --- Event handlers ---


def handle_sessions(event: ChangeEvent) -> None:
    """Handle session table changes."""
    if event.type == EventType.INSERT and event.after:
        session_id = event.after["id"]
        user_id = event.after["user_id"]
        ip = event.after.get("ip_address", "unknown")
        cache_set(f"session:{session_id}", event.after)
        print(f"  [CACHE] sessions: new session {session_id} for user {user_id} ({ip})")

    elif event.type == EventType.DELETE and event.before:
        session_id = event.before["id"]
        cache_delete(f"session:{session_id}")
        stats["cache_evict"] += 1
        print(f"  [CACHE] sessions: evict session {session_id}")

    elif event.type == EventType.UPDATE and event.after:
        session_id = event.after["id"]
        cache_set(f"session:{session_id}", event.after)
        print(f"  [CACHE] sessions: refresh session {session_id}")


def handle_user_settings(event: ChangeEvent) -> None:
    """Handle user_settings table changes."""
    if event.type != EventType.UPDATE:
        return
    if not event.before or not event.after:
        return

    user_id = event.after["user_id"]
    cache_key = f"user:{user_id}:settings"

    # Log what changed
    changes: list[str] = []
    for col in ("theme", "language", "timezone"):
        old = event.before.get(col)
        new = event.after.get(col)
        if old != new:
            changes.append(f"{col}: {old} -> {new}")

    cache_delete(cache_key)
    stats["cache_invalidate"] += 1
    print(f"  [CACHE] user_settings: invalidate {cache_key} ({', '.join(changes)})")


# Dispatch table -> handler
HANDLERS: dict[str, Callable[[ChangeEvent], None]] = {
    "sessions": handle_sessions,
    "user_settings": handle_user_settings,
}

# --- Main ---

running = True
total_events = 0


def handle_signal(_sig: int, _frame: object) -> None:
    """Handle termination signals."""
    global running
    running = False


def find_lib_path() -> str:
    """Find the libmes shared library.

    Checks in order:
    1. MES_LIB_PATH environment variable
    2. build-client/core/ (client-enabled build)
    3. build/core/ (standard build)
    """
    env_path = os.environ.get("MES_LIB_PATH")
    if env_path and Path(env_path).exists():
        return env_path

    project_root = Path(__file__).parent.parent.parent.parent
    lib_name = "libmes.dylib" if sys.platform == "darwin" else "libmes.so"

    for subdir in ("build-client", "build"):
        candidate = project_root / subdir / "core" / lib_name
        if candidate.exists():
            return str(candidate)

    print("Error: libmes not found. Set MES_LIB_PATH.", file=sys.stderr)
    sys.exit(1)


async def async_main(lib_path: str) -> None:
    """Stream CDC events and dispatch to table-specific handlers."""
    global total_events

    async for event in CdcStream(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        server_id=3,
        start_gtid="",
        lib_path=lib_path,
    ):
        if not running:
            break
        handler = HANDLERS.get(event.table)
        if handler:
            handler(event)
            total_events += 1


def main() -> None:
    """Entry point for the cache invalidation example."""
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    lib_path = find_lib_path()

    print("Cache Invalidation Example")
    print(f"  Server: {MYSQL_HOST}:{MYSQL_PORT} (MySQL or MariaDB)")
    print(f"  Tables: {', '.join(HANDLERS)}\n")
    print("Watching for changes... (Ctrl+C to stop)\n")

    asyncio.run(async_main(lib_path))

    s = stats
    print(
        f"\n  [AUDIT] {total_events} events processed "
        f"(cache_set={s['cache_set']}, "
        f"cache_invalidate={s['cache_invalidate']}, "
        f"cache_evict={s['cache_evict']})"
    )
    print(f"  [AUDIT] cache entries: {len(cache)}")


if __name__ == "__main__":
    main()
