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
    2. Build libmes:
       cmake -B build-client
       cmake --build build-client --parallel
    3. Create the tables (DDL above)

Usage (run from bindings/python):
    MES_LIB_PATH=../../build-client/core/libmes.dylib python examples/cache_invalidation.py
"""

from __future__ import annotations

import asyncio
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

Row = dict[str, Any]


def session_key(row: Row) -> str:
    return f"session:{row['id']}"


def settings_key(row: Row) -> str:
    return f"user:{row['user_id']}:settings"


def handle_sessions(event_type: EventType, before: Row | None, after: Row | None) -> None:
    """Handle session table changes."""
    if event_type == EventType.DELETE and before:
        cache_delete(session_key(before))
        stats["cache_evict"] += 1
        print(f"  [CACHE] sessions: evict session {before['id']}")

    elif after:
        cache_set(session_key(after), after)
        if event_type == EventType.INSERT:
            ip = after.get("ip_address", "unknown")
            print(
                f"  [CACHE] sessions: new session {after['id']} for user {after['user_id']} ({ip})"
            )
        else:
            print(f"  [CACHE] sessions: refresh session {after['id']}")


def handle_user_settings(event_type: EventType, before: Row | None, after: Row | None) -> None:
    """Handle user_settings table changes."""
    if event_type != EventType.UPDATE or not before or not after:
        return

    cache_key = settings_key(after)

    # Log what changed
    changes: list[str] = []
    for col in ("theme", "language", "timezone"):
        old = before.get(col)
        new = after.get(col)
        if old != new:
            changes.append(f"{col}: {old} -> {new}")

    cache_delete(cache_key)
    stats["cache_invalidate"] += 1
    print(f"  [CACHE] user_settings: invalidate {cache_key} ({', '.join(changes)})")


# Dispatch table -> (cache key of a row, handler)
HANDLERS: dict[str, tuple[Callable[[Row], str], Callable[..., None]]] = {
    "sessions": (session_key, handle_sessions),
    "user_settings": (settings_key, handle_user_settings),
}


def handle_event(event: ChangeEvent) -> bool:
    """Apply one row event to the cache; returns False when it was skipped.

    Rows are read by column name, so an event whose names could not be resolved
    is skipped. An UPDATE that changes the cache key also drops the old entry.
    """
    entry = HANDLERS.get(event.table)
    if entry is None:
        return False
    if not event.names_resolved:
        print(f"  [SKIP] {event.type.name} on {event.table}: column names unresolved")
        return False

    key_of, handler = entry
    before, after = event.before, event.after
    old_key = key_of(before) if event.type == EventType.UPDATE and before and after else None
    if old_key is not None and after and old_key != key_of(after) and cache_delete(old_key):
        stats["cache_evict"] += 1
        print(f"  [CACHE] {event.table}: evict stale {old_key}")
    handler(event.type, before, after)
    return True


# --- Main ---

total_events = 0


def install_shutdown_handlers(stream: CdcStream) -> None:
    """Close the stream on SIGINT/SIGTERM, which also ends a poll blocked on an idle server."""
    loop = asyncio.get_running_loop()
    closing: list[asyncio.Task[None]] = []

    def on_signal() -> None:
        closing.append(asyncio.ensure_future(stream.close()))

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, on_signal)


async def async_main() -> None:
    """Stream CDC events and dispatch to table-specific handlers."""
    global total_events

    async with CdcStream(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        # Local test server only; use verified TLS (ssl_mode/ssl_ca) in production.
        allow_public_key_retrieval=True,
        server_id=3,
    ) as stream:
        install_shutdown_handlers(stream)
        async for event in stream:
            if handle_event(event):
                total_events += 1


def main() -> None:
    """Entry point for the cache invalidation example."""
    print("Cache Invalidation Example")
    print(f"  Server: {MYSQL_HOST}:{MYSQL_PORT} (MySQL or MariaDB)")
    print(f"  Tables: {', '.join(HANDLERS)}\n")
    print("Watching for changes... (Ctrl+C to stop)\n")

    asyncio.run(async_main())

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
