#!/usr/bin/env python3
"""CDC Streaming - Real-time MySQL change event streaming via CdcStream.

Connects directly to MySQL using the replication protocol and streams
events as an async iterator.

Requires libmysqlclient (auto-detected at build time).

Prerequisites:
    1. Docker MySQL running: cd e2e/docker && docker compose up -d
    2. Build libmes with client support:
       cmake -B build-client
       cmake --build build-client --parallel

Usage:
    # Stream all changes from current position
    MES_LIB_PATH=./build-client/core/libmes.dylib python examples/cdc_streaming.py

    # Stream from a specific GTID
    MES_LIB_PATH=./build-client/core/libmes.dylib python examples/cdc_streaming.py \
        --gtid "uuid:1-100"

    # Filter by table
    MES_LIB_PATH=./build-client/core/libmes.dylib python examples/cdc_streaming.py --table items
"""

from __future__ import annotations

import argparse
import asyncio
import os
import signal
import sys
from pathlib import Path

# Add the parent src to path for mysql_event_stream import
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from mysql_event_stream import ChangeEvent, ColumnType, ColumnValue, EventType
from mysql_event_stream.stream import CdcStream

# --- Configuration ---

MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 13307
MYSQL_USER = "root"
MYSQL_PASSWORD = "test_root_password"

# --- Display ---

EVENT_COLORS = {
    EventType.INSERT: "\033[32m",  # green
    EventType.UPDATE: "\033[33m",  # yellow
    EventType.DELETE: "\033[31m",  # red
}
RESET = "\033[0m"
DIM = "\033[2m"


def format_column_value(col: ColumnValue) -> str:
    """Format a column value for display."""
    if col.type == ColumnType.NULL:
        return "NULL"
    if col.type == ColumnType.BYTES:
        assert isinstance(col.value, bytes)
        return f"0x{col.value.hex()}" if len(col.value) <= 16 else f"0x{col.value[:16].hex()}..."
    return repr(col.value)


def print_event(event: ChangeEvent) -> None:
    """Print a change event in a readable format."""
    color = EVENT_COLORS.get(event.type, "")
    label = event.type.name

    print(f"\n{color}[{label}]{RESET} {event.database}.{event.table}")
    print(
        f"  {DIM}binlog: {event.position.file}:{event.position.offset}  "
        f"ts: {event.timestamp}{RESET}"
    )

    if event.before is not None:
        vals = ", ".join(format_column_value(c) for c in event.before)
        print(f"  before: [{vals}]")
    if event.after is not None:
        vals = ", ".join(format_column_value(c) for c in event.after)
        print(f"  after:  [{vals}]")


# --- Main loop ---

running = True


def handle_signal(_sig: int, _frame: object) -> None:
    global running
    running = False
    print(f"\n{DIM}Shutting down...{RESET}")


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

    # Prefer build-client
    client_path = project_root / "build-client" / "core" / lib_name
    if client_path.exists():
        return str(client_path)

    # Fall back to standard build directory
    build_path = project_root / "build" / "core" / lib_name
    if build_path.exists():
        return str(build_path)

    print("Error: libmes not found.", file=sys.stderr)
    print(f"  Checked: {client_path}", file=sys.stderr)
    print(f"  Checked: {build_path}", file=sys.stderr)
    print("Set MES_LIB_PATH or install libmysqlclient and rebuild.", file=sys.stderr)
    sys.exit(1)


async def async_main(args: argparse.Namespace, lib_path: str) -> None:
    total_events = 0

    async for event in CdcStream(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        server_id=args.server_id,
        start_gtid=args.gtid,
        lib_path=lib_path,
    ):
        if not running:
            break
        if args.table and event.table != args.table:
            continue
        print_event(event)
        total_events += 1

    print(f"\n{DIM}Total events: {total_events}{RESET}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream MySQL CDC events via CdcStream")
    parser.add_argument("--table", type=str, default=None, help="Filter by table name")
    parser.add_argument(
        "--gtid", type=str, default="", help="Start GTID (empty = current position)"
    )
    parser.add_argument("--server-id", type=int, default=2, help="Replica server ID")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    lib_path = find_lib_path()

    print("mysql-event-stream CDC Streaming (CdcStream)")
    print(f"  MySQL: {MYSQL_HOST}:{MYSQL_PORT}")
    if args.gtid:
        print(f"  Start GTID: {args.gtid}")
    if args.table:
        print(f"  Filter: table={args.table}")
    print()
    print("Waiting for changes... (Ctrl+C to stop)\n")

    asyncio.run(async_main(args, lib_path))


if __name__ == "__main__":
    main()
