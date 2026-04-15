#!/usr/bin/env python3
"""CDC Watcher - Monitor MySQL / MariaDB changes in real time.

Connects to MySQL or MariaDB via CdcStream and prints change events
with colored output. Optionally filters by table name.
Server flavor is auto-detected.

Requires OpenSSL (linked at build time).

Prerequisites:
    1a. Docker MySQL running:   cd e2e/docker && docker compose up -d
    1b. Or Docker MariaDB:      cd e2e/docker && docker compose -f docker-compose.mariadb.yml up -d
        (same port 13308 / database mes_test; flavor auto-detected)
    2. Build libmes with client support:
       cmake -B build-client
       cmake --build build-client --parallel

Usage:
    # Watch all tables
    MES_LIB_PATH=./build-client/core/libmes.dylib python examples/cdc_watcher.py

    # Watch specific table
    MES_LIB_PATH=./build-client/core/libmes.dylib python examples/cdc_watcher.py --table items

Example output:
    mysql-event-stream CDC Watcher
      Server: 127.0.0.1:13308 (MySQL or MariaDB)

    Waiting for changes... (Ctrl+C to stop)

    [INSERT] mes_test.items
      binlog: mysql-bin.000003:3265  ts: 1773584163
      after:  [id=8, name='DocTest_Widget', value=42]

    [UPDATE] mes_test.items
      binlog: mysql-bin.000003:3611  ts: 1773584164
      before: [id=8, name='DocTest_Widget', value=42]
      after:  [id=8, name='DocTest_Widget', value=100]

    [DELETE] mes_test.items
      binlog: mysql-bin.000003:3922  ts: 1773584164
      before: [id=8, name='DocTest_Widget', value=100]
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

from mysql_event_stream import ChangeEvent, EventType
from mysql_event_stream.stream import CdcStream

# --- Configuration ---

MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 13308
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


def format_row(row: dict[str, object]) -> str:
    """Format a row dict for display."""
    parts = []
    for key, val in row.items():
        if val is None:
            parts.append(f"{key}=NULL")
        elif isinstance(val, bytes):
            hex_str = val.hex()
            parts.append(f"{key}=0x{hex_str}" if len(val) <= 16 else f"{key}=0x{val[:16].hex()}...")
        else:
            parts.append(f"{key}={val!r}")
    return ", ".join(parts)


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
        print(f"  before: [{format_row(event.before)}]")
    if event.after is not None:
        print(f"  after:  [{format_row(event.after)}]")


# --- Main ---

running = True


def handle_signal(_sig: int, _frame: object) -> None:
    global running
    running = False
    print(f"\n{DIM}Shutting down...{RESET}")


def find_lib_path() -> str:
    """Find the libmes shared library."""
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

    print("Error: libmes not found.", file=sys.stderr)
    print(f"  Checked: {client_path}", file=sys.stderr)
    print(f"  Checked: {build_path}", file=sys.stderr)
    print("Set MES_LIB_PATH or install OpenSSL and rebuild.", file=sys.stderr)
    sys.exit(1)


async def async_main(args: argparse.Namespace, lib_path: str) -> None:
    total_events = 0

    async with CdcStream(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        server_id=args.server_id,
        start_gtid="",
        lib_path=lib_path,
    ) as stream:
        async for event in stream:
            if not running:
                break
            if args.table and event.table != args.table:
                continue
            print_event(event)
            total_events += 1

    print(f"\n{DIM}Total events captured: {total_events}{RESET}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Watch MySQL CDC events via CdcStream")
    parser.add_argument("--table", type=str, default=None, help="Filter by table name")
    parser.add_argument("--server-id", type=int, default=2, help="Replica server ID")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    lib_path = find_lib_path()

    print("mysql-event-stream CDC Watcher")
    print(f"  Server: {MYSQL_HOST}:{MYSQL_PORT} (MySQL or MariaDB)")
    if args.table:
        print(f"  Filter: table={args.table}")
    print("\nWaiting for changes... (Ctrl+C to stop)\n")

    asyncio.run(async_main(args, lib_path))


if __name__ == "__main__":
    main()
