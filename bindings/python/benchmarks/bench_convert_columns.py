"""Cost of ctypes column marshalling in the Python binding.

Two measurements, both on the same synthetic streams the core benchmarks use:

* isolated -- ``_convert_columns`` called directly on a prepared
  ``mes_column_t`` array, reported as nanoseconds per column.
* in-context -- a full ``feed`` / ``next_event`` loop, run once normally and
  once with ``_convert_columns`` replaced by a stub, so the difference is the
  share of per-row time the marshalling actually owns.

The streams come from the core harness::

    cmake --build build-bench --target mes_benchmark_workloads
    build-bench/core/mes_benchmark_workloads --emit <dir>
    python bindings/python/benchmarks/bench_convert_columns.py --streams <dir>
"""

from __future__ import annotations

import argparse
import ctypes
import platform
import statistics
import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT / "bindings" / "python" / "src"))

from mysql_event_stream import engine as engine_mod  # noqa: E402
from mysql_event_stream._ffi import MESColumn, MESEvent  # noqa: E402

WORKLOADS = (
    "int1_write_x1",
    "temporal7_write_x1",
    "strings7_write_x1",
    "wide28_write_x1",
    "wide28_write_x50",
    "wide28_update_x50",
)

# Rows each stream file yields, mirroring the core workload definitions.
ROWS_PER_STREAM = {
    "int1_write_x1": 1,
    "temporal7_write_x1": 1,
    "strings7_write_x1": 1,
    "wide28_write_x1": 1,
    "wide28_write_x50": 50,
    "wide28_update_x50": 50,
}

REPEATS = 5

# Set from --lib so the numbers can be tied to a freshly built core.
LIB_PATH: str | None = None


def _median_rate(fn, iterations: int) -> float:
    """Median seconds per iteration over REPEATS timed runs, after a warmup."""
    fn(max(1, iterations // 10))
    samples = []
    for _ in range(REPEATS):
        started = time.perf_counter()
        fn(iterations)
        samples.append((time.perf_counter() - started) / iterations)
    return statistics.median(samples)


def _capture_columns(stream: bytes) -> list[tuple]:
    """Snapshot the first event's column array as plain Python values.

    ``mes_next_event`` invalidates its pointers on the following call, so the
    isolated benchmark rebuilds an equivalent array from copied payloads rather
    than holding the engine's memory.
    """
    with engine_mod.CdcEngine(LIB_PATH) as eng:
        offset = 0
        while offset < len(stream):
            consumed = eng.feed(stream[offset:])
            if consumed == 0:
                break
            offset += consumed
        event_ptr = ctypes.POINTER(MESEvent)()
        rc = eng._lib.mes_next_event(eng._handle, ctypes.byref(event_ptr))  # noqa: SLF001
        if rc != 0:
            raise RuntimeError(f"stream produced no event (rc={rc})")
        event = event_ptr.contents
        columns = event.after_columns if event.after_count else event.before_columns
        count = event.after_count or event.before_count
        captured = []
        for i in range(count):
            col = columns[i]
            payload = (
                ctypes.string_at(col.str_data, col.str_len) if col.str_data and col.str_len else b""
            )
            captured.append((col.type, col.int_val, col.double_val, payload, col.col_name))
        return captured


def _build_array(captured: list[tuple]) -> ctypes.Array:
    """Rebuild an owned mes_column_t array from captured values."""
    array = (MESColumn * len(captured))()
    keepalive = []
    for i, (col_type, int_val, double_val, payload, name) in enumerate(captured):
        buf = ctypes.create_string_buffer(payload, len(payload))
        name_buf = ctypes.create_string_buffer(name or b"")
        keepalive.append((buf, name_buf))
        array[i].type = col_type
        array[i].int_val = int_val
        array[i].double_val = double_val
        array[i].str_data = ctypes.cast(buf, ctypes.c_void_p).value if payload else None
        array[i].str_len = len(payload)
        array[i].col_name = ctypes.cast(name_buf, ctypes.c_char_p).value
    array._mes_keepalive = keepalive  # noqa: SLF001
    return array


def bench_isolated(stream: bytes, iterations: int) -> tuple[float, int]:
    """Seconds per _convert_columns call, and the column count measured."""
    captured = _capture_columns(stream)
    array = _build_array(captured)
    count = len(captured)
    convert = engine_mod._convert_columns  # noqa: SLF001
    cache: dict[bytes, str] = {}

    def run(n: int) -> None:
        for _ in range(n):
            convert(array, count, cache)

    return _median_rate(run, iterations), count


def bench_in_context(stream: bytes, rows: int, iterations: int) -> tuple[float, float]:
    """Seconds per row event with and without column marshalling."""

    def drain(n: int) -> None:
        with engine_mod.CdcEngine(LIB_PATH) as eng:
            for _ in range(n):
                offset = 0
                while offset < len(stream):
                    consumed = eng.feed(stream[offset:])
                    if consumed == 0:
                        break
                    offset += consumed
                while eng.next_event() is not None:
                    pass

    full = _median_rate(drain, iterations) / rows

    original = engine_mod._convert_columns  # noqa: SLF001
    engine_mod._convert_columns = lambda cols, count, name_cache=None: {}  # noqa: SLF001
    try:
        stubbed = _median_rate(drain, iterations) / rows
    finally:
        engine_mod._convert_columns = original  # noqa: SLF001
    return full, stubbed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--streams",
        type=Path,
        required=True,
        help="directory written by mes_benchmark_workloads --emit",
    )
    parser.add_argument("--iterations", type=int, default=2000)
    parser.add_argument(
        "--lib",
        type=Path,
        default=None,
        help="libmes shared library to load (default: the packaged one)",
    )
    args = parser.parse_args()

    global LIB_PATH
    LIB_PATH = str(args.lib) if args.lib else None

    print(
        f"python={platform.python_version()} impl={platform.python_implementation()} "
        f"machine={platform.machine()} repeats={REPEATS} iterations={args.iterations} "
        f"lib={LIB_PATH or 'packaged'}"
    )
    for name in WORKLOADS:
        path = args.streams / f"{name}.bin"
        if not path.exists():
            print(f"workload={name} SKIPPED (missing {path})")
            continue
        stream = path.read_bytes()
        rows = ROWS_PER_STREAM[name]

        per_call, columns = bench_isolated(stream, args.iterations * 10)
        full, stubbed = bench_in_context(stream, rows, args.iterations)
        marshal_share = (full - stubbed) / full * 100 if full > 0 else 0.0

        print(
            f"workload={name} columns={columns} "
            f"ns_per_column={per_call / columns * 1e9:.1f} "
            f"us_per_convert_columns={per_call * 1e6:.2f} "
            f"us_per_row_event={full * 1e6:.2f} "
            f"us_per_row_event_stubbed={stubbed * 1e6:.2f} "
            f"marshalling_share_pct={marshal_share:.1f}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
