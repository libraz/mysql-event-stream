# Decode performance baseline

Recorded numbers for the benchmark harnesses in this directory. Every figure
below comes from the commands quoted with it, so a later change can be compared
against the same measurement rather than against a remembered one.

## Measurement environment

| | |
|---|---|
| Date | 2026-08-15 |
| CPU | Apple M5 Max (18 cores), arm64 |
| RAM | 128 GB |
| OS | macOS 26.6 |
| Compiler | Apple clang 21.0.0 (`clang-2100.1.1.101`) |
| Build | `CMAKE_BUILD_TYPE=Release`, `-DBUILD_TESTING=OFF -DMES_BUILD_BENCHMARKS=ON` |
| Python | CPython 3.11.11 |

**The host was under concurrent load while these runs were taken** (load average
5-25 from unrelated work on the same machine). Repeated runs of the identical
command varied by up to 3x on the worst samples, so every table that compares
two builds alternates them run by run inside one session and reports medians;
an absolute rate from one section is not comparable with one from another.
Ratios within a section were stable across every round and are what the
conclusions rest on. Re-baselining on a quiet machine is worthwhile before
using these as a regression gate.

## Workloads

| Name | Shape |
|---|---|
| `int1_write_x1` | One nullable INT. Matches what `benchmark_feed` measures. |
| `temporal7_write_x1` | BIGINT + DATETIME2(6) + DATETIME2(0) + TIMESTAMP2(3) + DATE + TIME2(0) + DECIMAL(18,4). |
| `strings7_write_x1` | BIGINT + 4 VARCHAR + CHAR(32) + TEXT. Same column count as `temporal7`, no formatting work. |
| `wide28_write_x1` / `_x50` | 28 columns: integers, FLOAT/DOUBLE, three DECIMALs, three DATETIME2, two TIMESTAMP2, DATE, TIME2, YEAR, BIT(16), four VARCHAR, CHAR(32), TEXT, LONGBLOB. |
| `wide28_update_x1` / `_x50` | The same 28 columns as an UPDATE before/after pair. |
| `annotate{256,8k}_wide28_x{1,50,200}` | `wide28` preceded by a MariaDB ANNOTATE_ROWS statement of 256 B or 8192 B. |

Every TABLE_MAP carries SIGNEDNESS, COLUMN_CHARSET and COLUMN_NAME optional
metadata, and every event a valid CRC32, so the decode path is the one a real
server drives. `mes_benchmark_workloads --dump` prints the decoded values of
each schema for inspection.

## Throughput

Temporal and DECIMAL columns are rendered by writing their digits directly
rather than by calling `std::snprintf` per field. The two columns below are an
A/B pair of binaries differing only in `row_decoder.cpp` and
`binary_util.{h,cpp}`: *printf* formats through the varargs formatter, *direct*
through `binary::WritePaddedInt`. They were alternated run by run within one
session, so drift in the host's background load hits both equally. Each figure
is the median of 5-7 alternating runs of

```
mes_benchmark_workloads --only <workload> <iterations>
```

and each run is itself the median of 5 timed repeats inside the binary. The
iteration count is per workload, sized so a single run stays short enough to
alternate the binaries many times; the spread across the runs of one binary was
1-10% of its median on every workload, and 16% on the one worst case
(`strings7_write_x1`), which bounds how large a change this table can
resolve.

| Workload | Columns | Rows/event | Iterations | printf | direct | change | us/row event |
|---|---:|---:|---:|---:|---:|---:|---:|
| `int1_write_x1` (control) | 1 | 1 | 20,000 | 6,657,420 | 6,744,321 | +1% | 0.15 -> 0.15 |
| `temporal7_write_x1` | 7 | 1 | 20,000 | 1,190,934 | 3,101,396 | **2.60x** | 0.84 -> 0.32 |
| `strings7_write_x1` (control) | 7 | 1 | 20,000 | 3,470,591 | 3,395,970 | -2% | 0.29 -> 0.29 |
| `wide28_write_x1` | 28 | 1 | 8,000 | 753,650 | 1,699,115 | **2.25x** | 1.33 -> 0.59 |
| `wide28_write_x50` | 28 | 50 | 4,000 | 832,252 | 1,977,437 | **2.38x** | 1.20 -> 0.51 |
| `wide28_update_x1` | 28 | 1 | 5,000 | 403,539 | 881,232 | **2.18x** | 2.48 -> 1.13 |
| `wide28_update_x50` | 28 | 50 | 2,000 | 418,334 | 977,672 | **2.34x** | 2.39 -> 1.02 |

The two controls carry no temporal or DECIMAL column and move by 1-2%, which is
the run-to-run spread of this measurement; every workload that formats one gains
2.2-2.6x. The `annotate*` workloads, measured the same way in the same session,
move with the rest: `annotate256_wide28_x50` 834,668 -> 1,963,333,
`annotate8k_wide28_x50` 835,831 -> 1,911,572, `annotate8k_wide28_x200`
827,712 -> 1,924,363 row events/sec. Those absolute rates are not comparable
with the ANNOTATE section below, which is a different pair of binaries measured
in a different session.

Reference point: `build-bench/core/mes_benchmark_feed` reports 5,364,051
events/sec on the same host, consistent with `int1_write_x1`.

## ANNOTATE statement sharing

An ANNOTATE_ROWS event annotates every row of the ROWS event that follows it.
A `ChangeEvent` holds the statement by `shared_ptr`, so all rows of one ROWS
event reference a single copy; giving each row its own copy charged the
statement length once per row, which for an 8 KB statement is the whole cost of
a queued event.

Both columns below come from one session on the same host, alternating the two
binaries within each round and repeating the sweep with the order reversed, so
neither figure is favoured by drift in the host's background load. Each entry
is the fastest of 5 runs (6 for `annotate8k_wide28_x50` and the control) of

```
mes_benchmark_workloads --only <workload> 20000    # 5000 for the x200 workload
```

| Workload | Rows/event | per row copy | shared | change |
|---|---:|---:|---:|---:|
| `annotate256_wide28_x1` | 1 | 739,795 | 744,245 | +0.6% |
| `annotate256_wide28_x50` | 50 | 568,964 | 624,206 | +9.7% |
| `annotate8k_wide28_x1` | 1 | 413,536 | 424,142 | +2.6% |
| `annotate8k_wide28_x50` | 50 | 512,460 | 571,839 | +11.6% |
| `annotate8k_wide28_x200` | 200 | 536,173 | 610,804 | +13.9% |
| `wide28_write_x50` (control) | 50 | 795,476 | 794,500 | 0.0% |

Row events/sec. Process user CPU time for the same runs moves the same way and
is the less load-sensitive of the two measurements: `annotate8k_wide28_x50`
spends 10.60 s median (6 runs) copying per row against 9.21 s sharing, -13%.
The no-annotate control is unchanged, as expected.

The host carried a heavier background load during this sweep than during the
throughput table above (load average 11-44 against 18-22), so only the two
columns here are comparable with each other; neither is comparable with an
absolute rate from the table above.

Queued memory for the same workloads is in the table below. Sharing removes the
duplication but adds one control block per ANNOTATE_ROWS event, about 40 bytes;
where an event carries exactly one row there is nothing to share and that 40
bytes is a net loss.

## Memory per queued event

`build-bench/core/mes_benchmark_workloads_mem 10000`. The binary replaces global
`operator new`, fills the engine queue without draining it, and reports the
bytes still resident divided by the number of queued row events. `resident_total`
is what 10,000 pending events actually hold. The measurement is deterministic:
repeated runs give identical byte counts.

The *copy* and *shared* byte columns are the ANNOTATE statement A/B pair, taken
in one session from the same two binaries as the throughput comparison above.
The **allocs/event column is a single figure re-measured on the finished tree**,
not part of that pair: the temporal/DECIMAL formatting change landed after the
A/B run and removed allocations the shared-statement work had nothing to do
with, so carrying an A/B split there would attribute them to the wrong change.
The byte columns are unaffected — re-measuring reproduces the *shared* column
exactly.

| Workload | bytes/event copy | bytes/event shared | allocs/event | 10k events copy | 10k events shared |
|---|---:|---:|---:|---:|---:|
| `int1_write_x1` | 274 | 265 | 0.1 | 2.7 MB | 2.7 MB |
| `temporal7_write_x1` | 756 | 748 | 1.1 | 7.6 MB | 7.5 MB |
| `strings7_write_x1` | 497 | 489 | 3.1 | 5.0 MB | 4.9 MB |
| `wide28_write_x1` | 2,716 | 2,708 | 6.1 | 27.2 MB | 27.1 MB |
| `wide28_write_x50` | 661 | 653 | 6.1 | 6.6 MB | 6.5 MB |
| `wide28_update_x1` | 4,089 | 4,080 | 12.1 | 40.9 MB | 40.8 MB |
| `wide28_update_x50` | 1,113 | 1,105 | 12.1 | 11.1 MB | 11.0 MB |
| `annotate256_wide28_x1` | 922 | 962 | 8.1 | 9.2 MB | 9.6 MB |
| `annotate256_wide28_x50` | 925 | 659 | 6.1 | 9.3 MB | 6.6 MB |
| `annotate8k_wide28_x1` | 8,860 | 8,899 | 8.1 | 88.6 MB | 89.0 MB |
| `annotate8k_wide28_x50` | 8,862 | 818 | 6.1 | 88.6 MB | 8.2 MB |
| `annotate8k_wide28_x200` | 8,870 | 702 | 6.1 | 88.7 MB | 7.0 MB |

Writing temporal and DECIMAL digits directly instead of through `std::snprintf`
also removed one allocation per such column: `temporal7` fell from 2.1 to 1.1
allocs/event and every `wide28` variant by 2.0, because the formatted value no
longer outgrows its small-string buffer.

Four further effects are visible:

* ANNOTATE SQL is now charged **per ROWS event**, not per row: at 8 KB the cost
  falls from 8,862 to 818 bytes/event at 50 rows and to 702 at 200 rows, i.e.
  statement length divided by the row count plus the no-annotate baseline. It
  used to be identical at 1, 50 and 200 rows per event.
* At exactly one row per event there is nothing to share, and the event pays
  ~40 bytes and one allocation more for the shared statement's control block.
  8,860 -> 8,899 bytes/event is the price of the 10.8x saving at 50 rows.
* Every workload drops 8 bytes/event because a `shared_ptr` member is 8 bytes
  smaller than the `std::string` it replaced.
* The `_x1` variants pay for a fresh `TableMetadata` per event because each
  event is preceded by its own TABLE_MAP; at 50 rows per event that cost is
  amortised (2,716 -> 653 bytes/event).

## Thread scaling

`mes_benchmark_workloads --only <workload> --scaling <iterations>`, one
independent `CdcEngine` per thread, the same A/B pair as the throughput table
alternated over three rounds. Iterations per thread: 2,000,000 for
`temporal7_write_x1`, 3,000,000 for `strings7_write_x1`, 60,000 for
`wide28_write_x50`. Each cell is the median of the three rounds, in aggregate
row events/sec across all threads.

| Workload | Build | 1t | 2t | 4t | 8t |
|---|---|---:|---:|---:|---:|
| `temporal7_write_x1` | printf | 888,665 | 1,234,565 | 990,932 | 752,197 |
| | direct | 2,185,870 | 2,766,594 | 1,647,887 | 1,121,309 |
| `strings7_write_x1` | printf | 3,160,057 | 1,879,308 | 1,203,927 | 1,365,767 |
| | direct | 3,242,570 | 1,914,349 | 1,205,419 | 1,373,195 |
| `wide28_write_x50` | printf | 755,330 | 816,996 | 685,094 | 556,627 |
| | direct | 1,699,320 | 1,528,195 | 1,211,483 | 834,326 |

Every thread count decodes faster with the digits written directly: 1.49x at 8
threads on `temporal7_write_x1` and 1.50x on `wide28_write_x50`. The *ratio* to
the same build's own single-thread rate is nonetheless worse (0.85 -> 0.51 and
0.74 -> 0.49 at 8 threads), because the single-thread rate is what improved
most.

Aggregate throughput therefore still **falls** as threads are added past each
workload's peak -- two threads for the two that format columns, one thread for
`strings7_write_x1`. The locale lock printf took on every field is gone: every
`os_unfair_lock` sample in the printf profiles sits under `localeconv_l`, and
both are absent from the direct build. What remains is the other serialization
point, `std::pmr::synchronized_pool_resource`, now 67% of the 8-thread
profile.

## Plaintext socket read

`build-bench/core/mes_benchmark_socket_read` (256 MB per measurement, median of
5 after a warmup pass, loopback TCP).

| Chunk | `ReadExact` MB/s | spread | raw `recv` MB/s | ratio |
|---:|---:|---:|---:|---:|
| 64 B | 6,312 | 1,400 | 172 | 36.61 |
| 16 KiB | 8,052 | 1,166 | 6,808 | 1.18 |
| 64 KiB | 9,744 | 968 | 9,988 | 0.98 |
| 1 MiB | 8,625 | 2,616 | 9,092 | 0.95 |

The read-ahead buffer is worth 36x at binlog packet sizes. At and above its
64 KiB capacity the staging copy costs 2-5%, which is inside the 10-30%
run-to-run spread of the same measurement.

## Python column marshalling

`python3 bindings/python/benchmarks/bench_convert_columns.py --streams <dir>
--lib build-bench/core/libmes.dylib --iterations 1000`, median of 5 repeats.
`make benchmark-python` runs the same thing against whatever `python3` resolves
to; this table was taken on CPython 3.11.11. "Stubbed" replaces
`_convert_columns` with a no-op so the difference isolates marshalling from the
rest of the per-row Python path.

Every cell reads `payload-per-column string_at` -> `payload window`.
`_convert_columns` copies a column payload by slicing a fixed-size `c_char`
window laid over the C buffer; the left-hand figure is the field-at-a-time
variant that called `ctypes.string_at` once per column, which
`bindings/python/tests/test_column_marshalling.py` keeps as the differential
reference. The two were timed alternately inside a single process over five
rounds, so both see the same machine state.

| Workload | Columns | ns/column | us/row event | stubbed | marshalling share |
|---|---:|---:|---:|---:|---:|
| `int1_write_x1` | 1 | 185 -> 191 | 3.14 -> 3.07 | 2.87 -> 2.67 | 9% -> 13% |
| `temporal7_write_x1` | 7 | 337 -> 228 | 6.42 -> 5.61 | 3.81 -> 3.74 | 41% -> 33% |
| `strings7_write_x1` | 7 | 340 -> 232 | 5.78 -> 5.00 | 3.08 -> 3.08 | 47% -> 38% |
| `wide28_write_x1` | 28 | 281 -> 205 | 13.29 -> 10.74 | 4.37 -> 4.55 | 67% -> 58% |
| `wide28_write_x50` | 28 | 286 -> 203 | 11.47 -> 9.21 | 3.00 -> 2.98 | 74% -> 68% |
| `wide28_update_x50` | 28 | 287 -> 207 | 21.37 -> 16.79 | 4.56 -> 4.57 | 79% -> 73% |

`ctypes.string_at` is a libffi call whose fixed cost outweighs the copy itself
at row payload sizes, so dropping it from the per-column path is worth
1.37-1.47x on ns/column for every workload with more than one column.
`int1_write_x1` carries no payload to copy, so all it pays for is the window
alias it binds and never uses: 6 ns per call here, and 4 ns when the same
alternating comparison is timed on CPU time rather than wall clock.

## Profiles

`sample <pid> 8`, Release build, single-threaded unless noted. Percentages are
of all samples in the process, computed by summing the topmost occurrence of
each subtree.

Each cell is *printf* -> *direct*, the same pair of binaries as the throughput
table, profiled in one session.

| Workload | `snprintf` subtree | `memmove` | malloc | crc32 |
|---|---:|---:|---:|---:|
| `temporal7_write_x1` | 60.9% -> 0.0% | 9.2% -> 6.7% | 3.5% -> 4.0% | 1.1% -> 2.6% |
| `wide28_write_x50` | 52.7% -> 0.0% | 10.3% -> 10.4% | 9.4% -> 11.5% | 0.7% -> 2.3% |
| `strings7_write_x1` | 0.0% -> 0.0% | 3.3% -> 3.0% | 11.4% -> 16.8% | 6.8% -> 5.1% |
| `annotate8k_wide28_x50` | 52.1% -> 0.0% | 10.1% -> 9.2% | 9.8% -> 15.6% | 1.1% -> 2.9% |

`strings7_write_x1` runs the same code in both builds and still moves 5 points
on the malloc column, so treat any single-column movement smaller than that as
sampling noise. What the table shows is the `snprintf` subtree disappearing and
the remaining work redistributing over a run that is 2.2-2.6x shorter for the
same number of rows -- the shares grow because the denominator shrank, not because
those subtrees got slower.

In `annotate8k_wide28_x50`, `CdcEngine::ProcessRowEvent` ->
`std::string::__assign_no_alias` -> `__grow_by_and_replace` -- the per-row copy
of the ANNOTATE statement -- accounted for 606 of 6,844 samples (8.9%) while
each row held its own copy. With the statement shared, that subtree is absent:
0 of 6,875 samples in the same profile mention `__assign_no_alias`. The
`__grow_by_and_replace` samples that remain belong to the column decoders.

`temporal7_write_x1` on 8 threads, all-thread samples:

| Subtree | printf 1t | printf 8t | direct 1t | direct 8t |
|---|---:|---:|---:|---:|
| `snprintf` | 60.9% | 42.1% | 0.0% | 0.0% |
| `localeconv_l` (within `snprintf`) | 3.4% | 30.2% | 0.0% | 0.0% |
| `os_unfair_lock` (the locale lock) | 1.5% | 29.9% | 0.0% | 0.0% |
| `std::pmr::synchronized_pool_resource::do_allocate` | 0.8% | 15.6% | 2.0% | 38.0% |
| `std::pmr::synchronized_pool_resource::do_deallocate` | 0.7% | 16.1% | 1.8% | 29.0% |

`localeconv_l` resolves the decimal point on every printf call and takes a
process-wide `os_unfair_lock` to do it; at 8 threads that lock was 30% of all
samples. Writing the digits directly removes it outright. The pool resource is
the serialization point left standing.

## Position against the project targets

`CLAUDE.md` sets > 100k row events/sec, < 5 ms decode latency per event, and a
< 50 MB memory footprint.

* **Throughput**: met with margin everywhere. The slowest workload
  (`wide28_update_x1`, 881k/sec) is 8.8x the target.
* **Latency**: met by three orders of magnitude. The slowest row event costs
  1.13 us against a 5 ms budget.
* **Memory**: met except when every queued event carries its own distinct
  large statement. A full default queue (`MES_DEFAULT_QUEUE_SIZE` = 10,000) of
  `wide28` rows annotated with an 8 KB statement holds 8.2 MB at 50 rows per
  event and 7.0 MB at 200, against 6.5 MB for the same rows with no ANNOTATE.
  The one case still above the target is 10,000 single-row events each with
  their own 8 KB statement: 89.0 MB, of which 81.9 MB is the statement text
  itself. That is data the caller asked for rather than duplication, and
  `mes_set_max_queue_size()` is the lever for bounding it.
