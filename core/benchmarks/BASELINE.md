# Decode performance baseline

Recorded numbers for the benchmark harnesses in this directory. Every figure
below comes from the commands quoted with it, so a later change can be compared
against the same measurement rather than against a remembered one.

## Measurement environment

| | |
|---|---|
| Date | 2026-08-15; queued memory and thread scaling re-measured 2026-09-13 |
| CPU | Apple M5 Max (18 cores), arm64 |
| RAM | 128 GB |
| OS | macOS 26.6 |
| Compiler | Apple clang 21.0.0 (`clang-2100.1.1.101`) |
| Build | `CMAKE_BUILD_TYPE=Release`, `-DBUILD_TESTING=OFF -DMES_BUILD_BENCHMARKS=ON` |
| Python | CPython 3.11.11 |

**The host was under concurrent load while the two-build comparisons were
taken** (load average 5-25 from unrelated work on the same machine). Repeated
runs of the identical command varied by up to 3x on the worst samples, so every
table that compares two builds alternates them run by run inside one session and
reports medians; an absolute rate from one section is not comparable with one
from another. Ratios within a section were stable across every round and are
what the conclusions rest on.

The queued-memory and thread-scaling sections were taken later on a quiet host
(load average 1.6-4.1) against a single build, so their absolute figures are
usable directly. They are not comparable with an absolute rate from a two-build
section above.

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
duplication but adds one control block per ANNOTATE_ROWS event, about 56 bytes;
where an event carries exactly one row there is nothing to share and that 56
bytes is a net loss.

## Memory per queued event

`build-bench/core/mes_benchmark_workloads_mem 10000`. The binary replaces global
`operator new`, fills the engine queue without draining it, and reports the
bytes still resident divided by the number of queued row events, verified
against the count the engine is actually holding rather than the count the
workload was expected to produce. `resident_total` is what 10,000 pending events
actually hold.

**Each workload is measured in its own process.** `max_rss_bytes` is the peak
resident size of the whole process and only ever grows, so in a shared process
every workload after the first would report its predecessor's peak rather than
its own. The harness forks before it has decoded anything, which also keeps
whatever the decode path builds lazily on first use out of the first workload's
figures. Within that isolation the figures are deterministic and identical
between a debug and a release build, because what is counted is allocated bytes
rather than elapsed time.

| Workload | bytes/event | allocs/event | 10k events |
|---|---:|---:|---:|
| `int1_write_x1` | 241 | 1.0 | 2.4 MB |
| `temporal7_write_x1` | 657 | 2.0 | 6.6 MB |
| `strings7_write_x1` | 913 | 4.0 | 9.1 MB |
| `wide28_write_x1` | 2,418 | 7.0 | 24.2 MB |
| `wide28_write_x50` | 2,421 | 7.0 | 24.2 MB |
| `wide28_update_x1` | 4,659 | 14.0 | 46.6 MB |
| `wide28_update_x50` | 4,665 | 14.0 | 46.6 MB |
| `annotate256_wide28_x1` | 2,730 | 9.0 | 27.3 MB |
| `annotate256_wide28_x50` | 2,427 | 7.1 | 24.3 MB |
| `annotate8k_wide28_x1` | 10,667 | 9.0 | 106.7 MB |
| `annotate8k_wide28_x50` | 2,585 | 7.1 | 25.9 MB |
| `annotate8k_wide28_x200` | 2,470 | 7.1 | 24.7 MB |

Three effects are visible:

* **Row column storage dominates and cannot be amortised.** Every `wide28` write
  workload lands within 3 bytes of 2,420 whatever its rows per event, because
  each row owns its own column array however the events were framed. An UPDATE
  pays twice that, 4,659-4,665, for holding a before and an after image.
* **ANNOTATE SQL is charged per ROWS event, not per row.** At 8 KB the statement
  adds its length divided by the row count on top of that baseline: 8,192/50 =
  164 bytes at 50 rows and 41 at 200, so 2,585 and 2,470 against the 2,420 of
  the same schema with no annotation.
* **At one row per event there is nothing to share**, and the event instead pays
  ~56 bytes and one allocation for the statement's control block: 2,730 and
  10,667 are 2,418 plus the whole statement plus that overhead.

The column array of a row is one allocation per row, which is what separates
each workload's `allocs/event` from the count of its variable-length column
payloads: `int1_write_x1` allocates once for an event with no payload at all.

These are per-event costs, not a queue footprint: the measurement lifts the
queue byte budget so that nothing is retired before the snapshot. It is that
budget, not the entry count, that bounds a running engine, and these figures are
why the entry count alone could not. `MES_DEFAULT_QUEUE_SIZE` is 10,000 entries
and `MES_DEFAULT_QUEUE_BYTES` is 48 MB, so the entry count is what binds only
while an event costs under roughly 5 KB. The table spans 241 bytes to 10,667, a
forty-fold range set by the schema in front of the engine; at the top of it the
byte budget retires events at roughly half the default entry count. The budget
charges an event for the column names and statement that produced it as well,
so these figures place a workload in that range rather than predicting the
exact entry count at which it is retired.

## Thread scaling

`mes_benchmark_workloads --only <workload> --scaling <iterations>`, one
independent `CdcEngine` per thread, three rounds. Iterations per thread:
2,000,000 for `temporal7_write_x1`, 3,000,000 for `strings7_write_x1`, 60,000
for `wide28_write_x50`. Each cell is the median of the three rounds, in
aggregate row events/sec across all threads; the parenthesised figure is that
median against the same workload's own single-thread median.

| Workload | 1t | 2t | 4t | 8t |
|---|---:|---:|---:|---:|
| `temporal7_write_x1` | 3,655,937 | 6,910,545 (1.89x) | 12,820,813 (3.51x) | 19,408,091 (5.31x) |
| `strings7_write_x1` | 4,072,881 | 7,385,539 (1.81x) | 14,011,566 (3.44x) | 22,383,146 (5.50x) |
| `wide28_write_x50` | 1,998,903 | 3,009,955 (1.51x) | 4,542,358 (2.27x) | 9,561,361 (4.78x) |

Aggregate throughput rises at every thread count on every workload. Independent
engines share no allocator, no locale state and no lock, so what the numbers are
bounded by is the host: this machine has 6 performance cores and 12 efficiency
cores, which is why 8 threads returns 4.8-5.5x rather than 8x, and why
`wide28_write_x50` -- the workload with the largest working set per row --
falls furthest short.

Two things in the decode path are what this measurement is sensitive to, and
neither takes a lock. Temporal and DECIMAL columns write their digits directly
instead of through `std::snprintf`, which would resolve the decimal point
through `localeconv_l` and its process-wide `os_unfair_lock` on every field.
Row column arrays are allocated from `std::pmr::new_delete_resource()`, a
stateless resource with nothing for threads to contend over; `core/src/types.h`
records why it has to be that one resource for every row in the process.

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

`localeconv_l` resolves the decimal point on every printf call and takes a
process-wide `os_unfair_lock` to do it; at 8 threads that lock was 30% of all
samples. Writing the digits directly removes it outright.

No `std::pmr` pool symbol appears anywhere in a profile of this build; row
column arrays go straight to the system allocator, which accounts for 408 of the
5,220 samples on the decoding thread of a single-thread run, 7.8%. How that
allocator behaves as engines are added is read off the thread-scaling table
above, which measures the outcome rather than attributing it.

## Position against the project targets

`CLAUDE.md` sets > 100k row events/sec, < 5 ms decode latency per event, and a
< 50 MB memory footprint.

* **Throughput**: met with margin everywhere. The slowest workload
  (`wide28_update_x1`, 881k/sec) is 8.8x the target.
* **Latency**: met by three orders of magnitude. The slowest row event costs
  1.13 us against a 5 ms budget.
* **Memory**: met by the defaults, because `MES_DEFAULT_QUEUE_BYTES` is 48 MB
  and it is the bound that binds first on anything wide. Taking the entry-count
  default on its own, 10,000 queued events of `wide28` rows annotated with an
  8 KB statement would hold 25.9 MB at 50 rows per event and 24.7 MB at 200,
  against 24.2 MB for the same rows with no ANNOTATE. The one shape that would
  pass the target is 10,000 single-row events each with their own 8 KB
  statement, 106.7 MB, of which 81.9 MB is the statement text itself -- data the
  caller asked for rather than duplication. The byte budget retires that shape
  at roughly 4,700 events, well inside the target; `mes_set_max_queue_bytes()`
  is the lever for moving it.
