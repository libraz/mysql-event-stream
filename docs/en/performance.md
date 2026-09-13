# Performance

## Targets

| Metric | Target |
| --- | --- |
| Decode latency | under 5 ms per event |
| Throughput | over 100,000 row events per second |
| Memory footprint | under 50 MB |

The measurements below come from the benchmark harnesses under `core/benchmarks`, run on an Apple M5 Max (18 cores, 6 of them performance cores) with a Release build. `core/benchmarks/BASELINE.md` records the full tables, the exact commands, and the host conditions each figure was taken under.

## Decode throughput

Single-threaded, row events per second, by row shape:

| Row shape | Row events/sec | µs per row event |
| --- | ---: | ---: |
| One nullable INT | 6,744,321 | 0.15 |
| 7 columns, temporal and DECIMAL | 3,101,396 | 0.32 |
| 7 columns, integers and strings | 3,395,970 | 0.29 |
| 28 mixed columns, INSERT | 1,977,437 | 0.51 |
| 28 mixed columns, UPDATE | 977,672 | 1.02 |

The widest shape measured — 28 columns with both images of every row — is still an order of magnitude above the 100,000/sec target.

Temporal and DECIMAL columns write their digits directly rather than through `std::snprintf`, which is worth 2.2–2.6× on any shape that carries one. The formatter would resolve the decimal point through `localeconv_l` and its process-wide lock on every field, which costs single-threaded decoding and costs far more across threads.

## Thread scaling

Independent `CdcEngine` instances share no allocator, no locale state and no lock, so aggregate throughput rises at every thread count.

| Row shape | 1 thread | 2 | 4 | 8 |
| --- | ---: | ---: | ---: | ---: |
| 7 columns, temporal | 3,655,937 | 1.89× | 3.51× | 5.31× |
| 7 columns, strings | 4,072,881 | 1.81× | 3.44× | 5.50× |
| 28 columns, 50 rows/event | 1,998,903 | 1.51× | 2.27× | 4.78× |

Eight threads return 4.8–5.5× rather than 8× because the host has 6 performance cores and 12 efficiency cores. The shape with the largest working set per row falls furthest short.

## Memory per queued event

What one pending event holds, by row shape:

| Row shape | Bytes/event | 10,000 events |
| --- | ---: | ---: |
| One nullable INT | 241 | 2.4 MB |
| 7 columns, temporal and DECIMAL | 657 | 6.6 MB |
| 28 mixed columns, INSERT | 2,418 | 24.2 MB |
| 28 mixed columns, UPDATE | 4,659 | 46.6 MB |
| 28 columns with an 8 KB annotation, 1 row/event | 10,667 | 106.7 MB |
| 28 columns with an 8 KB annotation, 50 rows/event | 2,585 | 25.9 MB |

Three effects account for the spread.

Row column storage dominates and cannot be amortised: every 28-column write lands within 3 bytes of 2,420 whatever its rows per event, because each row owns its own column array. An `UPDATE` pays twice that for holding both images.

A MariaDB `ANNOTATE_ROWS` statement is charged per `ROWS` event, not per row — one statement is shared by every row it annotates. At 8 KB that is 164 bytes per row at 50 rows per event, against 8 KB when the event carries a single row and there is nothing to share.

This forty-fold range across schemas is why the queue is bounded by bytes and not only by an entry count. The 10,000-entry default binds only while an event stays under roughly 5 KB; above that, the 48 MiB byte budget is what stops the reader first. See [Backpressure and limits](backpressure.md).

## Where the time goes

The hot paths are the row decoder's column loop, the engine's feed buffering, and the marshalling at the C ABI boundary.

The read-ahead buffer in front of the socket is worth 36× over a raw `recv` at binlog packet sizes (64-byte reads), and costs 2–5% at and above its 64 KiB capacity — inside the run-to-run spread of the same measurement.

In Python, column marshalling is 33% of the per-row cost for a 7-column row and 73% for a 28-column `UPDATE`. It copies a column payload by slicing a fixed-size window laid over the C buffer; calling `ctypes.string_at` once per column instead costs 1.37–1.47× on every shape with more than one column, because libffi's fixed per-call cost outweighs the copy at row payload sizes.
