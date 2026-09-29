# Threading and lifecycle

Every object here is a single-owner object. One thread or task uses it at a time, and the library does not serialize concurrent calls on your behalf.

## CdcEngine

Do not call `feed()`, `nextEvent()`, `reset()`, or the filter and configuration setters concurrently on the same engine. Use one engine per thread or task, or serialize access yourself.

Independent engines share nothing — no allocator, no locale state, no lock — so decoding scales with the thread count. Eight threads reach 4.8–5.5× the single-thread rate on this project's benchmark host; see [Performance](performance.md).

An engine holds native state. Release it rather than waiting for a garbage collector:

```typescript
const engine = new CdcEngine();
try {
  // ...
} finally {
  engine.destroy();
}
```

```python
with CdcEngine() as engine:
    ...
```

`destroy()` and `close()` are idempotent.

## BinlogClient

The client runs an internal reader thread. Polling, iteration and the connection lifecycle calls are single-owner operations, and at most one poll may be in flight.

`stop()` is the exception, and the only one: it is callable from another thread and unblocks a pending `poll()`. This is the cancellation path — closing or destroying a client from another thread while a poll is blocked is not.

```typescript
process.on("SIGINT", () => client.stop());
```

Final destruction requests a stop and waits for in-flight poll access to finish before releasing the native client, so a shutdown does not race a reader mid-event.

## CdcStream

`CdcStream` has no `stop` method. Cancel it with `close()` from the task that owns the stream: `close()` interrupts the native poll first and then finalizes the iterator.

```typescript
await using stream = new CdcStream(config);
for await (const event of stream) {
  await handle(event);
}
```

```python
async with CdcStream(...) as stream:
    async for event in stream:
        await handle(event)
```

`await using` and `async with` close the stream on the way out, including on an exception. Without them, call `close()` / `aclose()` in a `finally`.

One stream supports one iteration. A second `for await` over the same object raises rather than interleaving two consumers on one connection.

In Node, each active stream occupies one blocking native poll worker, and that worker holds a libuv thread-pool slot even while idle. `pollBatch()` amortizes the handoff by draining up to 64 queued events after the first result, but it does not release the slot. Node defaults to four slots, so more than four concurrently idle streams need `UV_THREADPOOL_SIZE` raised before the process starts:

```sh
UV_THREADPOOL_SIZE=16 node app.mjs
```

`configure()` overrides options before iteration starts and raises afterwards — a stream that has started is streaming against a connection already negotiated from its configuration.

## At the C ABI

`mes_engine_t` and `mes_client_t` are not thread-safe. `mes_client_t` has eight exceptions — `mes_client_stop()` and the observers `mes_client_is_connected()`, `mes_client_is_streaming()`, `mes_client_checksum_enabled()`, `mes_client_queued_bytes()`, `mes_client_crc_errors()`, `mes_client_last_error()` and `mes_client_current_gtid()` — all callable from another thread. See [C API](c-api.md#invariants) for which are safe while a call other than `mes_client_destroy()` is in flight.

Event pointers returned by `mes_next_event()` are valid only until the next `mes_feed()`, `mes_next_event()` or `mes_reset()`; `mes_client_poll()` data only until the next poll. Copy anything that has to outlive the call. See the [C API](c-api.md).
