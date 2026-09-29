# Column names

Row data in a binlog carries values, not names. There are two ways to get names onto the keys of `before` and `after`, and they have different guarantees.

## From the binlog itself

With `binlog_row_metadata=FULL` the server writes column names into the `TABLE_MAP` event. Nothing else is needed: the names arrive with the data they describe, they match the schema as it was when the event was written, and no extra connection is opened.

This is the route to use for replay from an old position, and the only one that is correct there.

## From a metadata connection

Otherwise the names come from a separate connection that runs `SHOW COLUMNS` when a `TABLE_MAP` event introduces a table. Each surface opens that connection differently.

`CdcStream` opens it from the stream's own connection settings, so a stream needs no extra call.

`CdcEngine` does not. A caller feeding bytes to an engine enables the connection explicitly:

```typescript
const engine = new CdcEngine();
engine.enableMetadata({
  host: "mysql.example.com",
  user: "replicator",
  password: "secret",
  readTimeoutS: 30,
});
```

```python
engine = CdcEngine()
engine.enable_metadata(
    host="mysql.example.com",
    user="replicator",
    password="secret",
    read_timeout_s=30,
)
```

The credentials need `SELECT` on the streamed tables. The `SHOW COLUMNS` query runs synchronously while the `TABLE_MAP` event is being processed. A timeout on that query reconnects and retries once within the same call, so the worst case for one event is a read timeout, a connect timeout, a handshake read timeout and a second read timeout — all bounded by `readTimeoutS` / `read_timeout_s` and `connectTimeoutS` / `connect_timeout_s`, or by the library defaults (30 and 10 seconds respectively) when those options are `0`. If every attempt still fails, that event's names are left unresolved. A table that keeps failing — denied privilege, or another server error — is not requeried on every subsequent `TABLE_MAP`: it stays unresolved for 30 seconds before the next one retries, while a schema change (DDL) clears that immediately.

## Checking whether it worked

`namesResolved` / `names_resolved` is false when any column name for that event's table could not be resolved, and the keys are then the numeric indices `"0"`, `"1"`, `"2"` as strings. Check the flag on every event rather than assuming resolution succeeded.

```typescript
for await (const event of stream) {
  if (!event.namesResolved) {
    metrics.increment("cdc.unnamed_columns");
    continue;
  }
  await handle(event);
}
```

`CdcStream` reports a failure to *enable* the metadata connection through `onMetadataError` / `on_metadata_error`; it does not fire for a lookup that fails mid-stream, which instead surfaces as a log event with `namesResolved` / `names_resolved` false on the affected rows. Without that callback the enable failure is tolerated silently and the keys fall back to indices — the library writes nothing to stderr on its own.

## The schema it reads is the current one

The metadata connection reads the server's schema as it is now, not as it was at the binlog position being decoded. Names from that route are authoritative only while consuming at the current head.

A stream replaying from an older position crosses any `ALTER TABLE` committed since, and `SHOW COLUMNS` returns the post-`ALTER` layout for pre-`ALTER` rows. Use `binlog_row_metadata=FULL` for replay, where the names travel with the event.
