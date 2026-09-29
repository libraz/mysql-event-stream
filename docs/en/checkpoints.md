# Checkpoints and recovery

A stream started without a start position begins at the server's current position. Every change committed while the process was down is skipped. To pick up where the last run stopped, read the checkpoint the stream publishes and pass it back on the next run.

## Resuming

```typescript
await using stream = new CdcStream({
  host: "mysql.example.com",
  user: "replicator",
  password: "secret",
  serverId: 1001,
  startGtid: loadCheckpoint(), // omit to start at the server's current position
});

for await (const event of stream) {
  await handle(event);
  saveCheckpoint(stream.currentGtid);
}
```

```python
async with CdcStream(
    host="mysql.example.com",
    user="replicator",
    password="secret",
    server_id=1001,
    start_gtid=load_checkpoint(),  # omit to start at the server's current position
) as stream:
    async for event in stream:
        await handle(event)
        save_checkpoint(stream.current_gtid)
```

`currentGtid` / `current_gtid` covers every event delivered up to, but not including, the last `poll()` / `poll_batch()` result — `stop()`, `close()`, `disconnect()` and `destroy()` never advance it further. It stays readable and unchanged after the stream closes, so it can also be persisted once after the loop, including after a `break`, but the last delivered event or batch is excluded and is redelivered on the next resume: at-least-once, not exactly-once. `BinlogClient` carries the same pair under the same names.

## At-least-once, and what that costs you

Delivery is at-least-once. An event can be redelivered after a reconnect, so:

- Persist a checkpoint only after your own processing of that event has succeeded.
- Make that processing idempotent.

Saving the checkpoint first turns a crash into lost events; making the handler idempotent turns a redelivery into a no-op. The order above is the one that loses nothing.

## Starting somewhere exact

Three start modes exist, and a configuration picks one.

| Mode | How to ask for it |
| --- | --- |
| The server's current position | Omit `startGtid` and the binlog file/offset pair. |
| A GTID set | `startGtid` / `start_gtid`. An empty string requests the empty set — everything the server still has, but only when nothing has been purged. It goes through the same [purged-GTID preflight](#purged-positions) as any other requested set, so a source with a non-empty `gtid_purged` fails it too. |
| A binlog file and offset | `startBinlogFile` with `startBinlogPosition` (4 or greater — the first event begins after the file's 4-byte magic number). |

The two explicit modes cannot be combined: they name different start points and only one can be honoured. An offset naming no file is refused at configuration time rather than accepted and dropped.

## How a GTID entry is read

A MySQL entry naming a bare transaction number is widened before it goes on the wire. `uuid:N` resumes from `uuid:1-N`, and the tagged form `uuid:tag:N` from `uuid:tag:1-N`, so transactions 1 through N-1 are never delivered. An entry that already states an interval such as `uuid:5-9` is sent as written, and `uuid:0` contributes nothing.

MariaDB GTIDs use the `domain-server-seq` form and are sent verbatim.

## Purged positions

A GTID the server has already purged fails with code 405 rather than silently restarting from the head. Restarting from the head would leave a gap no later run could notice, so the failure is the useful outcome: take a fresh snapshot and start a new stream from the checkpoint captured with it.

## Automatic reconnection

`CdcStream` reconnects on its own after a transport failure, resuming from the last delivered checkpoint. Attempt *N* waits a base of `min(N seconds, 10s)` multiplied by 50–100% jitter, so the waits run 0.5–1s, 1–2s, and so on up to 5–10s once the cap is reached. Both bindings use the same schedule.

```typescript
const stream = new CdcStream({
  host: "mysql.example.com",
  user: "replicator",
  maxReconnectAttempts: 10, // default 10; 0 disables reconnection
});
```

Exhausting the attempts ends the iteration with the underlying error. A failure that reconnecting cannot fix — bad credentials (401), a rejected server configuration (402), a purged GTID (405) — is not retried at all. [Errors](errors.md) has the full division.

`BinlogClient` does not reconnect. It reports the failure and leaves the decision to the caller.

## Taking the first checkpoint

There is no snapshot facility here — the library reads the log, not the tables. An initial load looks like this:

1. Read the server's current GTID set and keep it.
2. Load the existing rows however you like: `mysqldump`, a `SELECT`, a replica.
3. Start the stream with the GTID set from step 1 as `startGtid`.

Any change committed during step 2 is in the log after that position, so the stream delivers it. The handler being idempotent is what makes the overlap harmless.
