# Node.js API

```sh
npm install @libraz/mysql-event-stream
```

Node.js 22 or later. The package is ESM with TypeScript declarations, and the addon is bundled rather than selected through optional platform dependencies — build [from source](getting-started.md#building-from-source) for a runtime it was not built for.

```typescript
import {
  BinlogClient,
  CdcEngine,
  CdcStream,
  isMesError,
  LogLevel,
  MesErrorCode,
  ServerFlavor,
  setLogCallback,
  SslMode,
} from "@libraz/mysql-event-stream";
```

Types: `ChangeEvent`, `ClientConfig`, `ColumnValue`, `EventType`, `LogHandler`, `MesError`, `PollResult`, `StreamConfig`.

## CdcStream

An `AsyncIterable<ChangeEvent>` and an `AsyncDisposable`.

```typescript
await using stream = new CdcStream(config);
for await (const event of stream) { /* ... */ }
```

| Member | Description |
| --- | --- |
| `new CdcStream(config: StreamConfig)` | Validates the whole config here rather than at first iteration, so a typo cannot leave the stream on a default nobody asked for. |
| `configure(overrides: Partial<StreamConfig>): void` | Replaces options before iteration starts; throws afterwards. Options that must be supplied together are judged against the configuration the update produces. |
| `close(): Promise<void>` | Interrupts the native poll, then finalizes the iterator. Idempotent. |
| `currentGtid: string` | The delivered, committed checkpoint. Survives `close()`. |

One stream supports one iteration; a second `for await` over the same object throws.

### StreamConfig

Extends `ClientConfig` with:

| Option | Default | Description |
| --- | --- | --- |
| `includeDatabases?: string[]` | all | Exact, case-sensitive database names. |
| `includeTables?: string[]` | all | `database.table`, a bare name, or a trailing `*`. |
| `excludeTables?: string[]` | none | Same forms; an exclude wins. |
| `maxReconnectAttempts?: number` | `10` | `0` disables reconnection. |
| `onMetadataError?: (error: Error) => void` | — | Called when the metadata connection fails. Unset means the failure is tolerated silently and column names fall back to numeric indices. |

### ClientConfig

| Option | Default | Description |
| --- | --- | --- |
| `host?: string` | `"127.0.0.1"` | |
| `port?: number` | `3306` | |
| `user?: string` | `"root"` | |
| `password?: string` | `""` | |
| `serverId?: number` | `1` | Replica identity. Must be unique per process — see [Server setup](server-setup.md#replica-identity). |
| `startGtid?: string` | — | Omitted snapshots the server's current set; `""` starts from the empty set. |
| `startBinlogFile?: string` | — | With `startBinlogPosition`, an exact file/offset start. Cannot be combined with `startGtid`. |
| `startBinlogPosition?: number` | — | 4 or greater. Requires `startBinlogFile`. |
| `connectTimeoutS?: number` | `10` | |
| `readTimeoutS?: number` | `30` | Bounds a single socket read, on the handshake and on the stream alike. |
| `sslMode?: SslMode` | `Preferred` | See [TLS and authentication](tls-and-authentication.md). |
| `sslCa?`, `sslCert?`, `sslKey?: string` | — | Certificate paths. An empty `sslCa` in a verification mode uses the OS trust store. |
| `allowPublicKeyRetrieval?: boolean` | `false` | Opts into unauthenticated RSA key retrieval. Prefer verified TLS. |
| `maxQueueSize?: number` | `10000` | |
| `maxQueueBytes?: number` | 48 MiB | |
| `maxEventSize?: number` | 32 MiB | `0` resolves to the 1 GiB hard cap. |

## BinlogClient

The constructor connects; a failure throws with the native code on it.

| Member | Description |
| --- | --- |
| `new BinlogClient(config: ClientConfig)` | Connects and validates the server configuration. |
| `start(): void` | Requests the binlog dump. |
| `poll(): Promise<PollResult>` | Resolves on the libuv thread pool, but the native call blocks until an event arrives or the stream stops. One poll at a time. |
| `pollBatch(maxEvents?: number): Promise<PollResult[]>` | Blocks for one event, then returns whatever else is already queued. |
| `stop(): void` | Callable from another thread; unblocks a pending `poll()`. |
| `disconnect(): void` | Closes the connection. |
| `destroy(): void` | Releases the native client. Idempotent. |

Read-only properties: `isConnected`, `isStreaming`, `lastError`, `flavor`, `currentGtid`, `checksumEnabled`, `queuedBytes`, `maxQueueBytes`, `maxEventSize`, `crcErrors`.

### PollResult

```typescript
interface PollResult {
  data: Uint8Array | null;
  isHeartbeat: boolean;
  checksumEnabled: boolean;
}
```

Frame the engine from `checksumEnabled` on the result, not from `client.checksumEnabled`. A `FORMAT_DESCRIPTION_EVENT` moves the client's framing while events read under the previous one are still queued.

## CdcEngine

| Member | Description |
| --- | --- |
| `new CdcEngine()` | |
| `feed(data: Uint8Array): number` | Returns bytes consumed. Stops early on a full queue. |
| `nextEvent(): ChangeEvent \| null` | `null` when the queue is empty. |
| `hasEvents(): boolean` | |
| `getPosition(): { file: string; offset: number \| bigint }` | |
| `reset(): void` | Clears the buffered bytes and the `TABLE_MAP` registry. |
| `setMaxQueueSize(n)`, `setMaxQueueBytes(n)`, `getMaxQueueBytes()` | |
| `setMaxEventSize(n)`, `getMaxEventSize()` | |
| `setChecksumEnabled(enabled)` | |
| `setTrailerPreVerified(v)`, `getTrailerPreVerified()` | Declares that the CRC32 was already verified upstream. Nothing checks the promise. |
| `setIncludeDatabases(list)`, `setIncludeTables(list)`, `setExcludeTables(list)` | |
| `enableMetadata(config: ClientConfig)` | Opens the metadata connection for column names. |
| `destroy(): void` | Releases the native engine. Idempotent. |

An engine holds native state; release it in a `finally` rather than waiting for the garbage collector.

## Errors

An error from the addon is a plain `Error`, `TypeError` or `RangeError` with a numeric `code`, not an instance of a class this package owns. Narrow a caught value with `isMesError()` and branch on `code`. See [Errors](errors.md).

## Logging

```typescript
setLogCallback(handler: LogHandler | null, level: LogLevel = LogLevel.Warn): void;
```

Process-wide, and the handler can run on the native reader thread. It does not by itself keep a process or a Worker alive.

Native delivery is bounded to 256 pending records. A JavaScript thread that falls behind loses the excess, and the next delivered record is preceded by `event=node_log_queue_overflow dropped=N`. Call `setLogCallback(null)` when the handler is no longer needed.

See [Logging](logging.md).
