# Node.js API

```sh
npm install @libraz/mysql-event-stream
```

Node.js 22 以降が必要です。パッケージは ESM で TypeScript の型定義付き、アドオンはオプショナルなプラットフォーム依存で選ぶのではなく同梱してあります。ビルド対象に含まれないランタイムで動かすには[ソースからビルド](getting-started.md#ソースからのビルド)してください。

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

型: `ChangeEvent`、`ClientConfig`、`ColumnValue`、`EventType`、`LogHandler`、`MesError`、`PollResult`、`StreamConfig`。

## CdcStream

`AsyncIterable<ChangeEvent>` であり、`AsyncDisposable` でもあります。

```typescript
await using stream = new CdcStream(config);
for await (const event of stream) { /* ... */ }
```

| メンバー | 説明 |
| --- | --- |
| `new CdcStream(config: StreamConfig)` | 最初の反復時ではなく、ここで設定全体を検証します。綴り誤りがあったときに、誰も指定していない既定値のままストリームが動き出すことはありません。 |
| `configure(overrides: Partial<StreamConfig>): void` | 反復が始まる前にオプションを差し替えます。始まったあとは例外を投げます。同時に指定しなければならないオプションは、更新後の設定に対して判定します。 |
| `close(): Promise<void>` | ネイティブの poll を中断し、イテレータを終了させます。冪等です。 |
| `currentGtid: string` | 配信済みでコミットされたチェックポイント。`close()` のあとも残ります。 |

1 つのストリームで反復できるのは 1 回だけです。同じオブジェクトに 2 回目の `for await` をかけると例外になります。

### StreamConfig

`ClientConfig` を次のオプションで拡張します。

| オプション | 既定値 | 説明 |
| --- | --- | --- |
| `includeDatabases?: string[]` | すべて | 完全一致で大文字小文字を区別するデータベース名。 |
| `includeTables?: string[]` | すべて | `database.table`、テーブル名のみ、末尾の `*` のいずれか。 |
| `excludeTables?: string[]` | なし | 形式は同じで、除外が優先します。 |
| `maxReconnectAttempts?: number` | `10` | `0` で再接続しなくなります。 |
| `onMetadataError?: (error: Error) => void` | — | メタデータ接続が失敗したときに呼ばれます。設定しなければ失敗は黙って許容され、カラム名は数値インデックスに落ちます。 |

### ClientConfig

| オプション | 既定値 | 説明 |
| --- | --- | --- |
| `host?: string` | `"127.0.0.1"` | |
| `port?: number` | `3306` | |
| `user?: string` | `"root"` | |
| `password?: string` | `""` | |
| `serverId?: number` | `1` | レプリカ識別子。プロセスごとに一意でなければなりません。[サーバー設定](server-setup.md#レプリカ識別子)を参照してください。 |
| `startGtid?: string` | — | 省略するとサーバーの現在のセットをスナップショットします。`""` なら空のセットから始めます。 |
| `startBinlogFile?: string` | — | `startBinlogPosition` と合わせて、ファイルとオフセットを指定した開始になります。`startGtid` とは併用できません。 |
| `startBinlogPosition?: number` | — | 4 以上。`startBinlogFile` が必要です。 |
| `connectTimeoutS?: number` | `10` | |
| `readTimeoutS?: number` | `30` | ソケット 1 回分の読み取りを制限します。ハンドシェイクでもストリームでも同じです。 |
| `sslMode?: SslMode` | `Preferred` | [TLS と認証](tls-and-authentication.md)を参照してください。 |
| `sslCa?`, `sslCert?`, `sslKey?: string` | — | 証明書のパス。検証モードで `sslCa` が空なら OS のトラストストアを使います。 |
| `allowPublicKeyRetrieval?: boolean` | `false` | 認証を伴わない RSA 鍵の取得を許可します。検証付きの TLS を優先してください。 |
| `maxQueueSize?: number` | `10000` | |
| `maxQueueBytes?: number` | 48 MiB | |
| `maxEventSize?: number` | 32 MiB | `0` は 1 GiB のハードリミットになります。 |

## BinlogClient

接続はコンストラクタが行います。失敗すると、ネイティブのコードを載せた例外を投げます。

| メンバー | 説明 |
| --- | --- |
| `new BinlogClient(config: ClientConfig)` | 接続し、サーバーの設定を検証します。 |
| `start(): void` | binlog のダンプを要求します。 |
| `poll(): Promise<PollResult>` | libuv のスレッドプール上で解決しますが、ネイティブの呼び出しはイベントが届くかストリームが止まるまでブロックします。同時に走らせられる poll は 1 つです。 |
| `pollBatch(maxEvents?: number): Promise<PollResult[]>` | イベントを 1 つ待ってから、すでにキューにあるものをまとめて返します。 |
| `stop(): void` | 別スレッドから呼べます。待機中の `poll()` を解放します。 |
| `disconnect(): void` | 接続を閉じます。 |
| `destroy(): void` | ネイティブのクライアントを解放します。冪等です。 |

読み取り専用プロパティ: `isConnected`、`isStreaming`、`lastError`、`flavor`、`currentGtid`、`checksumEnabled`、`queuedBytes`、`maxQueueBytes`、`maxEventSize`、`crcErrors`。

### PollResult

```typescript
interface PollResult {
  data: Uint8Array | null;
  isHeartbeat: boolean;
  checksumEnabled: boolean;
}
```

エンジンのフレーミングは、`client.checksumEnabled` ではなく結果の `checksumEnabled` から決めてください。`FORMAT_DESCRIPTION_EVENT` がクライアント側のフレーミングを動かしても、直前の設定で読まれたイベントはまだキューに残っています。

## CdcEngine

| メンバー | 説明 |
| --- | --- |
| `new CdcEngine()` | |
| `feed(data: Uint8Array): number` | 消費したバイト数を返します。キューが満杯になると早めに止まります。 |
| `nextEvent(): ChangeEvent \| null` | キューが空なら `null`。 |
| `hasEvents(): boolean` | |
| `getPosition(): { file: string; offset: number \| bigint }` | |
| `reset(): void` | バッファ済みのバイト列と `TABLE_MAP` のレジストリを破棄します。 |
| `setMaxQueueSize(n)`, `setMaxQueueBytes(n)`, `getMaxQueueBytes()` | |
| `setMaxEventSize(n)`, `getMaxEventSize()` | |
| `setChecksumEnabled(enabled)` | |
| `setTrailerPreVerified(v)`, `getTrailerPreVerified()` | CRC32 を上流がすでに検証したと宣言します。この宣言を裏付ける仕組みはありません。 |
| `setIncludeDatabases(list)`, `setIncludeTables(list)`, `setExcludeTables(list)` | |
| `enableMetadata(config: ClientConfig)` | カラム名のためのメタデータ接続を開きます。 |
| `destroy(): void` | ネイティブのエンジンを解放します。冪等です。 |

エンジンはネイティブの状態を持ちます。ガベージコレクタを待たず、`finally` で解放してください。

## エラー

アドオンから返るエラーは、数値の `code` を持つ素の `Error`、`TypeError`、`RangeError` であって、このパッケージが持つクラスのインスタンスではありません。捕捉した値は `isMesError()` で絞り込み、`code` で分岐してください。[エラー](errors.md)を参照してください。

## ロギング

```typescript
setLogCallback(handler: LogHandler | null, level: LogLevel = LogLevel.Warn): void;
```

プロセス全体に効き、ハンドラはネイティブのリーダースレッド上で実行されることがあります。ハンドラ自体がプロセスや Worker を生かし続けることはありません。

ネイティブ側の配送は未処理 256 件で頭打ちになります。JavaScript スレッドが遅れると超過分は失われ、次に届くレコードの前に `event=node_log_queue_overflow dropped=N` が入ります。ハンドラが不要になったら `setLogCallback(null)` を呼んでください。

[ロギング](logging.md)を参照してください。
