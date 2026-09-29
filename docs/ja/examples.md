# 実例

## 再開できるコンシューマ

ほとんどのアプリケーションが求める形です。イベントを処理してから checkpoint を永続化すれば、クラッシュしたときに飛ばすのではなく再配信されます。

```typescript
import { CdcStream } from "@libraz/mysql-event-stream";

await using stream = new CdcStream({
  host: "mysql.example.com",
  user: "replicator",
  password: process.env.MYSQL_PASSWORD,
  serverId: 1001,
  startGtid: await loadCheckpoint(),
  includeDatabases: ["shop"],
  onMetadataError: (err) => logger.warn({ err }, "column names unavailable"),
});

for await (const event of stream) {
  await handle(event); // 冪等
  await saveCheckpoint(stream.currentGtid);
}
```

```python
import os

from mysql_event_stream import CdcStream


async def run() -> None:
    async with CdcStream(
        host="mysql.example.com",
        user="replicator",
        password=os.environ["MYSQL_PASSWORD"],
        server_id=1001,
        start_gtid=await load_checkpoint(),
        include_databases=["shop"],
        on_metadata_error=lambda err: logger.warning("column names unavailable: %s", err),
    ) as stream:
        async for event in stream:
            await handle(event)  # 冪等
            await save_checkpoint(stream.current_gtid)
```

`handle` はチェックポイントの書き込みより前に実行されるので、イベントは最悪でも 2 回処理されるだけです。[チェックポイントと復旧](checkpoints.md)を参照してください。

## キャッシュの無効化

行が変わると、それをキーにしたキャッシュエントリは古くなります。前後どちらのイメージも手に入るので、キー自体が変わった場合にも対応できます。

```typescript
for await (const event of stream) {
  if (event.table !== "products") continue;

  const keys = new Set<string>();
  if (event.before) keys.add(`product:${event.before.id}`);
  if (event.after) keys.add(`product:${event.after.id}`);

  await cache.del(...keys);
  await saveCheckpoint(stream.currentGtid);
}
```

`DELETE` には `after` がなく、`INSERT` には `before` がなく、行をキーの間で移した `UPDATE` では捨てるエントリが 2 つになります。両方のイメージを読めば、イベント種別で分岐せずに 3 つとも扱えます。

## 必要なテーブルだけに絞る

忙しいサーバーが流すイベントは、1 つのコンシューマに必要な量をはるかに超えます。フィルタは、デコードの前にイベントを捨てます。

```typescript
const stream = new CdcStream({
  host: "mysql.example.com",
  serverId: 1002,
  includeDatabases: ["shop"],
  includeTables: ["shop.orders", "shop.order_items"],
  excludeTables: ["shop.audit_*"],
});
```

ログの `include_filter_matched_nothing` に注意してください。何にも一致しないフィルタは、静かなデータベースとまったく同じに見えます。[テーブルフィルタ](filtering.md)を参照してください。

## クライアントとエンジンを自分で動かす

`CdcStream` はこの 2 つをつないだものです。生のバイト列を途中でキューやファイル、別のプロセスへ渡したいときは、自分でつなぎます。

```typescript
import { BinlogClient, CdcEngine } from "@libraz/mysql-event-stream";

const client = new BinlogClient({ host: "mysql.example.com", serverId: 1003 });
const engine = new CdcEngine();
engine.enableMetadata({ host: "mysql.example.com", user: "replicator", password: secret });

client.start();
try {
  for (;;) {
    const result = await client.poll();
    if (result.isHeartbeat || result.data === null) continue;

    // エンジンのフレーミングは client.checksumEnabled ではなく result から決めます。
    // FORMAT_DESCRIPTION_EVENT はクライアント側のフレーミングを切り替えますが、
    // 前のフレーミングで読んだイベントはまだキューに残っています。
    engine.setChecksumEnabled(result.checksumEnabled);
    engine.feed(result.data);

    for (let e = engine.nextEvent(); e !== null; e = engine.nextEvent()) {
      await handle(e);
    }
  }
} finally {
  client.stop();
  client.destroy();
  engine.destroy();
}
```

サーバーに送るものがなかったとき、`poll()` はハートビートを返します。これはエラーではなく正常な無通信区間で、遅延メトリクスを進めるのに使えます。`stop()` は別のスレッドから呼べて、ブロックしている poll をキャンセルする手段です。[スレッドとライフサイクル](threading.md)を参照してください。

## 別の経路で届いたバイト列をデコードする

エンジンは、バイト列の出どころを問いません。ただしイベント境界から始まっている必要があります。生の binlog ファイルは先頭に 4 バイトのマジックナンバーがあり、これ自体はイベントではないので、最初の `feed()` の前に読み飛ばしてください。

```python
from mysql_event_stream import CdcEngine

with CdcEngine() as engine:
    engine.set_checksum_enabled(True)  # このバイト列が書かれたときのフレーミング

    with open("captured.binlog", "rb") as fh:
        fh.read(4)  # ファイル先頭のマジックナンバー。イベントではない
        pending = b""
        while chunk := fh.read(1 << 20):
            buffer = pending + chunk
            offset = 0
            while offset < len(buffer) or engine.has_events():
                while (event := engine.next_event()) is not None:
                    print(event.type, event.database, event.table)

                if offset < len(buffer):
                    consumed = engine.feed(buffer[offset:])
                    offset += consumed

                    if consumed == 0 and not engine.has_events():
                        break
            pending = buffer[offset:]
```

`feed()` が消費しなかった末尾は不完全なイベントです。保持して、次のチャンクの先頭に付けてください。オフセット 0 から渡し直してはいけません。

トランザクション境界から始まっていないキャプチャには `TABLE_MAP` を欠いた行イベントが含まれ、それらはまったくデコードできません。[変更イベント](change-events.md)を参照してください。

## 構造化ログを自分のロガーへ

```typescript
import { LogLevel, setLogCallback } from "@libraz/mysql-event-stream";

setLogCallback((level, message) => {
  logger.info({ native: message }, "mes");
}, LogLevel.Info);
```

Node はすべてのレコードを JS のイベントループへ回すため、これは常にそちら側で実行されます。それでも、メッセージをロガーへ渡したらすぐ戻ってください。そこからクライアントやエンジンを呼び返してはいけません。[ロギング](logging.md)を参照してください。
