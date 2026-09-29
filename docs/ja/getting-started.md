# はじめかた

## インストール

```sh
npm install @libraz/mysql-event-stream
```

```sh
pip install mysql-event-stream
```

Node.js 22 以降、Python 3.11 以降が必要です。Python パッケージはプラットフォームごとの wheel を公開しています。npm パッケージは、オプションのプラットフォーム依存パッケージから選び分けるのではなく、ビルド済みのアドオンを同梱しています。そのため、ビルド対象に含まれないランタイムでは [ソースからのビルド](#ソースからのビルド)が必要です。

C や C++ のプログラムは、代わりにコアライブラリをリンクします。[ソースからのビルド](#ソースからのビルド)と [C API](c-api.md)を参照してください。

## 最初の実行の前に

ソースは GTID 付きの行ベースレプリケーション向けに設定しておく必要があり、アカウントにはレプリケーション権限が要ります。[サーバー設定](server-setup.md)に、設定項目と権限付与、そして既定値を共有する 2 つのプロセスが破ってしまう `serverId` の規則があります。

## 最初のストリーム

`CdcStream` は接続し、反復し、スコープとともに後始末します。

```typescript
import { CdcStream } from "@libraz/mysql-event-stream";

await using stream = new CdcStream({
  host: "127.0.0.1",
  user: "replicator",
  password: "secret",
  serverId: 1001,
  includeDatabases: ["shop"],
});

for await (const event of stream) {
  console.log(event.type, `${event.database}.${event.table}`);
  console.log("before:", event.before);
  console.log("after:", event.after);
}
```

```python
import asyncio

from mysql_event_stream import CdcStream


async def main() -> None:
    async with CdcStream(
        host="127.0.0.1",
        user="replicator",
        password="secret",
        server_id=1001,
        include_databases=["shop"],
    ) as stream:
        async for event in stream:
            print(event.type, f"{event.database}.{event.table}")
            print("before:", event.before)
            print("after:", event.after)


asyncio.run(main())
```

これで、そのデータベースへの `UPDATE` が行の前後両方のイメージを出力します。`event.type` は Node では文字列 `"UPDATE"` ですが、Python では `EventType.UPDATE` という enum のメンバーであって文字列ではありません。`"UPDATE"` と比べるのではなく `==` で比較してください。

```json
{
  "type": "UPDATE",
  "database": "shop",
  "table": "items",
  "before": { "id": 8, "name": "Widget", "value": 42 },
  "after": { "id": 8, "name": "Widget", "value": 100 },
  "timestamp": 1773584164,
  "position": { "file": "mysql-bin.000003", "offset": 3611 },
  "namesResolved": true
}
```

開始位置を指定せずに開いたストリームはサーバーの現在位置から始まるので、接続より前にコミットされた変更は配信されません。前回の停止位置から再開する方法は [チェックポイントと復旧](checkpoints.md)にあります。

キーがカラム名ではなく `"0"`、`"1"`、`"2"` で返ってくる場合は、サーバーがカラムのメタデータを記録しておらず、メタデータ接続も使えなかったということです。実際の名前を得る 2 つの経路は [カラム名](column-names.md)で説明しています。

## 手元にあるバイト列をデコードする

`CdcEngine` は、入手元を問わず binlog のバイト列を受け取りますが、イベント境界から始まっている必要があります。生の binlog ファイルは先頭に 4 バイトのマジックナンバーがあるので、先に読み飛ばしてください。`feed()` は消費したバイト数を返し、キューが埋まると途中で止まるので、ループ側でキューを空にしてから、消費されなかった末尾を渡し直します。

```typescript
import { CdcEngine } from "@libraz/mysql-event-stream";

const engine = new CdcEngine();
try {
  let offset = 0;
  while (offset < chunk.length || engine.hasEvents()) {
    for (let event = engine.nextEvent(); event !== null; event = engine.nextEvent()) {
      console.log(event.type, event.database, event.table);
    }

    if (offset < chunk.length) {
      const consumed = engine.feed(chunk.subarray(offset));
      offset += consumed;

      // 何も消費されず、キューにも何も残っていないなら、末尾は不完全なイベントです。
      // chunk.subarray(offset) を保持して、次のチャンクの先頭に付けてください。
      if (consumed === 0 && !engine.hasEvents()) break;
    }
  }
} finally {
  engine.destroy();
}
```

```python
from mysql_event_stream import CdcEngine

with CdcEngine() as engine:
    offset = 0
    while offset < len(chunk) or engine.has_events():
        while (event := engine.next_event()) is not None:
            print(event.type, event.database, event.table)

        if offset < len(chunk):
            consumed = engine.feed(chunk[offset:])
            offset += consumed

            # 何も消費されず、キューにも何も残っていないなら、末尾は不完全な
            # イベントです。chunk[offset:] を保持して次のチャンクの先頭に付けます。
            if consumed == 0 and not engine.has_events():
                break
```

短く消費されたあとに、オフセット 0 から渡し直してはいけません。エンジンは受け取り済みの不完全なイベントを保持しているので、同じバイト列を再生すると状態が壊れます。

## ソースからのビルド

```sh
git clone https://github.com/libraz/mysql-event-stream.git
cd mysql-event-stream
```

必要なものは、CMake 3.20 以降、C++17 コンパイラ（GCC 9 以降または Clang 10 以降）、OpenSSL と zlib の開発パッケージです。

```sh
# macOS
brew install cmake openssl zlib

# Ubuntu / Debian
sudo apt install cmake build-essential libssl-dev zlib1g-dev pkg-config
```

コアのビルドとテストは `make` で行い、`make install` は `libmes` と `mes.h` を C や C++ のプロジェクトから見つかる場所に置きます。

```sh
make build
make test
sudo make install
```

各バインディングは、そのコアに対してビルドします。

```sh
cd bindings/node && yarn install && yarn build && yarn test
```

```sh
cd bindings/python && rye sync && rye run pytest
```

Python バインディングは [Rye](https://rye.astral.sh/) で管理しています。開発環境の正は `requirements.lock` と `requirements-dev.lock` です。

## 次に

- [変更イベント](change-events.md) — 何が届き、各フィールドが何を意味するか。
- [3 つの表面](introduction.md#3-つの表面) — `CdcStream` が適さない場合。
- [エラー](errors.md) — 再試行する価値のある失敗。
