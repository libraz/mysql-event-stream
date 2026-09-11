# mysql-event-stream

[![CI](https://img.shields.io/github/actions/workflow/status/libraz/mysql-event-stream/ci.yml?branch=main&label=CI)](https://github.com/libraz/mysql-event-stream/actions)
[![Version](https://img.shields.io/github/v/release/libraz/mysql-event-stream?label=version)](https://github.com/libraz/mysql-event-stream/releases)
[![npm](https://img.shields.io/npm/v/@libraz/mysql-event-stream?logo=npm)](https://www.npmjs.com/package/@libraz/mysql-event-stream)
[![PyPI](https://img.shields.io/pypi/v/mysql-event-stream?logo=python)](https://pypi.org/project/mysql-event-stream/)
[![codecov](https://codecov.io/gh/libraz/mysql-event-stream/branch/main/graph/badge.svg)](https://codecov.io/gh/libraz/mysql-event-stream)
[![License](https://img.shields.io/github/license/libraz/mysql-event-stream)](https://github.com/libraz/mysql-event-stream/blob/main/LICENSE)
[![C++17](https://img.shields.io/badge/C%2B%2B-17-blue?logo=c%2B%2B)](https://en.cppreference.com/w/cpp/17)
[![MySQL](https://img.shields.io/badge/MySQL-8.4%2B-blue?logo=mysql)](https://dev.mysql.com/)
[![MariaDB](https://img.shields.io/badge/MariaDB-10.11%2B-003545?logo=mariadb)](https://mariadb.org/)
[![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20macOS-lightgrey)](https://github.com/libraz/mysql-event-stream)

MySQL / MariaDB の binlog レプリケーションイベントをアプリケーション向けのストリーミング API に変換する軽量ライブラリです。

[mygram-db](https://github.com/libraz/mygram-db) のレプリケーション層を独立した CDC (Change Data Capture) エンジンとして切り出したプロジェクトです。

## 概要

mysql-event-stream は MySQL 8.4+ および MariaDB 10.11+ のバイナリログイベントをパースし、行レベルの変更イベント (INSERT / UPDATE / DELETE) を構造化データとして出力します。C ABI のコアライブラリに加え、Node.js と Python のバインディングを提供しており、リアルタイムデータパイプライン、監査ログ、キャッシュ無効化、イベント駆動アーキテクチャの構築に MySQL / MariaDB のどちらからでも利用できます。

## アーキテクチャ

```mermaid
graph TD
    MySQL[MySQL 8.4+ / MariaDB 10.11+ Primary] -->|binlog stream / GTID| Proto

    subgraph mysql-event-stream
        Proto[プロトコル層\nTCP + TLS + MySQL ワイヤープロトコル] --> Core[CDC エンジン\nC ABI: libmes]
        Core -->|N-API| Node[Node.js バインディング]
        Core -->|ctypes| Python[Python バインディング]
    end

    Node --> App[アプリケーション]
    Python --> App
```

## クイックスタート

### Node.js

```typescript
import { CdcEngine } from "@libraz/mysql-event-stream";

const engine = new CdcEngine();

// レプリケーションストリームから受信した binlog バイト列を投入する。
// イベントキューが埋まると feed() は途中で止まるため、キューを空にしてから
// 未消費の残りを投入し直す。
let offset = 0;
while (offset < binlogChunk.length) {
  const consumed = engine.feed(binlogChunk.subarray(offset));
  offset += consumed;

  while (engine.hasEvents()) {
    const event = engine.nextEvent();
    if (event === null) break;
    console.log(event.type, event.database, event.table);
    console.log("before:", event.before);
    console.log("after:", event.after);
  }

  // 1 バイトも消費されず、取り出せるイベントも無い場合は末尾がイベントの
  // 途中。binlogChunk.subarray(offset) を保持し、次のチャンクの先頭に連結する。
  if (consumed === 0) break;
}

engine.destroy();
```

### Python

```python
from mysql_event_stream import CdcEngine

engine = CdcEngine()

# binlog バイト列を投入する。イベントキューが埋まると feed() は途中で止まるため、
# キューを空にしてから未消費の残りを投入し直す。
offset = 0
while offset < len(binlog_chunk):
    consumed = engine.feed(binlog_chunk[offset:])
    offset += consumed

    while engine.has_events():
        event = engine.next_event()
        if event is None:
            break
        print(event.type, event.database, event.table)
        print("before:", event.before)
        print("after:", event.after)

    if consumed == 0:
        # 末尾がイベントの途中。binlog_chunk[offset:] を保持して
        # 次のチャンクの先頭に連結する。
        break

engine.close()
```

### C API

```c
#include "mes.h"

mes_engine_t* engine = mes_create();
size_t offset = 0;
while (offset < len) {
    size_t consumed = 0;
    if (mes_feed(engine, data + offset, len - offset, &consumed) != MES_OK) {
        /* mes_reset() を呼び、デコード済みのイベントを取り出す */
        break;
    }
    offset += consumed;

    const mes_event_t* event;
    while (mes_next_event(engine, &event) == MES_OK) {
        printf("%s.%s: type=%d\n", event->database, event->table, event->type);
    }

    /* 1 バイトも消費されず、取り出せるイベントも無い場合は末尾がイベントの途中。
       data + offset から data + len までを保持し、次のチャンクとともに投入し直す。
       offset 0 からの再投入は不可。 */
    if (consumed == 0) break;
}

mes_destroy(engine);
```

### 出力例

`ChangeEvent` にはイベント種別、データベース/テーブル名、binlog 位置、カラム名をキーとした辞書形式の行データが含まれます:

```
-- INSERT INTO items (name, value) VALUES ('Widget', 42)
{
  "type": "INSERT",
  "database": "mes_test",
  "table": "items",
  "before": null,
  "after": { "id": 8, "name": "Widget", "value": 42 },
  "timestamp": 1773584163,
  "position": { "file": "mysql-bin.000003", "offset": 3265 },
  "namesResolved": true
}

-- UPDATE items SET value = 100 WHERE name = 'Widget'
{
  "type": "UPDATE",
  "database": "mes_test",
  "table": "items",
  "before": { "id": 8, "name": "Widget", "value": 42 },
  "after": { "id": 8, "name": "Widget", "value": 100 },
  "timestamp": 1773584164,
  "position": { "file": "mysql-bin.000003", "offset": 3611 },
  "namesResolved": true
}

-- DELETE FROM items WHERE name = 'Widget'
{
  "type": "DELETE",
  "database": "mes_test",
  "table": "items",
  "before": { "id": 8, "name": "Widget", "value": 100 },
  "after": null,
  "timestamp": 1773584164,
  "position": { "file": "mysql-bin.000003", "offset": 3922 }
}
```

## 特徴

- **軽量** - 外部の MySQL クライアントライブラリに依存せず、バイナリサイズも小さい
- **自己完結パッケージ** - MySQL クライアントライブラリ不要。配布物は OpenSSL と zlib を静的リンク
- **ストリーミング処理** - バイト列の到着に合わせて逐次的にイベントを処理
- **多言語対応** - C/C++、Node.js (N-API)、Python (ctypes) バインディング
- **MySQL 8.4+** - LTS および Innovation リリースに対応
- **MariaDB 10.11+** - MariaDB 向け binlog プロトコル、GTID (`domain-server-seq` 形式)、ANNOTATE_ROWS SQL（`sourceSql` / `source_sql`）、slave capability ネゴシエーションに対応
- **GTID サポート** - GTID ベースのレプリケーションに対応した BinlogClient (MySQL / MariaDB 両形式)
- **行レベルイベント** - INSERT / UPDATE / DELETE の変更前後のカラム値を完全に取得
- **VECTOR 型** - MySQL 9.0+ の VECTOR カラムをネイティブサポート（生バイト列としてデコード）
- **カラム名解決** - `binlog_row_metadata=FULL` または `SELECT` 権限を持つメタデータ接続による自動カラム名解決。メタデータ接続は `CdcStream` が自前で開き、`CdcEngine` では明示的な呼び出しで有効化する
- **辞書形式** - 行データを `Record<string, unknown>` / `dict[str, Any]` で直感的にアクセス
- **SSL/TLS** - MySQL 接続の SSL/TLS 暗号化に対応
- **自動再接続** - 接続断時に jitter 付きリニアバックオフで自動再接続
- **流量制御** - 内部リーダースレッド + 上限付きイベントキュー（デフォルト10,000件）により、アプリ側の処理遅延でストリームが切断されるのを防止
- **テーブルフィルタ** - データベース・テーブル単位で取り込み対象を絞り込み
- **構造化ログ** - コールバック形式の構造化ログ出力 (event=name key=value)
- **安全な停止** - `BinlogClient.stop()` はどのスレッドからでも呼べ、読み取り側・処理側ともに即座にブロック解除。`CdcStream` は所有タスク上の `close()` で停止

## 設定

### レプリカ識別子

接続はいずれもレプリカとしてソースに登録され、その識別に `serverId` / `server_id` が使われます。この値は、同じソースに対する全レプリカ間で一意でなければなりません。本ライブラリを使う他のプロセスや、すでに接続されている実レプリカも含みます。

両バインディングの既定値は `1` です。そのためこのオプションを省略したプロセスが 2 つあると衝突します。ソースは古い方の登録を切断し、切断された側が再接続して相手を追い出すため、ストリームが両者の間で交互に切り替わり続けます。プロセスごとに異なる値を割り当ててください。

```typescript
// Node.js
const stream = new CdcStream({ host: "mysql.example.com", serverId: 1001 });
```

```python
# Python
stream = CdcStream(host="mysql.example.com", server_id=1002)
```

### SSL/TLS

```typescript
// Node.js
const stream = new CdcStream({
  host: "mysql.example.com",
  user: "replicator",
  password: "secret",
  sslMode: 4,  // 0=無効, 1=優先, 2=必須, 3=CA検証, 4=サーバー検証
  sslCa: "/path/to/ca.pem",
});
```

```python
# Python
stream = CdcStream(
    host="mysql.example.com",
    user="replicator",
    password="secret",
    ssl_mode=4,
    ssl_ca="/path/to/ca.pem",
)
```

`preferred` と `required` は暗号化しますが、サーバー証明書を検証しません。本番の認証情報には、CA バンドルまたは OS の信頼ストアとともに `verify_ca` か `verify_identity` を使ってください。

### 認証プラグイン

native client が対応する MySQL 認証プラグインは `caching_sha2_password` と
`mysql_native_password` です。これ以外をサーバーが要求した場合は、暗黙に
フォールバックせず認証エラーになります。

`caching_sha2_password` は MySQL 8.4+ の既定であり、9.x では唯一の選択肢です。
サーバー側のパスワードキャッシュが冷えている場合 — 新規ユーザー、サーバー再起動、
`FLUSH PRIVILEGES` の直後 — このプラグインは *full authentication* に切り替わります。
これを完了するには次のどちらかが必要です。

- `sslMode` / `ssl_mode` を `3`（`verify_ca`）または `4`（`verify_identity`）にし、
  証明書を検証済みの TLS セッション上でパスワードを送る
- `allowPublicKeyRetrieval` / `allow_public_key_retrieval` を有効にし、
  現在のチャネル経由でサーバーの RSA 公開鍵を取得してパスワードを暗号化する

`preferred`（`1`）と `required`（`2`）では**不十分**です。これらは通信を暗号化する
だけでサーバーを認証しないため、MITM が平文パスワードを取得できてしまいます。
これらのモードで `allowPublicKeyRetrieval` を設定せずに full authentication に
入った場合は、上記 2 つの対処を明示した認証エラーになります。公開鍵取得の opt-in は
その鍵自体が未認証であるため、検証付き TLS のほうを推奨します。

### カラム名

`binlog_row_metadata=FULL` であれば `TABLE_MAP` イベントにカラム名が載るため、他に何も必要ありません。設定していない場合は `SHOW COLUMNS` を実行する別接続からカラム名を取得しますが、その接続の開き方はクラスによって異なります。`CdcStream` はストリーム自身の接続設定を使って開くため、追加の呼び出しは不要です。`CdcEngine` は開きません。エンジンにバイト列を投入する側が、明示的に接続を有効化します。

```typescript
// Node.js
const engine = new CdcEngine();
engine.enableMetadata({
  host: "mysql.example.com",
  user: "replicator",
  password: "secret",
  readTimeoutS: 30,
});
```

```python
# Python
engine = CdcEngine()
engine.enable_metadata(
    host="mysql.example.com",
    user="replicator",
    password="secret",
    read_timeout_s=30,
)
```

この認証情報には、ストリーム対象のテーブルへの `SELECT` 権限が必要です。有効化すると `TABLE_MAP` の処理中に `SHOW COLUMNS` が同期的に実行され、その待ち時間は `readTimeoutS` / `read_timeout_s` で制限され、この値が `0` のときはライブラリ既定の 30 秒が適用されます。タイムアウトしたイベントはカラム名が未解決のまま残り、接続は一度だけ再試行されます。解決できたものとして扱わず、イベントごとに `namesResolved` / `names_resolved` を確認してください。

この接続が読むのは、デコード中の binlog 位置におけるスキーマではなく、サーバーの現在のスキーマです。カラム名を信頼できるのはストリームの先頭を追いかけている間だけで、古い位置から再生する場合は `binlog_row_metadata=FULL` を設定し、元の `TABLE_MAP` metadata を保持してください。

### テーブルフィルタリング

```typescript
// Node.js - 特定のテーブルのイベントのみ処理
const stream = new CdcStream({
  host: "mysql.example.com",
  includeDatabases: ["mydb"],
  excludeTables: ["mydb.audit_log"],
});
```

```python
# Python
stream = CdcStream(
    host="mysql.example.com",
    include_databases=["mydb"],
    exclude_tables=["mydb.audit_log"],
)
```

フィルタは大文字小文字を区別します。テーブルフィルタ（`includeTables` / `include_tables` と `excludeTables` / `exclude_tables`）は `database.table` またはテーブル名だけの完全一致に加え、末尾の `*` を prefix ワイルドカードとして使えます（例: `mydb.audit_*`）。それ以外の位置の `*` はリテラルです。一方、データベースフィルタ `includeDatabases` / `include_databases` はデータベース名をバイト単位で完全一致で比較し、ワイルドカードはありません。`shard_*` はその名前のデータベースだけに一致するため、対象のデータベースは列挙してください。include フィルタを設定し、TABLE_MAP を受信したにもかかわらず一件も一致しなければ、reset または stream close 時に log callback へ `include_filter_matched_nothing` WARN が一度だけ配送されます。MySQL の識別子の大文字小文字規則はサーバープラットフォームで異なるため、送信元サーバーが出力する名前を使ってください。

### 流量制御

```typescript
// BinlogClient は内部にリーダースレッドと上限付きイベントキューを持つ。
// デフォルトキューサイズ: 10,000 件。
// キューが満杯になると TCP レベルで自然にサーバー側の送信が抑制される。
const stream = new CdcStream({
  maxQueueSize: 5000,  // キューサイズ（デフォルト: 10000）
});
```

### スレッド安全性

`CdcEngine` は単一のスレッドまたはタスクから使う前提です。同じインスタンスに対して
`feed()`、`nextEvent()`、`reset()`、フィルタや設定メソッドを同時に呼び出さないで
ください。並行して使う場合は、スレッドやタスクごとに別のインスタンスを作るか、
呼び出し側で排他制御してください。

`BinlogClient` / `CdcStream` は内部でリーダースレッドを使います。poll や iteration、
接続・切断などのライフサイクル操作は、1 つのスレッドまたはタスクから行ってください。
待機中の `poll()` を別スレッドから止める場合は `BinlogClient.stop()` を使ってください。
`CdcStream` に `stop` メソッドはありません。停止はストリームを所有するタスク上で
`close()` を呼びます。内部でネイティブの poll を先に中断してから iterator を
後始末します。

### ログ

```c
// C API - 構造化ログコールバック
void my_log(mes_log_level_t level, const char* message, void* userdata) {
    fprintf(stderr, "[%d] %s\n", level, message);
    // 出力: [2] event=mysql_connected host=127.0.0.1 port=3306
}
mes_set_log_callback(my_log, MES_LOG_INFO, NULL);
```

### 自動再接続

```typescript
// Node.js - リニアバックオフ付き自動再接続。N 回目の待ち時間は
// min(N 秒, 10 秒) を基準値とし、そこに 50-100% の jitter を掛ける
// (0.5-1秒, 1-2秒, ... 上限到達後は 5-10秒)。Python バインディングも同じ。
const stream = new CdcStream({
  host: "mysql.example.com",
  user: "replicator",
  maxReconnectAttempts: 10,  // デフォルト: 10, 0 = 無効
});
```

### checkpoint からの再開

checkpoint を渡さずに開始したストリームは、サーバーの現在位置から読み始めます。プロセスが停止していた間の変更はすべて飛ばされます。前回の続きから読むには、ストリームからコミット済みの GTID を取得し、次回の開始位置として渡してください。

```typescript
// Node.js
const stream = new CdcStream({
  host: "mysql.example.com",
  user: "replicator",
  password: "secret",
  startGtid: loadCheckpoint(),  // 省略するとサーバーの現在位置から開始
});

try {
  for await (const event of stream) {
    await handle(event);
    saveCheckpoint(stream.currentGtid);
  }
} finally {
  await stream.close();
}
```

```python
# Python
async def run():
    async with CdcStream(
        host="mysql.example.com",
        user="replicator",
        password="secret",
        start_gtid=load_checkpoint(),  # 省略するとサーバーの現在位置から開始
    ) as stream:
        async for event in stream:
            await handle(event)
            save_checkpoint(stream.current_gtid)
```

`currentGtid` / `current_gtid` は読み取り側が最後にコミットした checkpoint です。ストリームを閉じたスコープを抜けても値は残るため、ループの後で一度だけ保存する形でも構いません。配送は at-least-once です。再接続後に同じイベントがもう一度届くことがあるため、checkpoint の保存は自分の処理が成功した後に行い、その処理は冪等にしてください。

`BinlogClient` にも同じ名前で同じ組み合わせがあります。サーバー側でパージ済みの GTID を要求した場合は、黙って先頭から読み直すのではなくコード 405 で失敗します。スナップショットを取り直す合図として扱ってください。

## エラーコード

ネイティブ側のエラーには安定した数値の `mes_error_t` コードが付きます。Node では
`error.code` に `MesErrorCode` の値が入り、Python でも例外の `.code` が同じ値を返します
(`MesErrorCode` は両方でエクスポートされます)。再試行するかどうかの判断には、
メッセージ文字列ではなくこのコードを使ってください。

| コード | 意味 | 再試行の指針 |
| --- | --- | --- |
| 1–2 | API 引数が不正 | 設定を直す。再試行しない |
| 100–101 | パースまたはチェックサム失敗 | 入力を調べたうえで reset / 再接続 |
| 200–202 | 行デコード失敗 | 同じ入力での再試行は無意味 |
| 301（イベントキュー） | クライアントのイベントキューのバイト数・件数の上限超過 | `maxQueueBytes` / `max_queue_bytes` を引き上げる。同じ入力での再試行は無意味 |
| 301（クエリ結果） | クライアントが実行するサーバークエリ（設定検証、GTID 取得、カラムメタデータ）の結果が 100,000 行または 64 MiB を超過 | 上限はビルド時定数で変更不可。接続も閉じられるため、再接続してから実行し直す |
| 400–401 | 接続または認証の失敗 | 一時的な接続失敗のみ再試行。401 は認証情報を修正 |
| 402 | サーバー設定の検証失敗 | サーバー設定を直す。再試行しない |
| 403–404 | ストリームの切断 | 保存済みの checkpoint から再接続（[checkpoint からの再開](#checkpoint-からの再開)） |
| 405 | 要求した GTID がパージ済み | 復旧点・スナップショットを取り直す。再試行しない |

`MesErrorCode` には、エラーとして呼び出し側に届かない値がさらに 5 つあります。`NoEvent`（300）はネイティブ層がキューの空を報告するための値で、両バインディングは `nextEvent()` / `next_event()` の `null` / `None` に変換します。`Internal`（99）、`Decode`（200）、`DecodeColumn`（201）、`GtidTaggedUnsupported`（406）は ABI の互換性のために残してある値で、現在のコアに発生元はありません。行デコードの失敗は `DecodeRow`（202）として報告されます。

C ABI の `mes_error_string()` は、数値コードに対応する正式な短い説明を返します。

## インストール

### パッケージからインストール

```bash
npm install @libraz/mysql-event-stream
pip install mysql-event-stream
```

Python パッケージはプラットフォーム別の wheel を配布します。npm パッケージは
optional な platform 依存でアドオンを選択する仕組みを持たないため、同梱のアドオンが
実行環境と合わない場合は、以下の手順で Node バインディングをソースからビルドしてください。

### 前提条件

- CMake 3.20+
- C++17 コンパイラ (GCC 9+ または Clang 10+)
- OpenSSL 開発ライブラリ
- zlib 開発ライブラリ
- macOS向けビルド済みパッケージはmacOS 15.0以降（サーバー用途はLinuxを推奨）

```bash
# macOS
brew install cmake openssl zlib

# Ubuntu / Debian
sudo apt install cmake build-essential libssl-dev zlib1g-dev pkg-config

# クローン
git clone https://github.com/libraz/mysql-event-stream.git
cd mysql-event-stream
```

### C++ コア

```bash
make build
make test

# オプション: C/C++ プロジェクトから利用する場合
sudo make install
sudo make uninstall
```

### Node.js バインディング

Node.js 22+ および Yarn が必要です。

```bash
cd bindings/node
yarn install
yarn build
yarn test
```

### Python バインディング

Python 3.11+ が必要です。バインディングは [Rye](https://rye.astral.sh/) で管理しており、
開発環境の依存関係は `requirements.lock` / `requirements-dev.lock` が正になります。

```bash
cd bindings/python
rye sync
rye run pytest
```

## プロジェクト構成

```
mysql-event-stream/
  core/                        # C++ コアライブラリ
    include/mes.h              #   パブリック C ABI ヘッダ
    src/
      protocol/                #   MySQL ワイヤープロトコル (TCP, TLS, 認証, クエリ, binlog)
      client/                  #   BinlogClient, EventQueue, ConnectionValidator
    tests/                     #   ユニットテスト (Google Test)
      e2e/                     #   E2E テスト (Docker MySQL 8.4+)
  bindings/
    node/                      # Node.js バインディング (N-API アドオン)
    python/                    # Python バインディング (ctypes)
  e2e/
    docker/                    # Docker Compose + MySQL 初期化 + SSL 証明書
```

## 経緯

このプロジェクトは、MySQL レプリケーションを活用したインメモリ全文検索エンジン [mygram-db](https://github.com/libraz/mygram-db) から、binlog パースおよびレプリケーション関連のコンポーネントを抽出したものです。mygram-db が完全な検索サーバーであるのに対し、mysql-event-stream は CDC に特化しており、MySQL の変更イベントストリーミングを任意のアプリケーションに組み込むことができます。

## 要件

**MySQL:**
- バージョン: 8.4+ (LTS および Innovation リリース)
- GTID モード有効 (BinlogClient 使用時)
- レプリケーション権限: `REPLICATION SLAVE`, `REPLICATION CLIENT`
- スキーマ由来のカラム名には `binlog_row_metadata=FULL` を設定するか、同じ認証情報に `SELECT` も付与します。メタデータクエリは別接続で実行されます。
- メタデータ接続は binlog の過去時点ではなくサーバーの**現在の**スキーマを読みます。古い checkpoint から再生する場合、カラム名を信頼できるのは `binlog_row_metadata=FULL` を設定し、元の TABLE_MAP metadata を保持しているときだけです。

**MariaDB:**
- バージョン: 10.11+ (10.11 / 11.4 で動作検証済み)
- GTID レプリケーション有効 (行フォーマットの `log_bin`)
- レプリケーション権限: `REPLICATION SLAVE`, `REPLICATION CLIENT`
- スキーマ由来のカラム名には `binlog_row_metadata=FULL` を設定するか、同じ認証情報に `SELECT` も付与します。メタデータクエリは別接続で実行されます。
- メタデータ接続は binlog の過去時点ではなくサーバーの**現在の**スキーマを読みます。古い checkpoint から再生する場合、カラム名を信頼できるのは `binlog_row_metadata=FULL` を設定し、元の TABLE_MAP metadata を保持しているときだけです。
- クライアントはサーバーフレーバーを自動検出し、MariaDB binlog プロトコル（GTID イベント type 162、ANNOTATE_ROWS、`@mariadb_slave_capability`）に切り替えます

### MySQL binlog 設定

接続 validator は次の MySQL 設定を必須とします。`my.cnf`（または include
される設定ファイル）へコピーし、変更後に MySQL を再起動してください。

```ini
[mysqld]
log_bin=ON
gtid_mode=ON
binlog_format=ROW
binlog_row_image=FULL
binlog_transaction_compression=OFF
binlog_row_value_options=""
```

`binlog_row_value_options` に `PARTIAL_JSON` を含めることはできません。MariaDB
では同等の行形式を検査し、`log_bin_compress=ON` を拒否します。

## ライセンス

[Apache License 2.0](LICENSE)

## 作者

- libraz
