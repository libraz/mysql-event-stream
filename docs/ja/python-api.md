# Python API

```sh
pip install mysql-event-stream
```

Python 3.11 以降が必要です。ランタイム依存はなく、型定義付き（`py.typed`）です。パッケージはネイティブライブラリを含むプラットフォーム wheel を配布します。

```python
from mysql_event_stream import (
    BinlogClient,
    BinlogPosition,
    CdcEngine,
    CdcStream,
    ChangeEvent,
    ChecksumError,
    ClientConfig,
    ColumnType,
    ColumnValue,
    DecodeError,
    EventType,
    LogLevel,
    MesConnectionError,
    MesError,
    MesErrorCode,
    ParseError,
    PollResult,
    ServerFlavor,
    SslMode,
    set_log_callback,
)
```

コンストラクタはいずれもキーワード引数だけを受け取ります。

## CdcStream

非同期イテレータであり、非同期コンテキストマネージャでもあります。

```python
async with CdcStream(host="...", server_id=1001) as stream:
    async for event in stream:
        ...
```

| メンバー | 説明 |
| --- | --- |
| `CdcStream(**options)` | 最初の反復時ではなく、ここでオプションを検証します。 |
| `configure(**overrides)` | 反復が始まる前にオプションを差し替えます。始まったあとは例外を送出します。 |
| `await close()` / `await aclose()` | ネイティブの poll を中断し、イテレータを終了させます。冪等です。 |
| `current_gtid: str` | 配信済みでコミットされたチェックポイント。`close()` のあとも残ります。 |

### オプション

| オプション | 既定値 | 説明 |
| --- | --- | --- |
| `host` | `"127.0.0.1"` | |
| `port` | `3306` | |
| `user` | `"root"` | |
| `password` | `""` | |
| `server_id` | `1` | レプリカ識別子。プロセスごとに一意でなければなりません。[サーバー設定](server-setup.md#レプリカ識別子)を参照してください。 |
| `start_gtid` | `None` | `None` ならサーバーの現在のセットをスナップショットします。`""` なら空のセットから始めます。 |
| `start_binlog_file` | `None` | `start_binlog_position` と合わせて、ファイルとオフセットを指定した開始になります。`start_gtid` とは併用できません。 |
| `start_binlog_position` | `0` | 4 から `UINT32_MAX` まで。`start_binlog_file` が必要です。 |
| `connect_timeout_s` | `10` | |
| `read_timeout_s` | `30` | ソケット 1 回分の読み取りを制限します。ハンドシェイクでもストリームでも同じです。 |
| `ssl_mode` | `1`（preferred） | [TLS と認証](tls-and-authentication.md)を参照してください。 |
| `ssl_ca`, `ssl_cert`, `ssl_key` | `""` | 証明書のパス。検証モードで `ssl_ca` が空なら OS のトラストストアを使います。 |
| `allow_public_key_retrieval` | `False` | 認証を伴わない RSA 鍵の取得を許可します。検証付きの TLS を優先してください。 |
| `max_queue_size` | `0`（10,000） | |
| `max_queue_bytes` | 48 MiB | |
| `max_event_size` | 32 MiB | `0` は 1 GiB のハードリミットになります。 |
| `include_databases` | `None` | 完全一致で大文字小文字を区別するデータベース名。 |
| `include_tables` | `None` | `database.table`、テーブル名のみ、末尾の `*` のいずれか。 |
| `exclude_tables` | `None` | 形式は同じで、除外が優先します。 |
| `max_reconnect_attempts` | `10` | `0` で再接続しなくなります。 |
| `on_metadata_error` | `None` | メタデータ接続が失敗したときに呼ばれます。設定しなければ失敗は黙って許容されます。 |
| `lib_path` | `None` | 同梱のものではなく、指定した `libmes` を読み込みます。 |

## BinlogClient

構築しただけでは接続しません。`connect()` を明示的に呼びます。

```python
with BinlogClient(host="mysql.example.com", server_id=1003) as client:
    client.connect()
    client.start()
    result = client.poll()
```

| メンバー | 説明 |
| --- | --- |
| `BinlogClient(*, config=None, **options)` | `config` に渡した `ClientConfig` は、`lib_path` を除いて個別のオプションより優先します。 |
| `connect()` | 接続し、サーバーの設定を検証します。 |
| `start()` | binlog のダンプを要求します。 |
| `poll() -> PollResult` | イベントが届くかストリームが止まるまでブロックします。同時に走らせられる poll は 1 つです。 |
| `poll_batch(max_events=...) -> list[PollResult]` | イベントを 1 つ待ってから、すでにキューにあるものをまとめて返します。 |
| `stop()` | 別スレッドから呼べます。待機中の `poll()` を解放します。 |
| `disconnect()` | 接続を閉じます。 |
| `close()` | ネイティブのクライアントを解放します。冪等です。 |

読み取り専用プロパティ: `is_connected`、`is_streaming`、`current_gtid`、`last_error`、`flavor`、`checksum_enabled`、`queued_bytes`、`max_queue_bytes`、`max_event_size`、`crc_errors`。

`PollResult` は `data: bytes | None`、`is_heartbeat: bool`、`checksum_enabled: bool` を持ちます。エンジンのフレーミングは、`client.checksum_enabled` ではなく結果の `checksum_enabled` から決めてください。

## CdcEngine

```python
with CdcEngine() as engine:
    consumed = engine.feed(chunk)
```

| メンバー | 説明 |
| --- | --- |
| `CdcEngine(lib_path=None)` | |
| `feed(data: bytes \| bytearray) -> int` | 消費したバイト数を返します。キューが満杯になると早めに止まります。 |
| `next_event() -> ChangeEvent \| None` | キューが空なら `None`。 |
| `has_events() -> bool` | |
| `get_position() -> BinlogPosition` | |
| `reset()` | バッファ済みのバイト列と `TABLE_MAP` のレジストリを破棄します。 |
| `set_max_queue_size(n)`, `set_max_queue_bytes(n)`, `get_max_queue_bytes()` | |
| `set_max_event_size(n)`, `get_max_event_size()` | |
| `set_checksum_enabled(enabled)` | |
| `set_trailer_pre_verified(v)`, `get_trailer_pre_verified()` | CRC32 を上流がすでに検証したと宣言します。この宣言を裏付ける仕組みはありません。 |
| `set_include_databases(list)`, `set_include_tables(list)`, `set_exclude_tables(list)` | |
| `enable_metadata(**options)` | カラム名のためのメタデータ接続を開きます。 |
| `close()` | ネイティブのエンジンを解放します。冪等です。 |

エンジンはネイティブの状態を持ちます。ガベージコレクタを待たず、`with` でスコープを区切ってください。

## ChangeEvent

凍結された dataclass で、`type`、`database`、`table`、`before`、`after`、`timestamp`、`position`、`names_resolved`、`source_sql` を持ちます。`before` と `after` は `dict[str, Any] | None` です。[変更イベント](change-events.md)と[カラム値](column-values.md)を参照してください。

## エラー

`MesError` は `RuntimeError` を継承し、`ParseError`、`DecodeError`、`ChecksumError` の基底になります。サーバーに到達できなかった場合は `MesConnectionError` で、これは `ConnectionError`、ひいては `OSError` を継承します。どちらも `code` を持ちます。[エラー](errors.md)を参照してください。

## ロギング

```python
set_log_callback(callback, level=LogLevel.WARN, *, lib_path=None)
```

プロセス全体に効き、コールバックはネイティブのリーダースレッド上で実行されることがあります。中で送出された例外は握り潰されます。[ロギング](logging.md)を参照してください。

## 特定のネイティブライブラリを読み込む

`CdcEngine`、`BinlogClient`、`set_log_callback` の `lib_path` で、使う `libmes` を指定できます。指定しない場合、リゾルバは環境変数 `MES_LIB_PATH`、ソースチェックアウトから import したときは開発用ビルド、パッケージの隣に同梱されたライブラリ、システムのライブラリパスの順に試します。

存在しないファイルを指す `MES_LIB_PATH` は、フォールバックではなくエラーになります。呼び出し側が指定したものと違うライブラリを解決すると、`libmes` の 2 つ目のイメージがプロセスに読み込まれることになり、2 つのイメージは状態を共有しません。
