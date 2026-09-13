# C API

公開されている表面は `core/include/mes.h` です。C または C++ のプログラムは `libmes` をリンクし、Node と Python のバインディングも同じヘッダの上に乗っています。

```c
#include "mes.h"
```

## 不変条件

- `mes_engine_t` と `mes_client_t` は**スレッドセーフではありません**。別スレッドから呼べる入口は `mes_client_stop()` だけです。
- `mes_next_event()` が返すイベントポインタは、そのエンジンに対する次の `mes_feed()` / `mes_next_event()` / `mes_reset()` **までの間だけ**有効です。`mes_client_poll()` のデータも次の呼び出しまでです。呼び出しをまたいで保持するものはコピーしてください。
- ライブラリが返す `const char*` は必ず非 NULL です。値が不明なときは `""` になります。

## バージョン

```c
const char* mes_version(void);   /* ライブラリのリリースバージョン */
uint32_t    mes_abi_version(void);
size_t      mes_sizeof_event(void);
size_t      mes_sizeof_column(void);
```

`mes_abi_version()` は ABI の世代を返します。2 つの `sizeof` 関数を使うと、バインディングはコンパイル時に想定した構造体サイズを検証できます。フィールドの追加は古いヘッダでビルドされたバイナリがそのまま動くように配置されていて、その逆を拒むのが ABI バージョンです。

## エラー

```c
typedef enum {
  MES_OK = 0,
  MES_ERR_NULL_ARG = 1,
  MES_ERR_INVALID_ARG = 2,
  MES_ERR_INTERNAL = 99,
  MES_ERR_PARSE = 100,
  MES_ERR_CHECKSUM = 101,
  MES_ERR_DECODE = 200,
  MES_ERR_DECODE_COLUMN = 201,
  MES_ERR_DECODE_ROW = 202,
  MES_ERR_NO_EVENT = 300,
  MES_ERR_QUEUE_FULL = 301,
  MES_ERR_CONNECT = 400,
  MES_ERR_AUTH = 401,
  MES_ERR_VALIDATION = 402,
  MES_ERR_STREAM = 403,
  MES_ERR_DISCONNECTED = 404,
  MES_ERR_GTID_PURGED = 405,
  MES_ERR_GTID_TAGGED_UNSUPPORTED = 406,
} mes_error_t;

const char* mes_error_string(mes_error_t error);
```

[エラー](errors.md)では、リトライの指針と、呼び出し側には届かないコードを説明しています。

## ロギング

```c
typedef enum { MES_LOG_ERROR = 0, MES_LOG_WARN = 1, MES_LOG_INFO = 2, MES_LOG_DEBUG = 3 } mes_log_level_t;

typedef void (*mes_log_callback_t)(mes_log_level_t level, const char* message, void* userdata);

void mes_set_log_callback(mes_log_callback_t callback, mes_log_level_t log_level, void* userdata);
```

コールバックはプロセス全体で 1 つで、リーダースレッド上で実行されることがあります。[ロギング](logging.md)を参照してください。

## イベント

```c
typedef enum { MES_EVENT_INSERT = 0, MES_EVENT_UPDATE = 1, MES_EVENT_DELETE = 2 } mes_event_type_t;
typedef enum { MES_COL_NULL = 0, MES_COL_INT = 1, MES_COL_DOUBLE = 2,
               MES_COL_STRING = 3, MES_COL_BYTES = 4 } mes_col_type_t;

typedef struct {
  mes_col_type_t type;
  int64_t        int_val;    /* type == MES_COL_INT */
  double         double_val; /* type == MES_COL_DOUBLE */
  const char*    str_data;   /* type == MES_COL_STRING または MES_COL_BYTES */
  uint32_t       str_len;
  const char*    col_name;   /* 不明なときは "" */
} mes_column_t;

typedef struct {
  mes_event_type_t   type;
  const char*        database;
  const char*        table;
  const mes_column_t* before_columns;
  uint32_t           before_count;
  const mes_column_t* after_columns;
  uint32_t           after_count;
  uint32_t           timestamp;
  const char*        binlog_file;    /* 最初の ROTATE イベントまでは "" */
  uint64_t           binlog_offset;  /* 次のイベントのオフセット。再開はここから */
  int                names_resolved;
  const char*        source_sql;     /* MariaDB の ANNOTATE_ROWS、なければ "" */
} mes_event_t;
```

`timestamp` は MySQL の 4 バイトのヘッダフィールドをそのまま写したもので、2038 年に溢れます。幅を広げるには ABI のメジャーバンプが必要です。`str_len` はバインディングを単純に保つためこの境界では 32 ビットで、単一イベントの上限が 1 GiB なのでクランプには届きません。

## エンジン

```c
mes_engine_t* mes_create(void);
void          mes_destroy(mes_engine_t* engine);

mes_error_t mes_feed(mes_engine_t* engine, const uint8_t* data, size_t len, size_t* consumed);
mes_error_t mes_next_event(mes_engine_t* engine, const mes_event_t** event);
int         mes_has_events(mes_engine_t* engine);
mes_error_t mes_get_position(mes_engine_t* engine, const char** file, uint64_t* offset);
mes_error_t mes_reset(mes_engine_t* engine);
```

`mes_feed()` はキューが満杯になった時点で早めに止まり、消費したバイト数を報告します。`mes_next_event()` はキューが空のとき `MES_ERR_NO_EVENT` を返します。`mes_reset()` はバッファ済みのバイト列と `TABLE_MAP` のレジストリをまとめて破棄します。

### 上限

```c
mes_error_t mes_set_max_queue_size(mes_engine_t* engine, size_t max_size);
mes_error_t mes_set_max_queue_bytes(mes_engine_t* engine, size_t max_queue_bytes);
size_t      mes_get_max_queue_bytes(mes_engine_t* engine);
mes_error_t mes_set_max_event_size(mes_engine_t* engine, uint32_t max_event_size);
uint32_t    mes_get_max_event_size(mes_engine_t* engine);
```

いずれも `0` を渡すと既定値に戻ります。`MES_DEFAULT_QUEUE_SIZE`（10,000）、`MES_DEFAULT_QUEUE_BYTES`（48 MiB）、そしてイベントサイズは 1 GiB のハードリミットです。[バックプレッシャーと上限](backpressure.md)を参照してください。

### フレーミング

```c
mes_error_t mes_set_checksum_enabled(mes_engine_t* engine, int enabled);
mes_error_t mes_set_trailer_pre_verified(mes_engine_t* engine, int pre_verified);
int         mes_get_trailer_pre_verified(mes_engine_t* engine);
```

`mes_set_checksum_enabled()` には、そのバイト列を返した poll 結果の `checksum_enabled` フィールドを渡します。クライアントの現在の見え方ではありません。`FORMAT_DESCRIPTION_EVENT` がその見え方を動かしても、直前の設定で読まれたイベントはまだキューに残っています。

`mes_set_trailer_pre_verified()` は、各イベントの CRC32 を上流がすでに検証したと宣言し、エンジンが二度目の計算をしないようにします。この宣言を裏付ける仕組みはありません。検証されていないストリームに設定すると、壊れたイベントが黙って受理されます。

### フィルタ

```c
mes_error_t mes_set_include_databases(mes_engine_t* engine, const char** databases, size_t count);
mes_error_t mes_set_include_tables(mes_engine_t* engine, const char** tables, size_t count);
mes_error_t mes_set_exclude_tables(mes_engine_t* engine, const char** tables, size_t count);
```

[テーブルフィルタ](filtering.md)を参照してください。

### カラム名

```c
mes_error_t mes_engine_set_metadata_conn(mes_engine_t* engine, const mes_client_config_t* config);
```

渡された資格情報で 2 本目の接続を開き、`TABLE_MAP` の処理中に `SHOW COLUMNS` でカラム名を解決します。解決できた検索も失敗した検索も 8,192 テーブル分のキャッシュを共有し、溢れたときは全体をまとめて破棄します。[カラム名](column-names.md)を参照してください。

## クライアント

```c
typedef enum { MES_SSL_DISABLED = 0, MES_SSL_PREFERRED = 1, MES_SSL_REQUIRED = 2,
               MES_SSL_VERIFY_CA = 3, MES_SSL_VERIFY_IDENTITY = 4 } mes_ssl_mode_t;
typedef enum { MES_SERVER_FLAVOR_MYSQL = 0, MES_SERVER_FLAVOR_MARIADB = 1 } mes_server_flavor_t;
typedef enum { MES_START_AT_CURRENT = 0, MES_START_AT_GTID = 1,
               MES_START_AT_POSITION = 2 } mes_start_position_mode_t;
```

`mes_client_config_t` は `host`、`port`、`user`、`password`、`server_id`、`start_gtid`、`connect_timeout_s`、`read_timeout_s`、4 つの TLS フィールド、`max_queue_size`、`allow_public_key_retrieval`、`start_position_mode`、`binlog_file`、`binlog_position` を持ちます。ゼロ初期化した config は慣例どおりの既定値、つまり TLS 無効とサーバーの現在位置からの開始を意味します。

```c
mes_client_t* mes_client_create(void);
void          mes_client_destroy(mes_client_t* client);

mes_error_t mes_client_connect(mes_client_t* client, const mes_client_config_t* config);
mes_error_t mes_client_start(mes_client_t* client);
void        mes_client_stop(mes_client_t* client);      /* 別スレッドから呼べる */
void        mes_client_disconnect(mes_client_t* client);
```

`mes_client_connect()` はサーバーの設定を検証し、必須の設定が誤っていれば `MES_ERR_VALIDATION` で失敗します。破棄では停止を要求し、実行中の poll が終わるまで待ちます。

### ポーリング

```c
typedef struct {
  mes_error_t    error;
  const uint8_t* data;            /* 次の poll まで有効。エラー時は NULL */
  size_t         size;
  int            is_heartbeat;
  int            checksum_enabled;
} mes_poll_result_t;

mes_poll_result_t mes_client_poll(mes_client_t* client);
mes_error_t       mes_client_poll_batch(mes_client_t* client, mes_poll_result_t* results,
                                        size_t max_results, size_t* count);
```

`error` と `is_heartbeat` は直交していて、1 つの結果で両方が立つことはありません。ハートビートは健全な無音区間です。ダンプが何も生まなかったことを、サーバーがそう伝えています。`checksum_enabled` に意味があるのは `data` が非 NULL のときだけです。

### イントロスペクション

```c
int                 mes_client_is_connected(mes_client_t* client);
int                 mes_client_is_streaming(mes_client_t* client);
mes_server_flavor_t mes_client_flavor(mes_client_t* client);
const char*         mes_client_last_error(mes_client_t* client);
const char*         mes_client_current_gtid(mes_client_t* client);
int                 mes_client_checksum_enabled(mes_client_t* client);
size_t              mes_client_queued_bytes(mes_client_t* client);
uint64_t            mes_client_crc_errors(mes_client_t* client);

mes_error_t mes_client_set_max_event_size(mes_client_t* client, uint32_t max_event_size);
uint32_t    mes_client_get_max_event_size(mes_client_t* client);
mes_error_t mes_client_set_max_queue_bytes(mes_client_t* client, size_t max_queue_bytes);
size_t      mes_client_get_max_queue_bytes(mes_client_t* client);
```

永続化すべきチェックポイントは `mes_client_current_gtid()` です。[チェックポイントと復旧](checkpoints.md)を参照してください。

## feed ループ

```c
mes_engine_t* engine = mes_create();
size_t offset = 0;
while (offset < len) {
    size_t consumed = 0;
    if (mes_feed(engine, data + offset, len - offset, &consumed) != MES_OK) {
        /* mes_reset() を呼んでから、デコード済みのイベントを吐き出す */
        break;
    }
    offset += consumed;

    const mes_event_t* event;
    while (mes_next_event(engine, &event) == MES_OK) {
        printf("%s.%s type=%d\n", event->database, event->table, event->type);
    }

    /* 何も消費せず、吐き出すものも残っていないなら、末尾は不完全なイベント。
       data + offset から data + len までを保持し、次のチャンクと一緒に
       feed し直す。オフセット 0 から feed し直してはいけない。 */
    if (consumed == 0) break;
}
mes_destroy(engine);
```
