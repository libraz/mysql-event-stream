# ロギング

コアは構造化されたログレコードをコールバック経由で発行します。自分から stderr へ書き出すことはないので、ハンドラを登録していないアプリケーションには何も見えません。

レコードは `key=value` の組の並びで、先頭がイベント名です。

```
event=mysql_connected host=127.0.0.1 port=3306
event=binlog_error type=event_exceeds_queue_byte_budget max_queue_bytes=50331648
```

## レベル

レベルは `ERROR`（0）、`WARN`（1）、`INFO`（2）、`DEBUG`（3）の 4 段階です。ハンドラは受け取りたい最大の詳細度を指定して登録し、それより詳細なものは抑止されます——`WARN` なら `ERROR` と `WARN` だけが届きます。既定は `WARN` です。

## ハンドラの登録

```typescript
import { LogLevel, setLogCallback } from "@libraz/mysql-event-stream";

const levelNames = ["error", "warn", "info", "debug"];

setLogCallback((level, message) => {
  logger.log(levelNames[level] ?? "info", message);
}, LogLevel.Info);

setLogCallback(null); // 解除
```

```python
from mysql_event_stream import LogLevel, set_log_callback

set_log_callback(lambda level, message: logger.info("%s", message), LogLevel.INFO)

set_log_callback(None)  # 解除
```

```c
void my_log(mes_log_level_t level, const char* message, void* userdata) {
    fprintf(stderr, "[%d] %s\n", level, message);
}

mes_set_log_callback(my_log, MES_LOG_INFO, NULL);
```

## ハンドラの中でできること

コールバックはプロセス全体に 1 つで、エンジンごと・クライアントごとではありません。ロードされたライブラリ全体を 1 つのコールバックが受け持つ C ABI に合わせた形です。

C ABI と Python では、このコールバックはネイティブのリーダースレッド上で走ることがあります。中から `stop()`、`close()`、`poll()` をはじめとするクライアントやエンジンの操作を呼び出してはいけません。メッセージを自分のロガーに渡して戻ります。Node はスレッドセーフ関数を介してすべてのレコードを JS のイベントループへ回すため、Node のハンドラは常にそちら側で走り、この制約はありません。ただし、すぐに戻るべきなのは変わりません。

コールバックの中で送出された例外は握り潰されます。ログのハンドラがストリーム処理を中断してはならないので、壊れたハンドラが失うのはイベントではなくログレコードです。

## 見ておきたいレコード

- `include_filter_matched_nothing` — 設定した include フィルタが `TABLE_MAP` イベントを受け取りながら、どれにも一致しませんでした。リセット時またはストリーム終了時に一度だけ発行されます。[テーブルフィルタ](filtering.md)を参照してください。
- `event_exceeds_queue_byte_budget` — `max_queue_bytes` を超える単一のイベントです。その接続は破棄され、poll はコード 301 を報告します。[バックプレッシャーと上限](backpressure.md)を参照してください。

メタデータ接続の失敗はこの経路を通りません。`CdcStream` は `onMetadataError` / `on_metadata_error` で報告します。[カラム名](column-names.md)を参照してください。
