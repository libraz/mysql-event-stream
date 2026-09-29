# エラー

あらゆる失敗は、C ABI の `mes_error_t` が定める安定した数値コードを持ちます。分岐にはメッセージ文字列ではなくコードを使います。

## コード範囲

| 範囲 | モジュール |
| --- | --- |
| 0 | 成功 |
| 1–99 | 一般 — 不正な引数、内部エラー |
| 100–199 | パース — ヘッダ、イベント本体、チェックサム |
| 200–299 | デコード — 行データ、カラム値 |
| 300–399 | 状態 — イベントなし、キュー満杯 |
| 400–499 | 接続 — 接続、認証、検証、ストリーム、切断 |

## それぞれへの対処

| コード | 意味 | 再試行の指針 |
| --- | --- | --- |
| 1–2 | API 引数が不正 | 呼び出しを修正します。再試行はしません。 |
| 100–101 | パースまたはチェックサムの失敗 | 入力を調べたうえでのみリセットまたは再接続します。 |
| 200–202 | 行デコードの失敗 | 同じ入力のままでは再試行しません。 |
| 301（イベントキュー） | キュー全体のバイト予算より大きい単一のイベント | `maxQueueBytes` / `max_queue_bytes` を引き上げます。同じ入力のままでは再試行しません。 |
| 301（クエリ結果） | 付随するクエリ（設定の検証、GTID の参照、カラムメタデータ）が 100,000 行または 64 MiB を超えて保持した | 上限はコンパイル時の定数で、この失敗は接続を閉じます。問い合わせ直す前に再接続します。 |
| 400 | 接続の失敗（ハンドシェイクの失敗、サーバーの挨拶の代わりに届いた ERR パケット、資格情報を送る前のトランスポート障害） | 再試行します。多くは一時的です。 |
| 401 | 認証の失敗（送った資格情報に対して、サーバーが ERR パケットを返した） | 資格情報を直します。再試行はしません。 |
| 402 | サーバー設定の検証に失敗（サーバーは応答したが、必須の設定が誤っているか欠けている） | [サーバー設定](server-setup.md)を直します。再試行はしません。同じ検証中に接続が切れた場合は代わりに 403 になり、こちらは再試行できます。 |
| 403–404 | ストリームのトランスポートが終了（`mes_client_connect()` がサーバー設定を検証している最中に接続が切れた場合を含む） | 永続化した[チェックポイント](checkpoints.md)から再接続します。 |
| 405 | サーバーが、要求した GTID がパージ済みであることを確認した | 新しいスナップショット地点を選びます。再試行はしません。サーバーがパージによるものと明言しない「fatal error reading binlog」（古すぎる位置、欠落したログなど）は代わりに 403 になります。 |

`CdcStream` はこの区分を自ら適用します。トランスポートの失敗では再接続し、「再試行しない」側に並ぶものはそのまま呼び出し側へ渡します。

## 呼び出し側に届かないコード

5 つの値は ABI の安定性のためにエクスポートされていますが、エラーとして届くことはありません。

`NoEvent`（300）はネイティブ層がキューの空を伝える手段で、どちらのバインディングも `nextEvent()` / `next_event()` の `null` / `None` に変換します。`Internal`（99）、`Decode`（200）、`DecodeColumn`（201）、`GtidTaggedUnsupported`（406）は現在のコアに生成元がありません。行デコードの失敗は `DecodeRow`（202）として報告されます。

## Node.js

アドオンから来るエラーは、`code` を持たせた素の `Error`、`TypeError`、`RangeError` であって、このパッケージが所有するクラスのインスタンスではありません。`instanceof` で判定できる対象がないので、捕捉した値は `isMesError()` で絞り込みます。

```typescript
import { isMesError, MesErrorCode } from "@libraz/mysql-event-stream";

try {
  await handle(event);
} catch (err) {
  if (isMesError(err) && err.code === MesErrorCode.GtidPurged) {
    await takeFreshSnapshot();
  } else {
    throw err;
  }
}
```

`name` にはカテゴリを表す文字列が入ります。`MesAuthError`、`MesDecodeError`、`MesParseError` のいずれかで、専用のカテゴリを持たないコードでは `MesError` になります。ログ行としては読みやすいものの、分岐に使う値は `code` です。`code` は `mes_error_t` をそのまま写しますが、カテゴリは複数のコードを 1 つの文字列にまとめます。

## Python

`MesError` は `RuntimeError` を継承し、`ParseError`、`DecodeError`、`ChecksumError` の基底になります。サーバーへ到達できなかった失敗は `MesConnectionError` で、これは組み込みの `ConnectionError`、したがって `OSError` を継承し、ほかのソケットクライアントが送出するものと揃います。

2 つの階層は `Exception` より下に共通の基底を持ちません。共有する属性は `code` です。

```python
from mysql_event_stream import MesError, MesConnectionError, MesErrorCode

try:
    await handle(event)
except (MesError, MesConnectionError) as exc:
    if exc.code == MesErrorCode.GTID_PURGED:
        await take_fresh_snapshot()
    else:
        raise
```

範囲外の値や型の違うオプション、未知の設定キー、負のキューサイズ、読み込めなかったライブラリなどは、ネイティブ層に届く前に拒否されます。送出されるのは `TypeError` か `ValueError` なので、`except TypeError` / `except ValueError` はそのまま使えます。`code` には、C ABI が不正な引数に返すのと同じ `MesErrorCode.INVALID_ARG` が入ります。

どちらのバインディングも `MesErrorCode` をエクスポートします。C では `mes_error_string()` が数値コードに対応する正規の短い説明を返します。
