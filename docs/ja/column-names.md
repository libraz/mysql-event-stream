# カラム名

binlog の行データに入っているのは値だけで、カラム名は入っていません。`before` と `after` のキーに名前を載せる方法は 2 つあり、保証の強さが違います。

## binlog 自体から取得する

`binlog_row_metadata=FULL` を設定すると、サーバーが `TABLE_MAP` イベントにカラム名を書き込みます。ほかに必要なものはありません。名前は説明対象のデータと一緒に届き、イベントが書かれた時点のスキーマと一致し、追加の接続も開きません。

古い位置からの再生に使うのはこの経路で、そこで正しい結果になるのもこの経路だけです。

## メタデータ接続から取得する

設定していない場合、名前は別の接続から取得します。この接続は、`TABLE_MAP` イベントがテーブルを提示したときに `SHOW COLUMNS` を実行します。接続の開き方は表面ごとに違います。

`CdcStream` はストリーム自身の接続設定からこの接続を開くので、追加の呼び出しは要りません。

`CdcEngine` は開きません。エンジンにバイト列を送り込む側が、接続を明示的に有効化します。

```typescript
const engine = new CdcEngine();
engine.enableMetadata({
  host: "mysql.example.com",
  user: "replicator",
  password: "secret",
  readTimeoutS: 30,
});
```

```python
engine = CdcEngine()
engine.enable_metadata(
    host="mysql.example.com",
    user="replicator",
    password="secret",
    read_timeout_s=30,
)
```

この資格情報には、ストリームするテーブルへの `SELECT` 権限が必要です。`SHOW COLUMNS` は `TABLE_MAP` イベントの処理中に同期的に実行され、`readTimeoutS` / `read_timeout_s` で上限が決まります。この値が `0` のときはライブラリ既定の 30 秒になります。タイムアウトすると、そのイベントの名前だけが未解決で残り、接続は 1 度だけ再試行されます。

## 解決できたかを確認する

`namesResolved` / `names_resolved` は、そのイベントのテーブルのカラム名を 1 つでも解決できなかったときに false になります。このときキーは `"0"`、`"1"`、`"2"` という文字列の数値インデックスです。解決が成功した前提で進めず、イベントごとにこのフラグを確認してください。

```typescript
for await (const event of stream) {
  if (!event.namesResolved) {
    metrics.increment("cdc.unnamed_columns");
    continue;
  }
  await handle(event);
}
```

`CdcStream` はメタデータ接続の失敗を `onMetadataError` / `on_metadata_error` で通知します。このコールバックを設定しないと、失敗は黙って受け流されてキーがインデックスに戻ります。ライブラリ自身が stderr に何かを書くことはありません。

## 読み取るのは現在のスキーマ

メタデータ接続が読むのは現在のサーバーのスキーマで、デコード中の binlog 位置の時点のスキーマではありません。この経路で得た名前が正しいのは、最新位置を追いかけている間だけです。

古い位置から再生するストリームは、その後にコミットされた `ALTER TABLE` を跨ぎます。行は `ALTER` 前のものなのに、`SHOW COLUMNS` が返すのは `ALTER` 後のレイアウトです。再生には `binlog_row_metadata=FULL` を使ってください。名前がイベントと一緒に運ばれます。
