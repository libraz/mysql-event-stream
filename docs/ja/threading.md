# スレッドとライフサイクル

ここに出てくるオブジェクトはすべて単一所有です。同時に使えるのは 1 つのスレッドまたはタスクだけで、ライブラリが並行呼び出しを代わりに直列化することはありません。

## CdcEngine

同じエンジンに対して `feed()`、`nextEvent()`、`reset()`、およびフィルタと設定のセッターを並行して呼び出さないでください。スレッドまたはタスクごとにエンジンを 1 つ用意するか、アクセスを自分で直列化します。

独立したエンジンどうしは何も共有しません。アロケータもロケール状態もロックも共有しないので、デコードはスレッド数に応じてスケールします。このプロジェクトのベンチマーク環境では、8 スレッドで単一スレッドの 4.8〜5.5 倍に達します。[パフォーマンス](performance.md)を参照してください。

エンジンはネイティブの状態を保持します。ガベージコレクタを待たずに解放してください。

```typescript
const engine = new CdcEngine();
try {
  // ...
} finally {
  engine.destroy();
}
```

```python
with CdcEngine() as engine:
    ...
```

`destroy()` と `close()` は冪等です。

## BinlogClient

クライアントは内部にリーダースレッドを持ちます。poll、反復、接続のライフサイクル呼び出しはいずれも単一所有の操作で、同時に実行できる poll は 1 つまでです。

例外は `stop()` だけです。別のスレッドから呼べて、待機中の `poll()` を解除します。キャンセルにはこの経路を使います。poll が止まっている間に別のスレッドからクライアントを閉じたり破棄したりするのは、キャンセルの方法ではありません。

```typescript
process.on("SIGINT", () => client.stop());
```

最終的な破棄は停止を要求し、実行中の poll アクセスが終わるのを待ってからネイティブのクライアントを解放します。そのため、シャットダウンがイベント処理中のリーダーと競合することはありません。

## CdcStream

`CdcStream` に `stop` メソッドはありません。キャンセルは、ストリームを所有するタスクから `close()` を呼んで行います。`close()` はまずネイティブの poll を中断し、その後イテレータを終了させます。

```typescript
await using stream = new CdcStream(config);
for await (const event of stream) {
  await handle(event);
}
```

```python
async with CdcStream(...) as stream:
    async for event in stream:
        await handle(event)
```

`await using` と `async with` はスコープを抜けるときにストリームを閉じます。例外で抜けた場合も閉じます。使わない場合は `finally` で `close()` / `aclose()` を呼んでください。

1 つのストリームで行える反復は 1 回だけです。同じオブジェクトに対する 2 回目の `for await` は、1 本の接続に 2 つの消費側を差し込むのではなく、例外を送出します。

Node では、動作中のストリーム 1 本がブロッキングのネイティブ poll ワーカーを 1 つ占有し、そのワーカーはアイドル中も libuv スレッドプールの枠を握り続けます。`pollBatch()` は最初の結果に続けてキュー上のイベントを最大 64 件まで引き取り、受け渡しの回数を均しますが、枠自体は手放しません。Node の既定の枠は 4 つなので、同時にアイドルになるストリームが 4 本を超える場合は、プロセスの起動前に `UV_THREADPOOL_SIZE` を上げます。

```sh
UV_THREADPOOL_SIZE=16 node app.mjs
```

`configure()` は反復が始まる前ならオプションを上書きし、始まった後は例外を送出します。開始済みのストリームは、その設定からすでにネゴシエートした接続に対して読み続けているためです。

## C ABI では

`mes_engine_t` と `mes_client_t` はスレッドセーフではありません。`mes_client_t` には 8 つの例外があります。`mes_client_stop()` と、オブザーバ群の `mes_client_is_connected()`、`mes_client_is_streaming()`、`mes_client_checksum_enabled()`、`mes_client_queued_bytes()`、`mes_client_crc_errors()`、`mes_client_last_error()`、`mes_client_current_gtid()` は、いずれも別スレッドから呼べます。`mes_client_destroy()` 以外の呼び出しが進行中でもどれが安全かは [C API](c-api.md#不変条件)を参照してください。

`mes_next_event()` が返すイベントのポインタが有効なのは、次の `mes_feed()`、`mes_next_event()`、`mes_reset()` までです。`mes_client_poll()` のデータは次の poll までです。呼び出しより長く保持するものはコピーしてください。[C API](c-api.md)を参照してください。
