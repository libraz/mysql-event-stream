# サーバー設定

クライアントは `connect()` の中でソースの設定を検証し、必要な設定が誤っていればエラーコード 402 で接続を拒否します。正しくデコードできないイベントを流し始めることはありません。

## 対応バージョン

| サーバー | バージョン |
| --- | --- |
| MySQL | 8.4 LTS と 9.x の Innovation リリース |
| MariaDB | 10.11 以降。10.11・11.8・12.3 でテスト |

フレーバーは接続時に検出され、`flavor` / `client.flavor` で参照できます。MariaDB は MySQL といくつかの点で異なり、パーサーはそれを明示的に扱います。[MariaDB](mariadb.md)を参照してください。

## MySQL の設定

次を `my.cnf`（または `my.cnf` が include するファイル）に書き、サーバーを再起動します。

```ini
[mysqld]
log_bin=ON
gtid_mode=ON
binlog_format=ROW
binlog_row_image=FULL
binlog_transaction_compression=OFF
binlog_row_value_options=""
```

いずれも検査の対象です。

- `log_bin=ON` — バイナリログがなければ流すものがありません。
- `gtid_mode=ON` — GTID は、ストリームが再開位置を指す手段です。
- `binlog_format=ROW` — ステートメントベースのロギングは、行ではなくステートメントを記録します。
- `binlog_row_image=FULL` — 部分イメージは変更されたカラムしか持たないため、`before` と `after` が欠けます。
- `binlog_transaction_compression` は `ON` であってはなりません。
- `binlog_row_value_options` に `PARTIAL_JSON` を含めてはなりません。値ではなく JSON の差分が記録されます。

## MariaDB の設定

```ini
[mysqld]
log_bin=ON
binlog_format=ROW
binlog_row_image=FULL
binlog_row_metadata=MINIMAL
log_bin_compress=OFF
```

MariaDB に `gtid_mode` 変数はなく、GTID のロギングは `log_bin` に従うため、MariaDB のソースではこの検査を飛ばします。`log_bin_compress=ON` は、MySQL のトランザクション圧縮と同じ理由で拒否します。MariaDB は `binlog_row_metadata` の既定が `NO_LOG` で、テキストカラムがバイト列として届きます。`MINIMAL`（MySQL の既定）か `FULL` にすると文字列に戻ります。[カラム値](column-values.md#テキストとバイナリ)を参照してください。

## ストリームを止めるイベント

いくつかのイベントは、ソースがコミットしていない変更を公開するか、チェックポイントで表せない欠落を残さない限り、変更イベントにできません。ストリームはこれらを読み飛ばさず、コード 100（`MES_ERR_PARSE`）で止まります。読み飛ばすと、どのコンシューマーにも届いていない変更をチェックポイントが越えてしまうためです。

- `XA_PREPARE` — XA トランザクションの第 1 フェーズです。PREPARE 済みの XA トランザクションは後でロールバックされることがあり、その行を公開すると、コミットされないかもしれない変更を公開することになります。XA トランザクションを使う MySQL ソースはストリームできません。
- `ANONYMOUS_GTID` — GTID なしで記録されたトランザクションで、`gtid_mode` が `ON` でない間に書かれます。GTID のチェックポイントではこれを表せません。
- `EXECUTE_LOAD_QUERY` — ステートメントベースの `LOAD DATA INFILE` です。その行はログに書かれません。
- `INCIDENT` — ソースがイベントを失ったと宣言したものです。
- 圧縮されたトランザクションペイロードと `PARTIAL_UPDATE_ROWS` — これらを生む設定は、上の設定検査ですでに拒否しています。
- デコーダが知らないイベント種別。ソースが `LOG_EVENT_IGNORABLE_F` を付けたものは、フラグが許すとおり読み飛ばします。フラグがなければストリームを止めます。

## カラムのメタデータ

`binlog_row_metadata=FULL` にすると `TABLE_MAP` イベントにカラム名が入り、`before` と `after` をカラム名で引くのに他は何も要りません。これがない場合、名前は `SHOW COLUMNS` を実行する別の接続から取得するので、ストリーム対象のテーブルに `SELECT` が必要です。[カラム名](column-names.md)が、このトレードオフと、2 つ目の経路が古い位置からのリプレイでは安全でない理由を扱っています。

## 権限

```sql
CREATE USER 'replicator'@'%' IDENTIFIED BY 'secret';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'replicator'@'%';
```

メタデータ接続でカラム名を解決する場合は、ストリーム対象のテーブルに `SELECT` を追加します。

```sql
GRANT SELECT ON shop.* TO 'replicator'@'%';
```

## レプリカ識別子

接続は毎回、`serverId` / `server_id` で識別されるレプリカとしてソースに登録します。この値は、そのソースに付くすべてのレプリカ、つまりこのライブラリを使う他のプロセスと、すでに接続している実レプリカの間で一意でなければなりません。

どちらのバインディングも既定値は `1` なので、このオプションを省いたプロセスが 2 つあると衝突します。ソースは古い方の登録を切り、切られた側は再接続してもう片方を追い出し、ストリームは 2 つの間で延々と入れ替わります。プロセスごとに別の値を割り当ててください。

```typescript
const stream = new CdcStream({ host: "mysql.example.com", serverId: 1001 });
```

```python
stream = CdcStream(host="mysql.example.com", server_id=1002)
```

## 設定の確認

検証に失敗した接続は、誤っていた設定とともにコード 402 を報告します。同じ値を手で確認するには次のようにします。

```sql
SHOW VARIABLES WHERE Variable_name IN (
  'log_bin', 'gtid_mode', 'binlog_format', 'binlog_row_image',
  'binlog_transaction_compression', 'binlog_row_value_options',
  'log_bin_compress', 'binlog_row_metadata'
);
```
