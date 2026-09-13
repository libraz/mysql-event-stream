# サーバー設定

クライアントは `connect()` の中でソースの設定を検証し、必要な設定が誤っていればエラーコード 402 で接続を拒否します。正しくデコードできないイベントを流し始めることはありません。

## 対応バージョン

| サーバー | バージョン |
| --- | --- |
| MySQL | 8.4 LTS と 9.x の Innovation リリース |
| MariaDB | 10.11 以降。10.11 と 11.4 でテスト |

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
log_bin_compress=OFF
```

MariaDB に `gtid_mode` 変数はなく、GTID のロギングは `log_bin` に従うため、MariaDB のソースではこの検査を飛ばします。`log_bin_compress=ON` は、MySQL のトランザクション圧縮と同じ理由で拒否します。

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
