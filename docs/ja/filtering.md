# テーブルフィルタ

フィルタはデコードの前にイベントを捨てます。混み合ったサーバーの中で 1 つのテーブルだけを見るストリームは、残りのコストを払いません。

```typescript
const stream = new CdcStream({
  host: "mysql.example.com",
  includeDatabases: ["shop"],
  includeTables: ["shop.orders", "shop.order_items"],
  excludeTables: ["shop.audit_*"],
});
```

```python
stream = CdcStream(
    host="mysql.example.com",
    include_databases=["shop"],
    include_tables=["shop.orders", "shop.order_items"],
    exclude_tables=["shop.audit_*"],
)
```

同じ 3 つのセッターは `CdcEngine` にもあります。`setIncludeDatabases()` / `set_include_databases()` とテーブル側の対応物で、バイト列を自分で送り込む側が使います。

## 各フィルタが受け付けるもの

**`includeDatabases` / `include_databases`** はデータベース名をバイト単位で比較し、ワイルドカードの形はありません。`shard_*` はその名前そのもののデータベースにだけ一致するので、対象のデータベースは 1 つずつ列挙してください。空または省略はすべてのデータベースを意味します。

**`includeTables` / `include_tables`** と **`excludeTables` / `exclude_tables`** は、`database.table` の修飾名、テーブル名だけ、末尾に `*` を置いた前方一致のいずれかを受け付けます（`shop.audit_*`、`orders_*`）。末尾以外の `*` はアスタリスクそのものとして扱われます。

データベース名やテーブル名自体に `.` が含まれていると、この方式では一意に区別できません。データベース `x` のテーブル `y.z` と、データベース `x.y` のテーブル `z` は、どちらも修飾形が同じ `x.y.z` になるため、一方を指定したエントリはもう一方にも一致します。

exclude は include より優先されます。

## 大文字小文字の区別

比較はすべて大文字と小文字を区別します。MySQL 自身の識別子の扱いはサーバーのプラットフォームによって違うので、スキーマファイルに書いた名前ではなく、取得元のサーバーが実際に出す名前を指定してください。

## 何にも一致しないとき

include フィルタを設定していて、`TABLE_MAP` イベントを受け取りながら 1 つも一致しなかった場合、接続の区切りごとに [ログコールバック](logging.md) 経由で `include_filter_matched_nothing` の警告が出ます。リセット時（再接続でも起きます）に 1 回、ストリームの終了時にもう 1 回です。何にも一致しないフィルタは、変更の少ないデータベースとまったく同じに見えます。この警告がその 2 つを分けます。
