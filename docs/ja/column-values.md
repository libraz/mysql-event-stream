# カラム値

MySQL のカラム型は、どのバインディングでもちょうど 1 つの型に対応します。下の表は `core/include/mes.h` にある正本で、各バインディングが持つ写しをこれと突き合わせるテストがあるため、3 つの表面が食い違うことはありません。

SQL の `NULL` は Node では `null`、Python では `None` です。

| MySQL の型 | Node.js | Python |
| --- | --- | --- |
| TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT, YEAR, BIT, ENUM, SET | `number` または `bigint` | `int` |
| FLOAT, DOUBLE | `number` | `float` |
| CHAR, VARCHAR, TEXT, DECIMAL, DATE, TIME, DATETIME, TIMESTAMP | `string` | `str` |
| BINARY, VARBINARY, BLOB, JSON, GEOMETRY, VECTOR | `Uint8Array` | `bytes` |

## 行の読み方

**整数**は、Node では `number` になり、正確な値が safe integer に収まらないときは `bigint` になります。Python が返す `int` にはそうした上限がありません。

**`INT64_MAX` を超える値**は正確な 10 進文字列として届きます。符号付き 64 ビット整数より大きい `BIGINT UNSIGNED`、`SET`、`BIT` の値がこれに当たり、コアにその値を保持できる整数型がないためです。

**ENUM** はラベルではなく、カラムの値リストに対する 1 始まりのインデックスです。**SET** は数値のビットマスクで、定義の *i* 番目のメンバーが含まれるとき、最下位から数えて *i* 番目のビットが立ちます。**BIT** はそのビット列を整数として表した値です。ラベルを復元するにはカラム定義が必要ですが、binlog はそれを運びません。

**日時型と DECIMAL** は、コアがテキストとして整形します。`TIMESTAMP` 系はどれも 10 進の Unix エポック秒の文字列で、カラムに宣言された精度と同じ桁数の小数部を持ちます。`"1735689600"`、`TIMESTAMP(6)` なら `"1735689600.123456"` です。

**JSON** は MySQL 内部のバイナリ JSON 形式の生バイト列として届きます。デコード済みのテキストでもパース済みのオブジェクトでもなく、読むにはバイナリ JSON のパーサーが必要です。

MySQL 9.0 以降の **VECTOR** は生バイト列として届きます。

## テキストとバイナリ

カラムがテキストになるかバイト列になるかは、宣言された型ではなく charset で決まります。バイナリ照合順序を持つ `TEXT` カラムはバイト列として届き、テキスト照合順序を持つ `BLOB` は文字列として届きます。

この判断には `TABLE_MAP` イベントの `COLUMN_CHARSET` メタデータが必要です。`binlog_row_metadata=NO_LOG` ではメタデータがなく、テキストとバイナリの各組が 1 つの binlog 型バイトを共有するため、文字列系も BLOB 系もバイト列のままになります。これが得られる唯一の無損失な読み方です。`MINIMAL` か `FULL` にすれば、文字列系は文字列に戻ります。

## 文字セット

テキストカラムは UTF-8 としてデコードします。latin1 や sjis など別の文字セットで保存されたデータはトランスコードしないので、そうしたカラムは値が壊れて返ることがあります。

2 つのバインディングは、UTF-8 として妥当でないバイト列の扱いが異なります。

- **Node** は U+FFFD に置き換えます。元のバイト列は失われます。
- **Python** は `surrogateescape` ハンドラを使うので、`value.encode("utf-8", errors="surrogateescape")` で元のバイト列が戻ります。この文字列はそのままでは JSON にシリアライズできません。

UTF-8 以外のカラムのバイト列をそのまま保ちたい場合は、バイナリ照合順序で宣言してバイト列としてデコードさせます。
