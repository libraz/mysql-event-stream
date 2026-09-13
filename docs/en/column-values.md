# Column values

Every MySQL column type maps to exactly one type in each binding. The table below is the canonical one from `core/include/mes.h`; a test compares each binding's copy against it, so the three surfaces cannot drift apart.

SQL `NULL` is `null` in Node and `None` in Python.

| MySQL type | Node.js | Python |
| --- | --- | --- |
| TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT, YEAR, BIT, ENUM, SET | `number` or `bigint` | `int` |
| FLOAT, DOUBLE | `number` | `float` |
| CHAR, VARCHAR, TEXT, DECIMAL, DATE, TIME, DATETIME, TIMESTAMP | `string` | `str` |
| BINARY, VARBINARY, BLOB, JSON, GEOMETRY, VECTOR | `Uint8Array` | `bytes` |

## Reading the rows

**Integers.** Node returns a `number`, or a `bigint` when the exact value does not fit a safe integer. Python returns an `int`, which has no such limit.

**Values above `INT64_MAX`.** A `BIGINT UNSIGNED`, `SET` or `BIT` value larger than a signed 64-bit integer arrives as an exact decimal string, because the core has no integer type that holds it.

**ENUM** is the 1-based index into the column's value list, not the label. **SET** is a numeric bitmask, bit *i* counting from the least significant set when the *i*-th member of the definition is present. **BIT** is the integer value of its bits. Recovering the labels needs the column definition, which the binlog does not carry.

**Temporal types and DECIMAL** are formatted as text by the core. Every `TIMESTAMP` variant is a string of decimal Unix epoch seconds carrying as many fractional digits as the column's declared precision — `"1735689600"`, or `"1735689600.123456"` for `TIMESTAMP(6)`.

**JSON** arrives as raw bytes in MySQL's internal binary JSON format. It is not decoded text and not a parsed object; reading it needs a binary-JSON parser.

**VECTOR** (MySQL 9.0 and later) arrives as raw bytes.

## Text and binary

Whether a column is text or bytes follows its charset, not its declared type. A `TEXT` column with a binary collation arrives as bytes, and a `BLOB` with a text collation arrives as a string.

That decision needs the `COLUMN_CHARSET` metadata in the `TABLE_MAP` event. With `binlog_row_metadata=NO_LOG` the metadata is absent, and each text/binary pair shares a single binlog type byte — so both the character and the BLOB families stay bytes, which is the only lossless reading available. `MINIMAL` or `FULL` restores strings for the character families.

## Character sets

Text columns are decoded as UTF-8. Data stored in another character set — latin1, sjis, and the rest — is not transcoded, and such columns may come back lossy.

The two bindings differ in what they do with a byte sequence that is not valid UTF-8:

- **Node** replaces it with U+FFFD. The original bytes are gone.
- **Python** uses the `surrogateescape` handler, so `value.encode("utf-8", errors="surrogateescape")` returns the original bytes. Such a string is not directly JSON-serializable.

To keep the exact bytes of a non-UTF-8 column, declare it with a binary collation so it decodes as bytes.
