# Table filtering

Filters drop events before they are decoded, so a stream watching one table out of a busy server does not pay for the rest.

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

The same three setters exist on `CdcEngine` — `setIncludeDatabases()` / `set_include_databases()` and their table counterparts — for a caller feeding bytes by hand.

## What each filter accepts

**`includeDatabases` / `include_databases`** compares the database name byte for byte and has no wildcard form. `shard_*` matches a database called exactly that and nothing else, so list every database instead. Empty or omitted means every database.

**`includeTables` / `include_tables`** and **`excludeTables` / `exclude_tables`** take a qualified `database.table`, a bare table name, or a trailing `*` as a prefix wildcard — `shop.audit_*`, `orders_*`. A `*` anywhere other than the end is a literal asterisk.

An exclude wins over an include.

## Case sensitivity

Every comparison is case-sensitive. MySQL's own identifier case rules differ by server platform, so use the names the source server emits rather than the names in your schema file.

## When nothing matches

Configured include filters that see `TABLE_MAP` events and match none of them produce one `include_filter_matched_nothing` warning through the [log callback](logging.md), at reset or at stream close. A filter that silently matches nothing looks exactly like a quiet database, and this is what separates the two.
