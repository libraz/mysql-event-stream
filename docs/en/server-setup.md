# Server setup

The client validates the source's configuration during `connect()` and refuses the connection with error code 402 when a required setting is wrong, rather than streaming events that would decode incorrectly.

## Supported versions

| Server | Versions |
| --- | --- |
| MySQL | 8.4 LTS and the 9.x Innovation releases |
| MariaDB | 10.11 and later; tested against 10.11, 11.8 and 12.3 |

The flavour is detected at connect time and reported by `flavor` / `client.flavor`. MariaDB diverges from MySQL in several places the parser handles explicitly — see [MariaDB](mariadb.md).

## MySQL configuration

Put these in `my.cnf` (or the file it includes) and restart the server.

```ini
[mysqld]
log_bin=ON
gtid_mode=ON
binlog_format=ROW
binlog_row_image=FULL
binlog_transaction_compression=OFF
binlog_row_value_options=""
```

Each one is checked:

- `log_bin=ON` — without a binary log there is nothing to stream.
- `gtid_mode=ON` — GTIDs are how a stream names the position it resumes from.
- `binlog_format=ROW` — statement-based logging records the statement, not the rows.
- `binlog_row_image=FULL` — a partial image carries only the changed columns, so `before` and `after` would be incomplete.
- `binlog_transaction_compression` must not be `ON`.
- `binlog_row_value_options` must not contain `PARTIAL_JSON`, which logs a JSON diff instead of the value.

## MariaDB configuration

```ini
[mysqld]
log_bin=ON
binlog_format=ROW
binlog_row_image=FULL
binlog_row_metadata=MINIMAL
log_bin_compress=OFF
```

MariaDB has no `gtid_mode` variable — GTID logging follows from `log_bin` — so that check is skipped for a MariaDB source. `log_bin_compress=ON` is rejected for the same reason MySQL's transaction compression is. MariaDB defaults `binlog_row_metadata` to `NO_LOG`, which delivers text columns as bytes; `MINIMAL` (the MySQL default) or `FULL` restores strings. See [Column values](column-values.md#text-and-binary).

## Events that stop the stream

A few events cannot become change events without publishing a change the source never committed, or without leaving a gap the checkpoint cannot describe. The stream stops at them with code 100 (`MES_ERR_PARSE`) instead of skipping them, because a skipped event lets the checkpoint move past changes no consumer received.

- `XA_PREPARE` — the first phase of an XA transaction. A prepared XA transaction can still roll back, so publishing its rows would publish changes that may never commit. A MySQL source that runs XA transactions cannot be streamed.
- `ANONYMOUS_GTID` — a transaction logged without a GTID, which happens while `gtid_mode` is not `ON`. A GTID checkpoint cannot cover it.
- `EXECUTE_LOAD_QUERY` — a statement-based `LOAD DATA INFILE`. Its rows are never written to the log.
- `INCIDENT` — the source declared that it lost events.
- Compressed transaction payloads and `PARTIAL_UPDATE_ROWS` — the configuration checks above already reject the settings that produce them.
- An event type the decoder does not know. One the source marks with `LOG_EVENT_IGNORABLE_F` is skipped, as the flag permits; without the flag it stops the stream.

## Column metadata

`binlog_row_metadata=FULL` puts column names into the `TABLE_MAP` event, and then nothing else is needed for `before` and `after` to be keyed by name. Without it, names come from a separate connection that runs `SHOW COLUMNS`, which needs `SELECT` on the streamed tables. [Column names](column-names.md) covers the trade-off, and why the second route is not safe for replay from an old position.

## Privileges

```sql
CREATE USER 'replicator'@'%' IDENTIFIED BY 'secret';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'replicator'@'%';
```

Add `SELECT` on the streamed tables when column names are resolved through a metadata connection:

```sql
GRANT SELECT ON shop.* TO 'replicator'@'%';
```

## Replica identity

Every connection registers with the source as a replica, identified by `serverId` / `server_id`. The value has to be unique among all replicas of that source — other processes using this library, and any real replica already attached.

Both bindings default to `1`, so two processes that omit the option collide. The source drops the older registration, the dropped side reconnects and displaces the other, and the stream alternates between them indefinitely. Assign a distinct value per process.

```typescript
const stream = new CdcStream({ host: "mysql.example.com", serverId: 1001 });
```

```python
stream = CdcStream(host="mysql.example.com", server_id=1002)
```

## Verifying the setup

A connection that fails validation reports code 402 with the setting that was wrong. Checking the same values by hand:

```sql
SHOW VARIABLES WHERE Variable_name IN (
  'log_bin', 'gtid_mode', 'binlog_format', 'binlog_row_image',
  'binlog_transaction_compression', 'binlog_row_value_options',
  'log_bin_compress', 'binlog_row_metadata'
);
```
