# Glossary

### ABI version

The generation of the C ABI a binary was built against, reported by `mes_abi_version()`. Additions are laid out so a binary built against an older header keeps linking and running; the ABI version is what refuses the reverse, where newer code reads a field an older library never writes.

### ANNOTATE_ROWS

A MariaDB binlog event carrying the SQL statement that produced the row changes following it. It reaches `sourceSql` / `source_sql`. One annotation covers a whole statement, not one row event. See [MariaDB](mariadb.md).

### at-least-once

The delivery guarantee here. An event can be delivered more than once — after a reconnect, the stream resumes from the last committed checkpoint and replays what followed it. Exactly-once is not offered. See [Checkpoints and recovery](checkpoints.md).

### before image / after image

The row as it was and as it is. An `INSERT` has only an after image, a `DELETE` only a before image, an `UPDATE` both. Complete images require `binlog_row_image=FULL` on the source.

### binlog / binary log

The server's log of every change to its data, written for replication and recovery. This library reads it as a replica would.

### binlog position

A binlog filename and a byte offset. The offset on an event is the offset of the *next* event, which is what a resume starts from.

### CDC (Change Data Capture)

Observing a database's changes as a stream of events instead of by polling for differences.

### checkpoint

The position a stream can resume from, published as `currentGtid` / `current_gtid`. Persist it only after processing the event succeeded.

### checksum framing

Whether an event's last four bytes are a CRC32 trailer. A `FORMAT_DESCRIPTION_EVENT` can change it mid-stream, so the framing travels with the event on the poll result rather than being sampled once from the client.

### COM_BINLOG_DUMP_GTID

The protocol command that asks the server to start streaming from a GTID set. MariaDB uses `COM_BINLOG_DUMP` with a GTID position instead.

### FORMAT_DESCRIPTION_EVENT

The event that opens a binlog stream and states how the events after it are framed — header length, checksum algorithm.

### GTID (Global Transaction Identifier)

A server-independent name for a transaction. MySQL spells one `uuid:seq`, MariaDB `domain-server-seq`. A GTID set is what a stream resumes from.

### heartbeat

A poll result carrying no event: the dump produced nothing during the interval, so the server said so. Healthy, and distinct from an error. Useful for advancing a lag metric.

### metadata connection

A second connection that resolves column names through `SHOW COLUMNS` when `binlog_row_metadata=FULL` is not in force. It reads the schema as it is now, which is why it is not safe for replay. See [Column names](column-names.md).

### NULL bitmap

The bits at the front of each row in a `ROWS_EVENT` marking which columns are `NULL`. Only the non-`NULL` values follow, packed end to end, so the bitmap has to be read before any of them can be.

### purged GTID

A transaction the server no longer has, because its binlogs were rotated away. Requesting one fails with code 405 rather than silently restarting from the head.

### replica identity / server id

The number a connection registers with the source under, `serverId` / `server_id`. Unique per process, or two processes displace each other indefinitely. See [Server setup](server-setup.md#replica-identity).

### ROWS_EVENT

The event carrying the actual row data — `WRITE_ROWS`, `UPDATE_ROWS`, `DELETE_ROWS`. It names its table by numeric id and carries no types, so it is undecodable without the `TABLE_MAP` that preceded it.

### server flavour

Whether the source is MySQL or MariaDB, detected at connect time and reported by `flavor`. It selects the GTID format, the dump command, and the checksum variable name.

### TABLE_MAP_EVENT

The event that binds a numeric table id to a database and table name and describes each column's type. Optional metadata — signedness, charsets, column names — is logged only when `binlog_row_metadata` asks for it. See [Change events](change-events.md).

### trailer pre-verification

A declaration that something upstream already verified each event's CRC32, so the engine skips computing it. Nothing checks the promise: set it on an unvalidated stream and a corrupt event is accepted in silence.
