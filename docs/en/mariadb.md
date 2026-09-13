# MariaDB

MariaDB 10.11 and later are supported alongside MySQL, against the same API. The flavour is detected at connect time and reported by `flavor` / `client.flavor`, so no configuration names it.

Four things differ, and each one is handled inside the client.

## GTIDs have a different shape

A MariaDB GTID is `domain-server-seq` — `0-1-4711` — rather than MySQL's `uuid:seq`. Checkpoints published by `currentGtid` / `current_gtid` use that form, and a `startGtid` in that form is sent verbatim, without the widening MySQL's bare-transaction form gets. See [Checkpoints and recovery](checkpoints.md).

Per-transaction GTID events (type 162) only appear once the client advertises `@mariadb_slave_capability = 4`, which it does before requesting the dump. Without that, the server falls back to a legacy replication format that omits them.

## ANNOTATE_ROWS carries the statement

MariaDB can log the statement that produced a set of row changes, as an `ANNOTATE_ROWS` event preceding them. It reaches `sourceSql` / `source_sql` on the event.

Two conditions have to hold. The server must be logging the events (`binlog_annotate_row_events=ON`), and the dump request must ask for them, which every request this client sends does. When either is missing, `sourceSql` is an empty string.

One `ANNOTATE_ROWS` covers a whole statement rather than one row event. A statement whose row data exceeds the server's per-event size limit is split across several consecutive `ROWS` events sharing that single annotation, so the same statement text appears on every row the statement produced.

The statement is held once and shared by every event it annotates. An 8 KB statement attached per row would otherwise dominate what a queued event costs — see [Performance](performance.md).

## Configuration checks differ

MariaDB has no `gtid_mode` variable; GTID logging follows from `log_bin`, so that check is skipped. In its place, `log_bin_compress` must not be `ON`, for the same reason MySQL's `binlog_transaction_compression` must not be. [Server setup](server-setup.md) has both configurations.

## Checksum negotiation uses the older name

MariaDB reads `@master_binlog_checksum` where MySQL 8.4 reads `@source_binlog_checksum`. The client sends the right one for the flavour it detected.

## What stays the same

Everything above the parser. `ChangeEvent` has the same fields, column values map the same way, the filters behave identically, the error codes are the same numbers, and both E2E suites run the same tests against a MariaDB container as against a MySQL one.
