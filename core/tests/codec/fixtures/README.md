# Codec fixtures

Binlog files copied byte for byte from real servers. Each file is one whole
binlog: the 4-byte magic `\xfebin`, the FORMAT_DESCRIPTION_EVENT,
PREVIOUS_GTIDS (MySQL) or GTID_LIST (MariaDB), the workload, and the closing
ROTATE.

| File | Server | `binlog_checksum` |
|---|---|---|
| `mysql84_none_tagged.bin` | `mysql:8.4` (8.4.13) | NONE |
| `mysql84_crc32_tagged.bin` | `mysql:8.4` (8.4.13) | CRC32 |
| `mysql97_none_tagged.bin` | `mysql:9.7` (9.7.4) | NONE |
| `mysql97_crc32_tagged.bin` | `mysql:9.7` (9.7.4) | CRC32 |
| `mariadb118_none.bin` | `mariadb:11.8` (11.8.9) | NONE |
| `mariadb118_crc32.bin` | `mariadb:11.8` (11.8.9) | CRC32 |

Under NONE the FDE still ends with its algorithm byte (0) and a 4-byte CRC;
no other event has a trailer.

## Capturing

```sh
./capture.sh                       # all six
./capture.sh mysql97_crc32_tagged  # selected targets
```

Needs Docker. Each target starts a throwaway container with
`binlog_format=ROW`, `binlog_row_image=FULL`, `binlog_row_metadata=FULL` and
the checksum above (MySQL also `gtid_mode=ON`; MariaDB also
`binlog_annotate_row_events=ON`), runs `FLUSH BINARY LOGS`, the workload, and
`FLUSH BINARY LOGS` again, then copies out the file in between. Server
versions come from the floating image tags, so a re-capture may change
timestamps, UUIDs and versions; tests that pin event counts or values must be
updated with it.

## Workload

- Table `fx.t` with one column of each common type: integer widths, unsigned,
  FLOAT/DOUBLE, DECIMAL(10,2) and DECIMAL(30,10), VARCHAR, BLOB, VARBINARY,
  DATETIME(3), DATE, TIME(6), TIMESTAMP(6), YEAR, ENUM, SET, BIT(10), JSON and a
  nullable INT. MariaDB adds `cz BLOB COMPRESSED`.
- Three inserted rows covering edge values: `-0.00`, signed and unsigned
  extremes, zero and partial-zero dates under `sql_mode=''`, TIME at ±838:59:59,
  a 300-byte BLOB, non-ASCII text, empty strings, NULL.
- One UPDATE (including a negative fractional TIME) and one DELETE.
- MySQL: two transactions with `gtid_next='AUTOMATIC:mytag'` (tagged GTID,
  event type 42), then one untagged.
- MariaDB: two inserts into the COMPRESSED column (one empty); every rows
  event is preceded by ANNOTATE_ROWS.
