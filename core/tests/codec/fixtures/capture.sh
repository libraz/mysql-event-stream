#!/usr/bin/env bash
# Captures real binlog files used as codec fixtures.
#
# Each target starts a throwaway server container, runs the same workload and
# copies out the one binlog file that holds it (magic + FDE ... ROTATE).
# Usage: capture.sh [target...]   (default: all targets below)
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"

# name image flavor checksum
TARGETS=(
  "mysql84_none_tagged mysql:8.4 mysql NONE"
  "mysql84_crc32_tagged mysql:8.4 mysql CRC32"
  "mysql97_none_tagged mysql:9.7 mysql NONE"
  "mysql97_crc32_tagged mysql:9.7 mysql CRC32"
  "mariadb118_none mariadb:11.8 mariadb NONE"
  "mariadb118_crc32 mariadb:11.8 mariadb CRC32"
)

COMMON_SQL="
SET SESSION sql_mode='';
CREATE DATABASE fx;
USE fx;
CREATE TABLE t (
  id INT PRIMARY KEY,
  i8 TINYINT, u32 INT UNSIGNED, i64 BIGINT,
  f FLOAT, d DOUBLE, dec1 DECIMAL(10,2), dec2 DECIMAL(30,10),
  s VARCHAR(32), b BLOB, vb VARBINARY(16),
  dt DATETIME(3), d0 DATE, tm TIME(6), ts TIMESTAMP(6) NULL, y YEAR,
  e ENUM('a','b','c'), st SET('x','y','z'), bt BIT(10), j JSON, n INT NULL
  @EXTRA_COLUMNS@
) DEFAULT CHARSET=utf8mb4;
INSERT INTO t (id,i8,u32,i64,f,d,dec1,dec2,s,b,vb,dt,d0,tm,ts,y,e,st,bt,j,n) VALUES
  (1,-128,4294967295,-9223372036854775808,1.5,-2.25,-0.00,-12345678901234567890.0123456789,
   'abc',X'00FF',X'0102','2024-02-29 23:59:59.123','2024-01-31','-12:34:56.789012',
   '2024-01-01 00:00:00.000001',2024,'b','x,z',b'1010101010','{\"k\":[1,2]}',NULL),
  (2,127,0,9223372036854775807,0,0,99999999.99,0,
   '',X'',X'','0000-00-00 00:00:00.000','0000-00-00','838:59:59.000000',
   NULL,0,'a','',b'0','null',7),
  (3,0,1,1,-0.5,1e300,0.01,1,
   'ü日本',REPEAT('z',300),X'FF','2024-00-15 01:02:03.004','2024-00-00','-838:59:59.000000',
   '1970-01-01 00:00:01.000000',1901,'c','x,y,z',b'1111111111','[]',-1);
UPDATE t SET s='changed', n=NULL, tm='-00:00:00.500000' WHERE id=1;
DELETE FROM t WHERE id=2;
"

MYSQL_TAGGED_SQL="
SET gtid_next='AUTOMATIC:mytag';
INSERT INTO fx.t (id,s) VALUES (10,'tagged-1');
SET gtid_next='AUTOMATIC:mytag';
INSERT INTO fx.t (id,s) VALUES (11,'tagged-2');
SET gtid_next='AUTOMATIC';
INSERT INTO fx.t (id,s) VALUES (12,'untagged');
"

capture() {
  local name="$1" image="$2" flavor="$3" checksum="$4"
  local cname="mes_codec_capture_${name}"
  local client args extra_cols extra_sql

  docker rm -f "$cname" >/dev/null 2>&1 || true
  if [[ "$flavor" == mysql ]]; then
    client=mysql
    args=(--server-id=1 --log-bin=binlog --gtid-mode=ON --enforce-gtid-consistency=ON
          --binlog-format=ROW --binlog-row-image=FULL --binlog-row-metadata=FULL
          "--binlog-checksum=${checksum}")
    extra_cols=""
    extra_sql="$MYSQL_TAGGED_SQL"
    docker run -d --name "$cname" -e MYSQL_ALLOW_EMPTY_PASSWORD=1 "$image" "${args[@]}" >/dev/null
  else
    client=mariadb
    args=(--server-id=1 --log-bin=binlog --binlog-format=ROW --binlog-row-image=FULL
          --binlog-row-metadata=FULL --binlog-annotate-row-events=ON
          "--binlog-checksum=${checksum}")
    extra_cols=", cz BLOB COMPRESSED"
    extra_sql="INSERT INTO fx.t (id,cz) VALUES (20, REPEAT('compress me ', 40)), (21, '');"
    docker run -d --name "$cname" -e MARIADB_ALLOW_EMPTY_ROOT_PASSWORD=1 "$image" "${args[@]}" >/dev/null
  fi

  # The image init runs a temporary server with networking off; TCP answers
  # only once the final server is up.
  local tries=0
  until docker exec "$cname" "$client" -uroot -h127.0.0.1 --protocol=TCP -e 'SELECT 1' >/dev/null 2>&1; do
    tries=$((tries + 1))
    if (( tries > 120 )); then
      echo "server in $cname did not come up" >&2
      docker logs "$cname" | tail -20 >&2
      docker rm -f "$cname" >/dev/null
      return 1
    fi
    sleep 1
  done

  local q=("$client" -uroot -h127.0.0.1 --protocol=TCP -N -B)
  docker exec "$cname" "${q[@]}" -e 'FLUSH BINARY LOGS'
  local file
  file="$(docker exec "$cname" "${q[@]}" -e 'SHOW BINARY LOGS' | tail -1 | cut -f1)"
  docker exec -i "$cname" "${q[@]}" <<<"${COMMON_SQL//@EXTRA_COLUMNS@/$extra_cols}${extra_sql}"
  docker exec "$cname" "${q[@]}" -e 'FLUSH BINARY LOGS'
  docker cp "$cname:/var/lib/mysql/$file" "$HERE/$name.bin"
  docker rm -f "$cname" >/dev/null
  echo "$name.bin <- $image ($checksum) $file"
}

selected=("$@")
for t in "${TARGETS[@]}"; do
  read -r name image flavor checksum <<<"$t"
  if (( ${#selected[@]} == 0 )) || [[ " ${selected[*]} " == *" $name "* ]]; then
    capture "$name" "$image" "$flavor" "$checksum"
  fi
done
