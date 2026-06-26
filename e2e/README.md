# E2E Matrix Tests

End-to-end verification of all three bindings against real MySQL and MariaDB
servers in Docker.

| Suite | Driver | DB container (port) |
|-------|--------|---------------------|
| C++ core | `ctest -R E2E` (in `build/`) | shared, `13308` |
| Python | `pytest e2e/tests` (in `bindings/python/`) | shared, `13308` |
| Node.js | `yarn test:e2e` (in `bindings/node/`) | own, `13307` |

The C++ and Python suites share a single database container per target; the
Node.js suite uses its own container, so the two never collide on a port.

## Quick start

```bash
# All suites, all DB versions (mysql 8.4/9.1, mariadb 10.11/11.4)
./e2e/run-matrix.sh
```

Because a full run starts and tears down a container for every target it can
take 15+ minutes — prefer running it in the background and tailing the log:

```bash
nohup ./e2e/run-matrix.sh > /tmp/e2e_matrix.txt 2>&1 &
tail -f /tmp/e2e_matrix.txt
```

## Scoping

```bash
./e2e/run-matrix.sh --only mysql:8.4                  # single target
./e2e/run-matrix.sh --only mysql:8.4,mariadb:11.4     # a couple of targets
./e2e/run-matrix.sh --cpp-only                        # one suite only
./e2e/run-matrix.sh --python-only --only mariadb:11.4 # one suite, one target
./e2e/run-matrix.sh --skip-node                       # all but one suite
./e2e/run-matrix.sh -- -R "Binlog"                    # extra ctest args (C++)
```

Suite flags: `--cpp-only` / `--node-only` / `--python-only` (exclusive) and
`--skip-cpp` / `--skip-node` / `--skip-python` (subtractive).

## Prerequisites

The runner skips a suite (with a warning) if its build artifact is missing,
so build whatever you want covered first:

```bash
# C++ core (also produces the shared library the Python binding loads)
cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build --parallel

# Node.js native addon
cd bindings/node && yarn install && yarn build

# Python package + dev deps (creates the .venv the runner prefers)
cd bindings/python && pip install -e ".[dev]"
```

The Python suite loads `build/core/libmes.{dylib,so}` via `MES_LIB_PATH` and
prefers `bindings/python/.venv/bin/pytest`; it falls back to `pytest` on PATH.

## Output

Each target prints `PASS`/`FAIL` and the run ends with a matrix summary. The
script exits non-zero if any target failed. Containers are torn down with
`down -v` between targets and at the end, so no test database is left running.
