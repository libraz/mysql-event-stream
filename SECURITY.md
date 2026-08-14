# Security policy

## Reporting a vulnerability

Report privately, not through the public issue tracker:

- **Preferred:** GitHub's private vulnerability reporting, from the repository's
  [Security tab](https://github.com/libraz/mysql-event-stream/security/advisories/new).
- **Alternative:** email `libraz@libraz.net`.

Include the event or server configuration that triggers it, what happened, the
affected version and binding, and a minimal reproduction if you have one. Expect
an acknowledgement within a few days.

## Supported versions

Fixes land on the latest minor of the current major line and on the minor before
it, so a deployment has one minor release of room to upgrade.

| Version | Supported |
|---------|-----------|
| v1.x    | latest minor + previous minor |
| v0.x    | unsupported |

## What is in scope

This library speaks the MySQL replication protocol and decodes binlog events, so
everything arriving over that connection is untrusted input. In scope:

- A binlog event, row image or table map that causes a crash, a hang, unbounded
  memory growth, or a read or write outside an allocation — in the C++ core or
  through the Node and Python bindings.
- Replication credentials or decoded row values appearing in logs, error
  messages or exception text where the caller did not put them.
- Failure to verify TLS when the connection was configured to require it, or any
  path that silently downgrades an encrypted connection.
- A malicious or compromised upstream server driving the client into any of the
  above.

## What is not in scope

- Events decoded to the wrong value. Decoding bugs are correctness bugs; report
  them as normal issues.
- Documented limits behaving as documented — event-size and buffer ceilings
  exist so an upstream server cannot exhaust the client. A ceiling that can be
  bypassed is in scope.
- Vulnerabilities in MySQL itself, or in the schema being replicated.
- Findings that require an attacker to already hold the replication credentials
  the caller configured.
