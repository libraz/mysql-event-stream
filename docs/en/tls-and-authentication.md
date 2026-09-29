# TLS and authentication

## SSL modes

`sslMode` / `ssl_mode` selects one of five modes. The default is `preferred` (1).

| Value | Mode | Encrypted | Server authenticated |
| --- | --- | --- | --- |
| 0 | `disabled` | No | No |
| 1 | `preferred` | If the server offers it | No |
| 2 | `required` | Yes | No |
| 3 | `verify_ca` | Yes | Certificate chain |
| 4 | `verify_identity` | Yes | Certificate chain and hostname |

```typescript
const stream = new CdcStream({
  host: "mysql.example.com",
  user: "replicator",
  password: "secret",
  sslMode: 4,
  sslCa: "/path/to/ca.pem",
});
```

```python
stream = CdcStream(
    host="mysql.example.com",
    user="replicator",
    password="secret",
    ssl_mode=4,
    ssl_ca="/path/to/ca.pem",
)
```

`preferred` and `required` encrypt the connection without authenticating the server, so a machine in the middle can terminate the session and read everything on it. Use `verify_ca` or `verify_identity` for production credentials. An empty `sslCa` / `ssl_ca` in a verification mode falls back to the OS trust store.

`sslCert` / `ssl_cert` and `sslKey` / `ssl_key` supply a client certificate when the server requires one.

## Authentication plugins

The native client implements MySQL's `caching_sha2_password` and `mysql_native_password`. The server's greeting names its default plugin, not necessarily the account's, so a greeting naming a plugin outside that pair is not itself fatal: the client answers with `caching_sha2_password` regardless and lets the server's own `AuthSwitchRequest` name the account's real plugin. Authentication only fails once the server has had that chance and still cannot be satisfied.

`caching_sha2_password` is the default on MySQL 8.4 and the only option on 9.x.

## Full authentication

`caching_sha2_password` normally completes against the server's password cache. When that cache is cold — a fresh user, a server restart, a `FLUSH PRIVILEGES` — the plugin falls back to *full authentication*, which sends the password in a form the server can read. That needs one of two things:

- `sslMode` / `ssl_mode` of `3` (`verify_ca`) or `4` (`verify_identity`), so the password travels over a TLS session whose certificate has been verified; or
- `allowPublicKeyRetrieval` / `allow_public_key_retrieval`, which opts into fetching the server's RSA public key over the current channel and encrypting the password with it.

`preferred` (1) and `required` (2) are not sufficient. They encrypt the channel without authenticating the server, so a machine in the middle could collect the cleartext password. Full authentication under those modes without `allowPublicKeyRetrieval` fails with an authentication error naming both remedies.

Verified TLS is the one to use. Public-key retrieval trusts a key that has not itself been authenticated, which is the same exposure in a different shape.

A connection that only ever authenticates against a warm cache will work under `preferred` and then fail after the next server restart. That is worth knowing before the restart rather than after it.
