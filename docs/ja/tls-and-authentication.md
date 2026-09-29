# TLS と認証

## SSL モード

`sslMode` / `ssl_mode` で 5 つのモードから 1 つを選びます。既定は `preferred`（1）です。

| 値 | モード | 暗号化 | サーバーの認証 |
| --- | --- | --- | --- |
| 0 | `disabled` | なし | なし |
| 1 | `preferred` | サーバーが提供していれば行う | なし |
| 2 | `required` | あり | なし |
| 3 | `verify_ca` | あり | 証明書チェーン |
| 4 | `verify_identity` | あり | 証明書チェーンとホスト名 |

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

`preferred` と `required` は接続を暗号化しますが、サーバーを認証しません。そのため間に割り込んだ機器がセッションを終端し、そこを流れるものをすべて読めます。本番の資格情報には `verify_ca` か `verify_identity` を使います。検証を行うモードで `sslCa` / `ssl_ca` が空の場合は、OS のトラストストアにフォールバックします。

`sslCert` / `ssl_cert` と `sslKey` / `ssl_key` は、サーバーがクライアント証明書を要求するときにそれを渡します。

## 認証プラグイン

ネイティブクライアントは MySQL の `caching_sha2_password` と `mysql_native_password` を実装しています。サーバーの挨拶が名乗るのはサーバー側の既定プラグインであって、必ずしもそのアカウントのものではありません。そのため挨拶がこの 2 つ以外のプラグインを名乗っていても、それだけでは致命的ではありません。クライアントはとりあえず `caching_sha2_password` で応答し、サーバー自身が返す `AuthSwitchRequest` にアカウントの実際のプラグインを名乗らせます。認証が失敗するのは、その機会を経てもなお成立しなかった場合だけです。

`caching_sha2_password` は MySQL 8.4 の既定であり、9.x では唯一の選択肢です。

## フル認証

`caching_sha2_password` は通常、サーバーのパスワードキャッシュに対して認証を完了します。キャッシュが冷えているとき——新しいユーザー、サーバーの再起動、`FLUSH PRIVILEGES` のあと——プラグインはフル認証（*full authentication*）にフォールバックし、サーバーが読み取れる形でパスワードを送ります。これには次のどちらかが必要です。

- `sslMode` / `ssl_mode` が `3`（`verify_ca`）または `4`（`verify_identity`）であること。証明書を検証した TLS セッションの上をパスワードが通ります。
- `allowPublicKeyRetrieval` / `allow_public_key_retrieval` を有効にすること。サーバーを認証していないチャネルの上でフル認証を完了させるためのオプトインです。

このオプトインの中身は TLS が有効かどうかで変わります。`preferred`（1）や `required`（2）のように暗号化はされていても認証されていない場合は、検証済みモードと同じくパスワードをその TLS セッションの上に平文で送ります。TLS がまったく無い場合は、代わりに平文のチャネル越しにサーバーの RSA 公開鍵を取得し、その鍵でパスワードを暗号化します。`preferred` や `required` でこのオプトインを付けずにフル認証へ入ると、2 つの対処法を両方挙げた認証エラーになります。

使うべきは検証済みの TLS です。このオプトインはどちらの形でも、それ自体が認証されていないチャネルないし鍵を信頼するので、間に割り込んだ機器がパスワードを取得できる余地は残ります。盗聴だけなら TLS で防げます。

温まったキャッシュに対してしか認証しない接続は `preferred` でも動き、次にサーバーが再起動したあとで失敗します。知っておく価値があるのは、その再起動のあとではなく前です。
