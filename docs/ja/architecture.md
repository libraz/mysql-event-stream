# アーキテクチャ

![各レイヤーと、それぞれが受け持つもの](../images/architecture-ja.svg)

## クライアントライブラリを使わない

MySQL のワイヤプロトコルは OpenSSL の上に直接実装しています。ハンドシェイク、capability negotiation、`caching_sha2_password` と `mysql_native_password`、`COM_QUERY`、`COM_BINLOG_DUMP` と `COM_BINLOG_DUMP_GTID` です。クライアントライブラリをリンクすれば、このプロジェクトが存在する理由が失われます。

公開する成果物はすべて OpenSSL と zlib を静的にリンクし、Linux 向けでは GNU C++ ランタイムも静的にリンクします。npm パッケージの linux-x64 アドオンと Linux 向け wheel が依存するのは glibc だけで、macOS 向け wheel が依存するのはシステムライブラリだけです。リリースビルドは出荷前に各バイナリの動的依存を検査し、各成果物には OpenSSL と zlib のライセンス文を同梱しています。

## プロトコル

`core/src/protocol` がソケットを受け持ちます。接続タイムアウトと読み取りタイムアウトを設定できる TCP、OpenSSL による TLS、パケットのフレーミング、認証、そして上位のレイヤーが送る 2 つのコマンド、つまりクエリと binlog dump 要求です。

ソケットの手前には先読みバッファがあります。binlog のパケットは小さく届き、そのサイズではソケットを直接読むより 36 倍速くなります。[パフォーマンス](performance.md)を参照してください。

このレイヤーより上は、ソケットを読んでいることを知りません。

## クライアント

`core/src/client` が接続の生存期間を受け持ちます。`ConnectionValidator` はイベントが流れ始める前にサーバーの設定を検査し、正しくデコードできないソースを拒否します。`BinlogClient` は dump をネゴシエートし、リーダースレッドを動かし、コンシューマーが読み出す上限付きの `EventQueue` にイベントを積みます。[バックプレッシャーと上限](backpressure.md)を参照してください。

クライアントはデコードしません。返すのは 1 イベント分の生のバイト列と、それを読み取ったときのフレーミングです。フレーミングを併せて返すのは、`FORMAT_DESCRIPTION_EVENT` がフレーミングを変えても、前のフレーミングで読まれたイベントがまだキューに残っているからです。

## エンジン

`core/src` がデコードを受け持ちます。複数回の feed にまたがる部分バイト列をバッファし、各ファイルの `FORMAT_DESCRIPTION_EVENT` からチェックサムアルゴリズムを読み取って CRC32 トレーラをそれに従って検証し、後続の row イベントにカラムのレイアウトを与える `TABLE_MAP` レジストリを保持し、row イメージを型付きのカラム値へデコードします。フィルタはここで、捨てるイベントをデコードする前に適用します。忠実な変更イベントにできないイベントは読み飛ばさず、エラーでストリームを止めます。該当するイベントは[サーバーの設定](server-setup.md#ストリームを止めるイベント)に挙げています。

エンジンはネットワークも自前のスレッドも持ちません。渡されたバイト列をデコードするだけであり、だからこそリプレイもオフラインデコードも、ライブのストリーミングと同じコードパスを通ります。

## binlog コーデック

binlog のワイヤフォーマットは、別ライブラリの [libmybinlog](https://github.com/libraz/libmybinlog) がデコードします。ビルド時に CMake の `FetchContent` で取得し、リビジョンは `cmake/libmybinlog.cmake` で固定しています。受け持つ範囲は、イベントのフレーミングとチェックサム、`TABLE_MAP`、`QUERY`、row イメージとその値、MySQL と MariaDB の GTID、全イベント種別の分類、そして `ConnectionValidator` が適用するサーバー設定の規則です。

クライアントとエンジンはこのライブラリを呼び出します。このリポジトリに残るのは、接続、キュー、フィルタ、C ABI、そして `value_render` です。`value_render` はデコード済みの値を `mes.h` が公開する文字列に変換します。ワイヤフォーマットの不具合は libmybinlog 側で直し、クライアントとエンジンの両方にその修正が入ります。

## C ABI

`core/include/mes.h` が公開する表面です。`mes_engine_t`、`mes_client_t`、`mes_event_t`、`mes_error_t` と、それらを扱う関数です。`mes_abi_version()` は ABI の世代を返し、`mes_sizeof_event()` / `mes_sizeof_column()` を使うと、バインディングはコンパイル時に前提とした構造体サイズを確認できます。

バインディングの作者が、明記しなければ取り違える不変条件が 2 つあります。

- `mes_engine_t` と `mes_client_t` はスレッドセーフではありません。ただし `mes_client_t` には 8 つの例外があり、`mes_client_stop()` と、オブザーバ群の `mes_client_is_connected()`、`mes_client_is_streaming()`、`mes_client_checksum_enabled()`、`mes_client_queued_bytes()`、`mes_client_crc_errors()`、`mes_client_last_error()`、`mes_client_current_gtid()` はいずれも別スレッドから呼べます。[C API](c-api.md#不変条件)を参照してください。
- `mes_next_event()` が返すイベントポインタは、次の `mes_feed()`、`mes_next_event()`、`mes_reset()` までしか有効ではなく、`mes_client_poll()` のデータは次の poll までしか有効ではありません。

追加は、古いヘッダでビルドされたバイナリがそのままリンクして動き続けるように配置します。逆向きは ABI バージョンが拒否します。

## バインディング

`bindings/node` は同じ C ABI の上に載る N-API アドオンで、`bindings/python` は `ctypes` で `libmes` を読み込みます。どちらもプロトコルやデコーダを作り直してはいません。

2 つの表面は 1 つの共有された振る舞いの契約に従います。同じクラス名、各言語の命名規則に沿った同じオプションの意味、同じエラーコード、同じライフサイクル規則です。クロスバインディングのテストが両者をこの契約と突き合わせるので、片方の修正がもう片方に入らないまま残ることはありません。

## エラーコード

1 つの数値空間があらゆるレイヤーにまたがり、範囲で区切られています。パースエラーは 100 番台、デコードは 200 番台、状態は 300 番台、接続は 400 番台です。呼び出し側は、どのレイヤーが出したかではなく番号で分岐します。[エラー](errors.md)を参照してください。
