# dox_mysql_native

Native MySQL and MariaDB client for Dart. It is the default MySQL driver of the Dox ORM.

[![pub package](https://img.shields.io/pub/v/dox_mysql_native.svg)](https://pub.dev/packages/dox_mysql_native)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/dartondox/dox-mysql-native/blob/main/LICENSE)

Part of [Dox](https://dartondox.dev), a web backend framework for Dart.
Source code: [github.com/dartondox/dox-mysql-native](https://github.com/dartondox/dox-mysql-native).

## Install

You usually do not add this package. `dox_orm` depends on it and uses it by default.
Add it only to use the pool without the ORM:

```bash
dart pub add dox_mysql_native
```

To see what is ready and what may still change, read [Status and roadmap](https://dartondox.dev/latest/concepts/status-and-roadmap/).

## How it works

- A small MySQL client in Rust owns the connections. It runs on the thread of each isolate, so there is no extra thread in between.
- The pool is for the whole process. Isolates that open the same settings share it, and `maxConnections` counts for all of them. A connection that one isolate does not use goes to an isolate that waits, and busy isolates get a fair share each.
- Many queries go down one connection at once (pipelining). The server answers them in order.
- Your models and queries stay in Dart. You do not change any config.
- Errors are `MysqlException`, with `message`, `code` and `sqlState`. For a server error, `code` is the MySQL error number (eg. 1062 for a duplicate key) and `sqlState` is the 5 character SQLSTATE. For other errors both are null.
- Works with MySQL 8.4 (`caching_sha2_password` login) and MariaDB 11.

## The native library

A build hook (`hook/build.dart`) gets the Rust library. It tries these in order:

1. A path you set in your app `pubspec.yaml` (`hooks: user_defines: dox_mysql_native: lib_path: ...`).
2. A ready-built library from the GitHub release, checked with sha256.
3. A build from source with `cargo`. This needs [Rust](https://rustup.rs).

Ready-built libraries exist for macOS (arm64, x64), Linux (x64, arm64) and Windows (x64).
On these systems you do not need Rust.
There is no Dart driver fallback. Without the native library, the pool does not work.

Use `dart run`, `dart test` or `dart build cli`. `dart compile exe` does not run build hooks.

## Use it

```dart
import 'package:dox_mysql_native/dox_mysql_native.dart';

void main() async {
  MysqlNativePool pool = MysqlNativePool.open(
    host: 'localhost',
    user: 'root',
    password: 'password',
    database: 'app',
  );
  MysqlNativeResult result = await pool.query(
    'SELECT id, name FROM users WHERE id = ?',
    parameters: <Object?>[1],
  );
  print(result.rows); // [{id: 1, name: Ada}]
}
```

## Turn it off

- For all connections: set the env var `DOX_DB_DRIVER=dart`.
- For one connection: `extra: <String, dynamic>{'native': false}` in its `ConnectionConfig`.

## Skip the download

By default the build hook gets the native library for this package. If your app does not use this database, you can turn that off.
In your app `pubspec.yaml`:

```yaml
hooks:
  user_defines:
    dox_mysql_native:
      skip: true
```

Then no library is downloaded or built. Only a real `true` counts.
If the app still opens a pool, it throws an error that says the library is not in the build.

## Features

- Many queries go to Rust in one call. Many results come back in one message.
- Each connection keeps a prepared statement cache (`statementCacheSize`, 32 by default). Over it, the statement used longest ago is closed. The same SQL with other param types (eg. a number, then a string) is a separate statement.
- TLS with rustls (`secure: true`). The login works with `caching_sha2_password` (also without TLS: the password goes RSA encrypted) and `mysql_native_password`.
- A request waits at most 300 s. A transaction that gets no request for 300 s is rolled back.
- `insertId` and `affectedRows` on each result.
- Transactions: `pool.begin()`, `pool.commit(tx)`, `pool.rollback(tx)`.
- Parameters as a list for `?`, or a map for `@name`.
- Not yet: the `sha256_password` login, and the `timeZone` and `collation` options of the Dart driver.

## URL options

`MysqlNativePool.fromUrl()` takes the URL options that mysql_async took, so old URLs still work.

These work like before: `pool_max`, `pool_min`, `stmt_cache_size`, `conn_ttl` (close a connection with no traffic for this many seconds), `wait_timeout` (the same, when `conn_ttl` is not set), `inactive_connection_ttl` (close idle connections over `pool_min`), `abs_conn_ttl` and `abs_conn_ttl_jitter` (close a connection after this age, once it has no query), `reset_connection` (on by default: reset the session when a connection has no query left), `tcp_keepalive` (milliseconds), `socket`, `require_ssl`, `verify_ca`, `verify_identity`, `built_in_roots`, `client_found_rows`, `tcp_nodelay` and `enable_cleartext_plugin`.
`ssl_ca` is new: the path of a PEM file with more CA certificates to trust, eg. a private CA. The Mozilla roots stay trusted, unless `built_in_roots=false`. In `MysqlNativePool.open()` it is `sslRootCert`.
`MysqlNativePool.open()` sets `reset_connection=false`, so the prepared statements stay.

These are accepted and do nothing:

- `compression`: the compressed protocol is not built. Queries work the same, only with more bytes on the network.
- `prefer_socket`: mysql_async switched a local TCP connection to the Unix socket of the server. Use `socket` to pick the socket yourself.
- `ttl_check_interval`: the ttl checks run at the exact time, so no interval is needed.
- `max_allowed_packet`: only a check on the client side. The server still checks its own limit.
- `secure_auth`: the old (pre 4.1) login is never used.

An unknown option is an error.

## Documentation

- [Native MySQL driver](https://dartondox.dev/latest/database/mysql-native/)
- [API reference](https://pub.dev/documentation/dox_mysql_native/latest/)

## License

MIT. See [LICENSE](https://github.com/dartondox/dox-mysql-native/blob/main/LICENSE).
