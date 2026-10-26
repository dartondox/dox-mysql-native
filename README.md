# dox_mysql_native

Native MySQL and MariaDB client for Dart. It is the default MySQL driver of the Dox ORM.

[![pub package](https://img.shields.io/pub/v/dox_mysql_native.svg)](https://pub.dev/packages/dox_mysql_native)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/dartondox/dox-mysql-native/blob/main/LICENSE)

Part of [Dox](https://dartondox.dev), a web backend framework for Dart, like Laravel.
Source code: [github.com/dartondox/dox-mysql-native](https://github.com/dartondox/dox-mysql-native).

## Install

You usually do not add this package. `dox_orm` depends on it and uses it by default.
Add it only to use the pool without the ORM:

```bash
dart pub add dox_mysql_native
```

To see what is ready and what may still change, read [Status and roadmap](https://dartondox.dev/latest/concepts/status-and-roadmap/).

## How it works

- Rust ([mysql_async](https://docs.rs/mysql_async)) owns the connections and the pool.
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

## Features

- Many queries go to Rust in one call. Many results come back in one message.
- Each connection keeps a prepared statement cache.
- `insertId` and `affectedRows` on each result.
- Transactions: `pool.begin()`, `pool.commit(tx)`, `pool.rollback(tx)`.
- Parameters as a list for `?`, or a map for `@name`.
- Not yet: the `sha256_password` login, and the `timeZone` and `collation` options of the Dart driver.

## Documentation

- [Native MySQL driver](https://dartondox.dev/latest/database/mysql-native/)
- [API reference](https://pub.dev/documentation/dox_mysql_native/latest/)

## License

MIT. See [LICENSE](https://github.com/dartondox/dox-mysql-native/blob/main/LICENSE).
