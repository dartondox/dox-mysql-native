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

### Pool settings

All are optional. The defaults keep the old behavior.

- `requestTimeout` (default 300 s): the longest wait for one request (a free connection, the connect and the query). Over it the request fails, and its connection is closed. A transaction with no request for this long is rolled back.
- `maxConnectionAge` (default none): close a connection older than this, once it has no query.
- `idleTimeout` (default 12 hours): close a connection with no traffic for this long, once it has no query.

- `serverKill` (default true): stop a cancelled or timed out query on the server, with `KILL QUERY` or `KILL` sent on a new connection. URL option `server_kill`.

A connection that runs a query is never closed by these.

The KILL goes on a new connection. That connection goes only to the address of the query's connection. Before the KILL, the driver checks that it reached the same server (`@@server_uuid`, on MariaDB the hostname, port and server_id). If not, no KILL is sent and a warning is printed. Two servers with the same `server_uuid` (eg. a copied data folder), or on MariaDB the same hostname, port and server_id, still look like one server. Behind a proxy that has its own connection ids (eg. ProxySQL), set `serverKill: false`. Then a cancelled query fails at once in Dart but runs to its end on the server, and a timed out query only has its socket closed. The KILL connection also counts for `max_user_connections`. A KILL that fails (eg. no free connection) prints a warning on stderr.

### Cancel a query

```dart
MysqlCancelToken token = MysqlCancelToken();
Future<MysqlNativeResult> slow = pool.query('SELECT SLEEP(10)', cancelToken: token);
token.cancel(); // slow fails with MysqlQueryCancelledException
```

- `query`, `execute` and `stream` take `cancelToken`.
- Not started yet: it does not run. Running: the driver sends `KILL QUERY` on a new connection, so the server stops it (see `serverKill` above). Ended already: nothing changes.
- In a transaction, the next queries wait until the KILL has its answer, so it can only stop the cancelled query.
- When more queries wait on the same connection, the cancelled one runs to its end instead, and its rows are dropped. A late `KILL QUERY` could stop the next query, so it is not sent then.
- A connection that got a `KILL QUERY` takes no new query, and closes once it has none. Inside a transaction it stays, and the transaction goes on.

### Stream rows

```dart
await for (MysqlNativeResult chunk in pool.stream('SELECT * FROM logs', chunkSize: 1000)) {
  print(chunk.rows.length); // up to 1000
}
```

- Each event is a chunk of up to `chunkSize` rows, with the same columns and rows as `query`.
- Rows are read from the server as they come. Rust reads at most 2 chunks ahead of your listener. A paused listener stops the reading, so a big result does not fill the memory.
- Cancel the subscription (eg. `break` in `await for`) to stop the query. It is stopped with `KILL QUERY`, the rows that are still on the way are dropped, and the connection is closed.
- Outside a transaction, a stream gets a connection of its own. Pass `transaction:` to stream inside a transaction.
- The server stops a connection whose client does not read for `net_write_timeout` (60 s by default). Do not pause a stream for longer.

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
- A request waits at most `requestTimeout` (300 s by default). A transaction that gets no request for that long is rolled back.
- `insertId` and `affectedRows` on each result.
- Transactions: `pool.begin()`, `pool.commit(tx)`, `pool.rollback(tx)`.
- Parameters as a list for `?`, or a map for `@name`.
- Not yet: the `sha256_password` login, and the `timeZone` and `collation` options of the Dart driver.

## URL options

`MysqlNativePool.fromUrl()` takes the URL options that mysql_async took, so old URLs still work.

These work like before: `pool_max`, `pool_min`, `stmt_cache_size`, `conn_ttl` (close a connection with no traffic for this many seconds), `wait_timeout` (the same, when `conn_ttl` is not set), `inactive_connection_ttl` (close idle connections over `pool_min`), `abs_conn_ttl` and `abs_conn_ttl_jitter` (close a connection after this age, once it has no query), `reset_connection` (on by default: reset the session when a connection has no query left), `tcp_keepalive` (milliseconds), `socket`, `require_ssl`, `verify_ca`, `verify_identity`, `built_in_roots`, `client_found_rows`, `tcp_nodelay` and `enable_cleartext_plugin`.
`server_kill` is new: `serverKill` (true by default).
`request_timeout_ms` is new: `requestTimeout` in milliseconds (300000 by default).
`ssl_ca` is new: the path of a PEM file with more CA certificates to trust, eg. a private CA. The Mozilla roots stay trusted, unless `built_in_roots=false`. In `MysqlNativePool.open()` it is `sslRootCert`.
`MysqlNativePool.open()` sets `reset_connection=false`, so the prepared statements stay.

These are accepted and do nothing:

- `compression`: the compressed protocol is not built. Queries work the same, only with more bytes on the network.
- `prefer_socket`: mysql_async switched a local TCP connection to the Unix socket of the server. Use `socket` to pick the socket yourself.
- `ttl_check_interval`: the ttl checks run at the exact time, so no interval is needed.
- `max_allowed_packet`: only a check on the client side. The server still checks its own limit.
- `secure_auth`: the old (pre 4.1) login is never used.

An unknown option is an error.

## Fuzzing

The code that reads what the server sends has fuzz targets in `native/fuzz/`. CI runs each one for 60 seconds when `native/` changes.

- `packets`: splits bytes into packets, also when they come in small pieces, and joins big packets. Checks: the pieces give the same packets (or the same error for too many parts) as the whole.
- `rows`: column definitions, then rows in the text and the binary format. Checks: no panic, a bounded result.
- `handshake`: the greeting, auth switch, OK, ERR and end packets, and the length-encoded numbers and strings. Checks: reads stay inside the bytes.
- `conn`: a whole connection (login, then answers to commands) fed with bytes of a broken server, all at once and in random pieces. Streams get more credit when they pause, and the server may close the socket at the end. Checks: no panic, the same result both ways, bounded memory, each command gets one answer, and every record for Dart can be read the way `lib/src/pool.dart` reads it.
- `roundtrip`: what we write we read back (length numbers, packets with the split and join, text and binary rows built like a server does). Checks: the same value comes back, and a cut row is never read as a whole.

To run one locally:

```sh
rustup toolchain install nightly
cargo install cargo-fuzz --locked
cd native
cp Cargo.lock fuzz/Cargo.lock
mkdir -p /tmp/corpus/conn
cargo +nightly fuzz run conn /tmp/corpus/conn fuzz/corpus/conn -- -max_total_time=300 -max_len=8192 -timeout=10
```

Use the name of the other targets in the same way (`packets`, `rows`, `handshake`, `roundtrip`).

- The first folder gets the new inputs the fuzzer finds. Use a temp folder, so `fuzz/corpus/` keeps only the small seed files. Commit a new seed there only when it is useful.
- In fuzz builds only, a packet of 1024 bytes counts as a full one (the real size is 0xFFFFFF). So the code that joins packets is reached with small inputs (a few KB). The release library is not changed.
- A crash stops the run. The input that caused it is in `fuzz/artifacts/<target>/`. Run it again with `cargo +nightly fuzz run <target> fuzz/artifacts/<target>/<file>`.
- The fuzz crate builds the same source as `native/`. Keep the dependencies in `native/fuzz/Cargo.toml` the same as in `native/Cargo.toml`. CI checks this.

## Documentation

- [Native MySQL driver](https://dartondox.dev/latest/database/mysql-native/)
- [API reference](https://pub.dev/documentation/dox_mysql_native/latest/)

## License

MIT. See [LICENSE](https://github.com/dartondox/dox-mysql-native/blob/main/LICENSE).
