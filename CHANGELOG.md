## Unreleased

- Fix: on MySQL 8.4 a text param could be stored with the wrong type after a table was changed. Example: a column was `int`, the table was dropped and made again with a `varchar` column, then `'007'` was inserted with the same SQL. It was stored as `'7'`. MySQL keeps the param types of a prepared statement when the table changes under it. Now a `CREATE`, `ALTER`, `DROP` or `RENAME` sent on the pool makes every connection prepare its statements again. Normal queries do no extra round trip. A schema change from another process, or with a comment before the keyword, is not seen. MariaDB was not affected.
- Fixed wrong rows on MySQL 8.4 when the same SQL ran with a number param first and a text param later. MySQL keeps the param types of the first run of a prepared statement, so the text was compared as a number (`'zzz'` became 0 and matched `'taken@dox.dev'`). Now the statement cache key is the SQL plus the param types, so each set of types gets its own prepared statement. MariaDB was not affected.
- Opening a pool is no longer a leaf FFI call. It looks up the host and connects, which can be slow. A leaf call blocks the safepoint of the isolate, so a GC in the other isolates had to wait.
- `sslRootCert` (URL option `ssl_ca`) without `secure` (`require_ssl=true`) still does nothing, but now prints one warning on stderr that it is ignored because TLS is off.
- A pool that can not open now says why. The `ArgumentError` still starts with `dox_mysql_native: bad connection config`, then the reason (eg. a bad URL option, or a TLS setup that can not work).
- New `sslRootCert` option of `MysqlNativePool.open()` (URL option `ssl_ca`): a PEM file with more CA certificates to trust, eg. a private CA. Before, only the Mozilla roots were trusted, so a private CA meant turning the check off. Without the option nothing changes.
- Opening a pool no longer looks up the host name while it holds the lock of all pools. A slow DNS lookup in one isolate no longer stops the other isolates.
- The native ABI version is now 3.

## 3.0.1

- The Rust client now runs on the thread of each isolate. Before, every query went to a tokio thread and back (mysql_async). Now a small MySQL client in Rust writes the query to the socket in the same call, and reads the answer when Dart runs next. An idle helper thread wakes Dart only when it is idle and an answer comes. `GET /db` on 2 cores went from 111k to 242k requests per second (Bun: 195k).
- Queries are pipelined: many go down one connection at once, and the server answers them in order. Each query still fails on its own.
- The pool is still one per process (same settings = same pool), and `maxConnections` still counts for the whole process. A connection that one isolate does not use goes to an isolate that waits. A busy isolate can not starve the others, and busy isolates share the connections fairly.
- An isolate that dies (eg. it ends without `close()`) gives its connections back at once. Its open transactions roll back.
- The prepared statement cache has an LRU limit (`statementCacheSize`). The statement used longest ago is closed on the server.
- `threads` of `MysqlNativePool.open()` and `fromUrl()` does nothing any more. There is no runtime thread to size.
- A transaction id still works in every isolate. A request from another isolate goes to the isolate that began the transaction, and the answer comes back.
- mysql_async and tokio are gone. TLS is rustls with ring, like before. The URL options of mysql_async still work (see the README). A few do nothing now; the README says which and why.
- The host is looked up when the pool opens. When a connect fails, it is looked up again on a helper thread, so a new address (eg. after a failover) is used. The isolate never waits for DNS.
- The native ABI version is now 2.
- New `tool/stress.dart`: many isolates, transactions (also across isolates), statement cache churn, errors and a killer that ends random connections. Any request over 2 s counts as a hang.

## 3.0.0

- The first stable release.

## 3.0.0-alpha.7

- The package moved to its own repository: https://github.com/dartondox/dox-mysql-native. The ready-built libraries are on its releases (tag `v<version>`).

- Each request now has a timeout of 300 s (pool slot, connect and query), like `dox_postgres_native`. Before, a request could wait for a free connection forever.
- A transaction that gets no request for 300 s (eg. its isolate is gone) is rolled back, and its connection goes back to the pool. Before, it held the connection forever.
- Fewer copies of the result bytes. The record head is written in front of the rows, so the record is not copied again. The first result of a batch is moved into the post buffer, not copied. A batch of 64 KiB or more goes to Dart without a copy (external typed data).
- The column names are kept per SQL text. The next result of the same query does not decode them again.
- New `verifyCertificate` on `MysqlNativePool.open()`. It is true by default, so TLS checks the server certificate. Set it to false only for a self-signed certificate in development.
- Fix: a finished transaction gives its connection back to the pool at once. Before, its connection stayed taken for 300 s after commit or rollback. So after 10 transactions in a row the pool was empty, and the next request waited forever.
- Breaking: the `mysql_dart` package is removed. There is no Dart driver fallback any more.
- Breaking: errors are now `MysqlException` (`message`, `code`, `sqlState`). Before, they were `MySQLServerException` and `MySQLClientException` from `mysql_dart`. For a server error, `code` is the MySQL error number (eg. 1062) and `sqlState` is the 5 character SQLSTATE. For other errors both are null.

## 3.0.0-alpha.6

- Unix sockets. In `MysqlNativePool.open()`, a `host` that starts with `/` is the socket file. `fromUrl()` takes `?socket=/path`.
- `close()` is safe with many isolates. Isolates that open the same settings share one pool. Each `open` now counts, and only the last `close()` disconnects the pool. Before, the first `close()` closed it for every isolate.

## 3.0.0-alpha.5

- The hook comment says how to fill the sha256 list from a release: `dart run bin/native_hashes.dart <tag>`. The list stays empty until the first release.

## 3.0.0-alpha.4

- Version bump only, to match the other packages.

## 3.0.0-alpha.3

- Breaking: this package no longer depends on `dox_orm`. `MysqlNativeDriver` moved into `dox_orm`, which now uses it as its default MySQL driver. This package keeps `MysqlNativePool`, with the same API.

## 3.0.0-alpha.2

- First release.
- Native MySQL and MariaDB driver: Rust (mysql_async, tokio) runs the pool
  and a prepared statement cache per connection. Dart gets typed rows.
- `MysqlNativeDriver` plugs into the Dox ORM. The ORM gets the same types
  as with the Dart driver (mysql_dart).
- Works with MySQL 8.4 (caching_sha2_password) and MariaDB 11.
- The build hook downloads a ready-built library. No Rust needed.
