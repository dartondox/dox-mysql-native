import 'dart:async';
import 'dart:convert';
import 'dart:ffi';
import 'dart:io';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:dox_mysql_native/src/bindings.dart';
import 'package:ffi/ffi.dart';

/// The result of one query.
class MysqlNativeResult {
  /// column names, empty when the query returns no rows (eg. INSERT)
  final List<String> columns;

  /// one map per row, column name to value
  final List<Map<String, dynamic>> rows;

  /// rows changed by INSERT, UPDATE or DELETE
  final int affectedRows;

  /// the AUTO_INCREMENT id of an INSERT, else 0
  final int insertId;

  const MysqlNativeResult(
    this.columns,
    this.rows,
    this.affectedRows,
    this.insertId,
  );
}

/// A pool of MySQL or MariaDB connections, owned by Rust. The Rust client
/// runs on the thread of each isolate. Each connection keeps a cache of
/// prepared statements.
///
/// ```
/// MysqlNativePool pool = MysqlNativePool.open(
///   host: 'localhost', user: 'root', password: 'secret', database: 'app',
/// );
/// MysqlNativeResult r = await pool.query(
///   'SELECT * FROM users WHERE id = ?',
///   parameters: <Object?>[1],
/// );
/// ```
///
/// Values come back typed: int, double (also DECIMAL), String, bool
/// (TINYINT(1)), DateTime in UTC (DATE, DATETIME, TIMESTAMP), decoded JSON,
/// Uint8List (BLOB, BINARY) and null. MariaDB keeps JSON in LONGTEXT, so
/// there TEXT that is a JSON object or array comes back decoded.
///
/// Connections open when they are needed. Isolates that open the same
/// settings share one pool. Or give [handle] to another isolate and use
/// [MysqlNativePool.fromHandle] there.
class MysqlNativePool {
  MysqlNativePool.fromHandle(this.handle);

  /// the pool id in Rust. Works in every isolate of this process.
  final int handle;

  /// true when the native library is loaded and works.
  static bool get isAvailable => _Link.available;

  /// why [isAvailable] is false, or null.
  static String? get loadError => _Link.loadError;

  /// Makes a pool of up to [maxConnections] connections.
  ///
  /// A [host] that starts with `/` is the path of a Unix socket file, eg.
  /// `/var/run/mysqld/mysqld.sock` or `/cloudsql/project:region:instance`.
  /// Then [port] is not used.
  ///
  /// [secure] turns on TLS. The server certificate is checked. Set
  /// [verifyCertificate] to false to skip the check, only for a self-signed
  /// certificate in development.
  ///
  /// [sslRootCert] is the path of a PEM file with more CA certificates to
  /// trust when [secure] is on, eg. a private CA or the CA bundle of a
  /// cloud database. The Mozilla roots stay trusted too. Use it instead of
  /// turning [verifyCertificate] off. A file that is missing or has no
  /// valid certificate throws an [ArgumentError] with the reason.
  ///
  /// [maxConnections] is for the whole process: isolates that open the
  /// same settings share it.
  ///
  /// [requestTimeout] is the longest wait for one request: a free
  /// connection, the connect and the query. Over it the request fails
  /// with a [MysqlException], and its connection is closed. With
  /// [serverKill] on, a `KILL` on a new connection also stops the query on
  /// the server. A transaction with no request for this long is rolled
  /// back. For a [stream], the time counts from the last chunk.
  ///
  /// [serverKill] (default true): a cancelled or timed out query is
  /// stopped on the server with `KILL QUERY` or `KILL`, sent on a new
  /// connection. The new connection goes only to the address of the
  /// query's connection, and the KILL is only sent when it reached the
  /// same server (same `@@server_uuid`, on MariaDB same hostname, port and
  /// server_id). Else no KILL is sent. Turn it off behind a proxy that
  /// has its own connection ids (eg. ProxySQL). Off: a cancelled query
  /// runs to its end on the server and only its result is dropped, and a
  /// timed out query only has its socket closed. The new connection also
  /// counts for `max_user_connections`. A KILL that fails prints a warning
  /// on stderr.
  ///
  /// [maxConnectionAge] closes a connection that is older than this, once
  /// it has no query (so a connection in use is never closed). null: no
  /// limit. Whole seconds.
  ///
  /// [idleTimeout] closes a connection that had no traffic for this long,
  /// once it has no query. null: 12 hours. Whole seconds.
  ///
  /// [threads] is not used any more. The Rust client runs on the thread of
  /// each isolate, so there is no runtime to size. It stays so old code
  /// still compiles.
  factory MysqlNativePool.open({
    String host = 'localhost',
    int port = 3306,
    required String user,
    String password = '',
    required String database,
    int maxConnections = 10,
    bool secure = false,
    bool verifyCertificate = true,
    String? sslRootCert,
    int statementCacheSize = 32,
    Duration requestTimeout = const Duration(seconds: 300),
    Duration? maxConnectionAge,
    Duration? idleTimeout,
    bool serverKill = true,
    int threads = 1,
  }) {
    bool socket = host.startsWith('/');
    Uri url = Uri(
      scheme: 'mysql',
      userInfo: '${Uri.encodeComponent(user)}:${Uri.encodeComponent(password)}',
      host: socket ? 'localhost' : host,
      port: port,
      path: '/${Uri.encodeComponent(database)}',
      queryParameters: <String, String>{
        // reset_connection would reset each connection when it goes back to
        // the pool: one more round trip, and it drops the statement cache.
        'reset_connection': 'false',
        // do not switch to the unix socket of the server (eg. in docker)
        'prefer_socket': 'false',
        'pool_min': '$maxConnections',
        'pool_max': '$maxConnections',
        'stmt_cache_size': '$statementCacheSize',
        'conn_ttl': '${(idleTimeout ?? const Duration(hours: 12)).inSeconds}',
        if (maxConnectionAge != null)
          'abs_conn_ttl': '${maxConnectionAge.inSeconds}',
        'request_timeout_ms': '${requestTimeout.inMilliseconds}',
        if (!serverKill) 'server_kill': 'false',
        if (secure) 'require_ssl': 'true',
        // sent also without TLS, so fromUrl() can warn that it is ignored
        'ssl_ca': ?sslRootCert,
        // only for a self-signed certificate in development
        if (secure && !verifyCertificate) ...<String, String>{
          'verify_ca': 'false',
          'verify_identity': 'false',
        },
        if (socket) 'socket': host,
      },
    );
    return MysqlNativePool.fromUrl(url.toString(), threads: threads);
  }

  /// Makes a pool from a `mysql://user:password@host:port/database` URL.
  /// It takes the URL options of mysql_async (see the README), like
  /// `?pool_max=20`, or `?socket=/var/run/mysqld/mysqld.sock` for a Unix
  /// socket. [threads] is not used any more.
  ///
  /// Throws an [ArgumentError] with the reason when the pool can not open,
  /// eg. a bad URL or a TLS setup that can not work.
  factory MysqlNativePool.fromUrl(String url, {int threads = 1}) {
    if (!isAvailable) {
      throw UnsupportedError('dox_mysql_native is not available: $loadError');
    }
    _warnIfCaIgnored(url);
    Uint8List bytes = utf8.encode(url);
    const int errorCapacity = 1024;
    // Native memory, not `.address`: the connect is not a leaf call (it
    // looks up the host), and only a leaf call can take a Dart list. Zeroed,
    // because the error text has no 0 at its end.
    Pointer<Uint8> memory = calloc<Uint8>(bytes.length + errorCapacity);
    int handle;
    Uint8List error;
    try {
      memory.asTypedList(bytes.length).setAll(0, bytes);
      Pointer<Uint8> errorPointer = memory + bytes.length;
      handle = doxMysqlConnect(
        memory,
        bytes.length,
        threads,
        errorPointer,
        errorCapacity,
      );
      error = Uint8List.fromList(errorPointer.asTypedList(errorCapacity));
    } finally {
      calloc.free(memory);
    }
    if (handle <= 0) {
      int end = error.indexOf(0);
      String why = utf8.decode(
        error.sublist(0, end < 0 ? null : end),
        allowMalformed: true,
      );
      throw ArgumentError(
        'dox_mysql_native: bad connection config${why.isEmpty ? '' : ': $why'}',
      );
    }
    return MysqlNativePool.fromHandle(handle);
  }

  static bool _warnedCaIgnored = false;

  /// `ssl_ca` does nothing without `require_ssl=true`. Says so on stderr,
  /// once per isolate, so the CA file is not silently ignored.
  static void _warnIfCaIgnored(String url) {
    if (_warnedCaIgnored) return;
    Map<String, String> query =
        Uri.tryParse(url)?.queryParameters ?? <String, String>{};
    if (query.containsKey('ssl_ca') && query['require_ssl'] != 'true') {
      _warnedCaIgnored = true;
      stderr.writeln(
        'dox_mysql_native: warning: sslRootCert (ssl_ca) is ignored '
        'because TLS is off (secure / require_ssl is not on).',
      );
    }
  }

  /// Runs [sql] and returns its rows.
  ///
  /// [parameters] is a list for `?` placeholders, or a map for `@name`
  /// placeholders (like the Dox ORM). Values can be null, int, double,
  /// bool, String, DateTime, Uint8List, or a Map or List (sent as JSON).
  /// [transaction] is an id from [begin].
  /// Throws [MysqlException]. For an error from the server it has the
  /// error `code` and the `sqlState`. For other errors they are null.
  ///
  /// [cancelToken] can stop the query: then it throws a
  /// [MysqlQueryCancelledException]. See [MysqlCancelToken].
  Future<MysqlNativeResult> query(
    String sql, {
    Object? parameters,
    int transaction = 0,
    MysqlCancelToken? cancelToken,
  }) => _send(_opQuery, sql, parameters, transaction, cancelToken);

  /// Like [query], but the rows are not sent back to Dart.
  Future<MysqlNativeResult> execute(
    String sql, {
    Object? parameters,
    int transaction = 0,
    MysqlCancelToken? cancelToken,
  }) => _send(_opExecute, sql, parameters, transaction, cancelToken);

  /// Runs [sql] and sends its rows in chunks of up to [chunkSize] rows.
  /// Each chunk has the same columns and row maps as [query]. The rows are
  /// read from the server as they come, and Rust reads at most 2 chunks
  /// ahead of the listener. So a big result does not fill the memory.
  ///
  /// The query starts when the stream is listened to. A paused
  /// subscription stops the reading. Cancel the subscription to stop the
  /// query: the connection is cleaned up and goes back to the pool. An
  /// error is a stream error, and then the stream closes.
  ///
  /// A stream gets a connection of its own (outside a transaction), so no
  /// other query waits behind it. Inside a [transaction] it runs on the
  /// connection of the transaction like any query.
  ///
  /// Note: the MySQL server stops a connection whose client does not read
  /// for `net_write_timeout` seconds (60 by default). So do not pause a
  /// stream for longer than that.
  Stream<MysqlNativeResult> stream(
    String sql, {
    Object? parameters,
    int transaction = 0,
    int chunkSize = 1000,
    MysqlCancelToken? cancelToken,
  }) {
    if (chunkSize < 1) {
      throw ArgumentError.value(chunkSize, 'chunkSize', 'must be 1 or more');
    }
    List<Object?> params;
    (sql, params) = _params(sql, parameters);
    return _Link.instance.stream(
      handle,
      transaction,
      sql,
      params,
      chunkSize,
      cancelToken,
    );
  }

  /// Starts a transaction on one connection. Use its id as `transaction`
  /// of [query], then [commit] or [rollback] it.
  Future<int> begin() async {
    MysqlNativeResult r = await _send(_opBegin, '', null, 0);
    return r.insertId;
  }

  /// Commits the transaction and gives its connection back to the pool.
  Future<void> commit(int transaction) =>
      _send(_opCommit, '', null, transaction);

  /// Rolls the transaction back and gives its connection back to the pool.
  Future<void> rollback(int transaction) =>
      _send(_opRollback, '', null, transaction);

  /// Gives back this open of the pool. Isolates that open the same
  /// settings share one pool, and the last [close] disconnects it for
  /// all of them. Running queries finish. Call it once per
  /// [MysqlNativePool.open]; a pool from [MysqlNativePool.fromHandle] is
  /// closed by the isolate that opened it.
  void close() => doxMysqlClose(handle);

  Future<MysqlNativeResult> _send(
    int op,
    String sql,
    Object? parameters,
    int transaction, [
    MysqlCancelToken? token,
  ]) {
    List<Object?> params;
    (sql, params) = _params(sql, parameters);
    return _Link.instance.send(handle, transaction, op, sql, params, token);
  }

  static (String, List<Object?>) _params(String sql, Object? parameters) {
    if (parameters is Map<String, dynamic>) {
      return _toPositional(sql, parameters);
    } else if (parameters is List<Object?>) {
      return (sql, parameters);
    } else if (parameters == null) {
      return (sql, const <Object?>[]);
    }
    throw ArgumentError.value(parameters, 'parameters', 'a List or a Map');
  }

  /// changes each known `@key` to `?` and lists its value in the same
  /// order. An unknown `@word` (eg. 'a@b.com' in a string) stays as it is.
  // ponytail: no cache of the converted sql. The Dox ORM driver sends
  // lists, so only direct pool users with maps pay for the regex.
  static (String, List<Object?>) _toPositional(
    String sql,
    Map<String, dynamic> values,
  ) {
    List<Object?> params = <Object?>[];
    String converted = sql.replaceAllMapped(_paramPattern, (Match m) {
      String key = m.group(1)!;
      if (!values.containsKey(key)) return m.group(0)!;
      params.add(values[key]);
      return '?';
    });
    return (converted, params);
  }

  static final RegExp _paramPattern = RegExp(r'@(\w+)');
}

const int _opQuery = 0;
const int _opExecute = 1;
const int _opBegin = 2;
const int _opCommit = 3;
const int _opRollback = 4;
const int _opStream = 5;
const int _opCancel = 6;
const int _opMore = 7;

const int _tNull = 0;
const int _tInt = 1;
const int _tDouble = 2;
const int _tText = 3;
const int _tBytes = 4;
const int _tDate = 5;
const int _tBool = 6;
const int _tJson = 7;
const int _tLongText = 8;

/// An error from the pool.
///
/// For an error from the MySQL server, [code] is the MySQL error number
/// (eg. 1062 for a duplicate key) and [sqlState] is the 5 character
/// SQLSTATE. For other errors (eg. no connection) both are null.
class MysqlException implements Exception {
  MysqlException(this.message, {this.code, this.sqlState});

  final String message;
  final int? code;
  final String? sqlState;

  @override
  String toString() {
    String extra = <String>[
      if (code != null) 'code $code',
      if (sqlState != null) 'sqlState $sqlState',
    ].join(', ');
    return extra.isEmpty
        ? 'MysqlException: $message'
        : 'MysqlException($extra): $message';
  }
}

/// The query was cancelled with a [MysqlCancelToken]. The server may have
/// run a part of it, or all of it when it ended at the same time.
class MysqlQueryCancelledException extends MysqlException {
  MysqlQueryCancelledException()
    : super('dox_mysql_native: the query was cancelled', sqlState: '70100');
}

/// Stops queries. Give it as `cancelToken` to [MysqlNativePool.query],
/// [MysqlNativePool.execute] or [MysqlNativePool.stream], then call
/// [cancel].
///
/// - A query that did not start yet does not run.
/// - A running query is stopped on the server with `KILL QUERY`, sent on a
///   new connection. When more queries wait on the same connection, it
///   runs to its end instead, and its rows are dropped.
/// - The KILL is only sent when the new connection reached the same
///   server. Behind a proxy with its own connection ids, turn the KILL off
///   with `serverKill: false` in [MysqlNativePool.open]. Then the query fails at once, but runs to its
///   end on the server. The KILL connection also counts for
///   `max_user_connections`. A KILL that fails prints a warning on stderr.
/// - A query that ended already is not changed.
///
/// The query then fails with a [MysqlQueryCancelledException]. One token
/// can stop many queries. After [cancel], new queries with it fail at
/// once.
class MysqlCancelToken {
  bool _cancelled = false;

  /// the ids of the running requests that use this token
  final Set<int> _ids = <int>{};

  /// true after [cancel]
  bool get isCancelled => _cancelled;

  /// Stops the queries of this token.
  void cancel() {
    if (_cancelled) return;
    _cancelled = true;
    for (int id in _ids.toList()) {
      _Link._instance?._control(_opCancel, id, 0);
    }
  }
}

/// One per isolate: the link in Rust, and the requests that wait for their
/// results. Requests of one microtask go to Rust in one call. Rust's idle
/// helper posts `[1]` to the port when results come: then [_run] reads
/// them. An empty message only checks that the isolate is alive.
class _Link {
  _Link._() {
    _port = RawReceivePort(_onMessage, 'dox_mysql_native');
    _port.keepIsolateAlive = false;
    _handle = doxMysqlClient(_port.sendPort.nativePort);
    if (_handle <= 0) {
      _port.close();
      throw MysqlException('dox_mysql_native: dox_mysql_client failed');
    }
  }

  static _Link? _instance;

  static _Link get instance {
    if (!available) {
      throw UnsupportedError('dox_mysql_native is not available: $loadError');
    }
    return _instance ??= _Link._();
  }

  static bool? _available;
  static String? _loadError;

  static bool get available => _available ??= _load();

  static String? get loadError {
    available;
    return _loadError;
  }

  static bool _load() {
    try {
      int version = doxMysqlVersion();
      if (version != abiVersion) {
        _loadError = 'native library ABI $version, expected $abiVersion';
        return false;
      }
      int rc = doxMysqlInit(NativeApi.initializeApiDLData);
      if (rc != 0) {
        _loadError = 'dox_mysql_init failed ($rc)';
        return false;
      }
      return true;
    } catch (error) {
      // the build hook found no library for this platform
      _loadError =
          'the native library is not in the build ($error). If `skip: true` '
          'is set for dox_mysql_native under `hooks: user_defines:` in the '
          "app's root pubspec.yaml, remove it.";
      return false;
    }
  }

  late final RawReceivePort _port;
  late final int _handle;
  int _nextId = 1;
  final Map<int, _Pending> _pending = <int, _Pending>{};

  Uint8List _out = Uint8List(1 << 14);
  late ByteData _outData = ByteData.sublistView(_out);
  int _len = 0;
  bool _flushQueued = false;

  Future<MysqlNativeResult> send(
    int pool,
    int tx,
    int op,
    String sql,
    List<Object?> params, [
    MysqlCancelToken? token,
  ]) {
    if (token != null && token.isCancelled) {
      return Future<MysqlNativeResult>.error(MysqlQueryCancelledException());
    }
    _Pending pending = _Pending(sql, pool, tx, token)
      ..completer = Completer<MysqlNativeResult>();
    _start(pending, op, params);
    return pending.completer!.future;
  }

  /// Rows in chunks. The request goes when the stream is listened to.
  Stream<MysqlNativeResult> stream(
    int pool,
    int tx,
    String sql,
    List<Object?> params,
    int chunkSize,
    MysqlCancelToken? token,
  ) {
    _Pending pending = _Pending(sql, pool, tx, token);
    // sync: a listener that pauses in its handler (eg. `await for`) is
    // paused right after the add, so no more chunks are asked for
    StreamController<MysqlNativeResult> controller =
        StreamController<MysqlNativeResult>(sync: true);
    pending.controller = controller;
    controller.onListen = () {
      if (token != null && token.isCancelled) {
        controller
          ..addError(MysqlQueryCancelledException())
          ..close();
        return;
      }
      _start(pending, _opStream, <Object?>[...params, chunkSize]);
    };
    controller.onResume = () {
      int owed = pending.owed;
      pending.owed = 0;
      if (owed > 0 && _pending[pending.id] == pending) {
        _control(_opMore, pending.id, owed);
      }
    };
    controller.onCancel = () {
      if (_pending[pending.id] != pending) return;
      _control(_opCancel, pending.id, 0);
      _done(pending.id);
    };
    return controller.stream;
  }

  /// Writes the request of [pending] and queues the flush.
  void _start(_Pending pending, int op, List<Object?> params) {
    int id = pending.id = _nextId++;
    if (_pending.isEmpty) _port.keepIsolateAlive = true;
    _pending[id] = pending;
    int start = _len;
    try {
      _write(id, pending.pool, pending.tx, op, pending.sql, params);
    } catch (_) {
      _len = start;
      _done(id);
      rethrow;
    }
    pending.token?._ids.add(id);
    _queueFlush();
  }

  /// A cancel or more record for the request [id] (see lib.rs).
  void _control(int op, int id, int count) {
    _Pending? pending = _pending[id];
    if (pending == null) return;
    _write(0, pending.pool, pending.tx, op, '', <Object?>[
      id,
      if (op == _opMore) count,
    ]);
    _queueFlush();
  }

  void _queueFlush() {
    if (!_flushQueued) {
      _flushQueued = true;
      scheduleMicrotask(_flush);
    }
  }

  void _flush() {
    _flushQueued = false;
    if (_len == 0) return;
    // 1: some results are ready already (eg. the pool is closed)
    if (doxMysqlSubmit(_handle, _out.address, _len) == 1) {
      scheduleMicrotask(_run);
    }
    _len = 0;
    if (_out.length > 1 << 20) {
      _out = Uint8List(1 << 14); // do not keep a big buffer forever
      _outData = ByteData.sublistView(_out);
    }
  }

  void _done(int id) {
    _pending.remove(id)?.token?._ids.remove(id);
    if (_pending.isEmpty) _port.keepIsolateAlive = false;
  }

  // ---- requests ----

  void _ensure(int extra) {
    int need = _len + extra;
    if (need <= _out.length) return;
    int size = _out.length * 2;
    while (size < need) {
      size *= 2;
    }
    Uint8List grown = Uint8List(size)..setRange(0, _len, _out);
    _out = grown;
    _outData = ByteData.sublistView(grown);
  }

  void _write(
    int id,
    int pool,
    int tx,
    int op,
    String sql,
    List<Object?> params,
  ) {
    // head: u64 id, u64 pool, u64 tx, u8 op + 3 reserved, u32 sql_len,
    // u32 param count
    _ensure(36);
    int start = _len;
    _outData.setUint64(start, id, Endian.little);
    _outData.setUint64(start + 8, pool, Endian.little);
    _outData.setUint64(start + 16, tx, Endian.little);
    _outData.setUint32(start + 24, op, Endian.little);
    _outData.setUint32(start + 32, params.length, Endian.little);
    _len += 36;
    _string(sql);
    _outData.setUint32(start + 28, _len - start - 36, Endian.little);
    for (Object? value in params) {
      _param(value);
    }
  }

  /// writes utf8 bytes, ASCII without a copy
  void _string(String s) {
    _ensure(s.length);
    Uint8List o = _out;
    int at = _len;
    for (int i = 0; i < s.length; i++) {
      int c = s.codeUnitAt(i);
      if (c >= 128) {
        Uint8List bytes = utf8.encode(s);
        _ensure(bytes.length);
        _out.setRange(_len, _len + bytes.length, bytes);
        _len += bytes.length;
        return;
      }
      o[at++] = c;
    }
    _len = at;
  }

  void _param(Object? value) {
    if (value is bool) value = value ? 1 : 0;
    if (value == null) {
      _ensure(1);
      _out[_len++] = _tNull;
    } else if (value is int) {
      _ensure(9);
      _out[_len] = _tInt;
      _outData.setInt64(_len + 1, value, Endian.little);
      _len += 9;
    } else if (value is double) {
      _ensure(9);
      _out[_len] = _tDouble;
      _outData.setFloat64(_len + 1, value, Endian.little);
      _len += 9;
    } else if (value is Uint8List) {
      _ensure(5 + value.length);
      _out[_len] = _tBytes;
      _outData.setUint32(_len + 1, value.length, Endian.little);
      _out.setRange(_len + 5, _len + 5 + value.length, value);
      _len += 5 + value.length;
    } else {
      String text = value is DateTime
          ? _dateText(value)
          : value is Map || value is List
          ? jsonEncode(value)
          : value.toString();
      _ensure(5);
      int at = _len;
      _out[at] = _tText;
      _len += 5;
      _string(text);
      _outData.setUint32(at + 1, _len - at - 5, Endian.little);
    }
  }

  /// `2024-01-02 03:04:05.000006`, the time in UTC
  static String _dateText(DateTime d) {
    String s = d.toUtc().toIso8601String();
    return s.substring(0, s.length - 1).replaceFirst('T', ' ');
  }

  // ---- results ----

  /// the results, written by Rust
  Uint8List _in = Uint8List(1 << 16);

  void _onMessage(Object? message) {
    if ((message as Uint8List).isNotEmpty) _run();
  }

  /// Reads the results that are there. Never waits.
  void _run() {
    int n = doxMysqlRun(_handle, _in.address, _in.length);
    while (n < 0) {
      int size = _in.length * 2;
      while (size < -n) {
        size *= 2;
      }
      _in = Uint8List(size);
      n = doxMysqlRun(_handle, _in.address, _in.length);
    }
    // a view, no copy: _onBatch reads all it needs before it returns
    if (n > 0) _onBatch(Uint8List.sublistView(_in, 0, n));
    if (_in.length > 1 << 20) _in = Uint8List(1 << 16);
  }

  void _onBatch(Uint8List b) {
    ByteData d = ByteData.sublistView(b);
    int p = 0;
    while (p + 16 <= b.length) {
      int id = d.getUint64(p, Endian.little);
      int status = b[p + 8];
      int len = d.getUint32(p + 12, Endian.little);
      int body = p + 16;
      p = body + len;
      _Pending? pending = _pending[id];
      if (pending == null) continue;
      // one try for each record, not for each cell: no cost while nothing
      // throws
      try {
        _record(pending, id, status, len, b, d, body);
      } catch (e) {
        _bad(pending, id, status, e);
      }
    }
  }

  /// One result record of a request.
  void _record(
    _Pending pending,
    int id,
    int status,
    int len,
    Uint8List b,
    ByteData d,
    int body,
  ) {
    StreamController<MysqlNativeResult>? controller = pending.controller;
    if (controller == null) {
      _done(id);
      if (status == 0) {
        pending.completer!.complete(_rows(b, d, body, pending.sql));
      } else {
        pending.completer!.completeError(_error(b, d, body, len));
      }
      return;
    }
    if (status == 1) {
      _done(id);
      controller
        ..addError(_error(b, d, body, len))
        ..close();
      return;
    }
    MysqlNativeResult chunk = _rows(b, d, body, pending.sql);
    if (status == 2) {
      pending.sent = true;
      controller.add(chunk);
      // the listener took it: Rust may read one more chunk. Else ask
      // when it resumes.
      if (_pending[id] != pending) return; // cancelled in the listener
      if (controller.isPaused) {
        pending.owed++;
      } else {
        _control(_opMore, id, 1);
      }
      return;
    }
    _done(id);
    // the last rows. No empty last chunk, but one result when there was
    // none (eg. the affected rows of an UPDATE).
    if (chunk.rows.isNotEmpty || !pending.sent) controller.add(chunk);
    controller.close();
  }

  /// A record could not be read (a broken server, or a bug). Only this
  /// request fails, the other records of the batch are still read. A
  /// stream is stopped on the Rust side too, so its connection is free.
  void _bad(_Pending pending, int id, int status, Object e) {
    MysqlException error = MysqlException('bad result: $e');
    StreamController<MysqlNativeResult>? controller = pending.controller;
    // a chunk (status 2): more may come, so stop the query. Before
    // `_done`, as in `onCancel`: `_control` needs the pending request.
    if (controller != null && status == 2 && _pending[id] == pending) {
      _control(_opCancel, id, 0);
    }
    _done(id);
    if (controller == null) {
      if (!pending.completer!.isCompleted) {
        pending.completer!.completeError(error);
      }
      return;
    }
    if (!controller.isClosed) {
      controller
        ..addError(error)
        ..close();
    }
  }

  static MysqlException _error(Uint8List b, ByteData d, int at, int len) {
    int code = d.getUint16(at, Endian.little);
    int kind = b[at + 2];
    String message = utf8.decode(
      Uint8List.sublistView(b, at + 8, at + len),
      allowMalformed: true,
    );
    if (kind == 2) return MysqlQueryCancelledException();
    if (kind != 0) return MysqlException(message);
    String sqlState = ascii.decode(
      Uint8List.sublistView(b, at + 3, at + 8),
      allowInvalid: true,
    );
    return MysqlException(message, code: code, sqlState: sqlState.trim());
  }

  /// the column names of the last result of each sql, and the bytes they
  /// came in. The same query gives the same columns, so most results skip
  /// the decode. The same name strings also keep their hash code for the
  /// row maps.
  // ponytail: cleared when full, like the sql caches of the drivers
  static final Map<String, (Uint8List, List<String>)> _columns =
      <String, (Uint8List, List<String>)>{};

  static MysqlNativeResult _rows(Uint8List b, ByteData d, int p, String sql) {
    int affected = d.getUint64(p, Endian.little);
    int insertId = d.getUint64(p + 8, Endian.little);
    int ncols = d.getUint32(p + 16, Endian.little);
    int nrows = d.getUint32(p + 20, Endian.little);
    p += 24;
    int start = p;
    for (int i = 0; i < ncols; i++) {
      p += 2 + d.getUint16(p, Endian.little);
    }
    (Uint8List, List<String>)? known = _columns[sql];
    // the bytes are checked: the columns change after eg. ALTER TABLE
    if (known == null || !_same(known.$1, b, start, p)) {
      List<String> names = List<String>.filled(ncols, '');
      int at = start;
      for (int i = 0; i < ncols; i++) {
        int len = d.getUint16(at, Endian.little);
        names[i] = _text(b, at + 2, len);
        at += 2 + len;
      }
      if (_columns.length >= 1024) _columns.clear();
      known = _columns[sql] = (
        Uint8List.fromList(Uint8List.sublistView(b, start, p)),
        List<String>.unmodifiable(names), // shared by results
      );
    }
    List<String> names = known.$2;
    List<Map<String, dynamic>> rows = <Map<String, dynamic>>[];
    for (int r = 0; r < nrows; r++) {
      Map<String, dynamic> row = <String, dynamic>{};
      rows.add(row);
      for (int c = 0; c < ncols; c++) {
        int tag = b[p++];
        dynamic value;
        switch (tag) {
          case _tNull:
            value = null;
          case _tInt:
            value = d.getInt64(p, Endian.little);
            p += 8;
          case _tDouble:
            value = d.getFloat64(p, Endian.little);
            p += 8;
          case _tDate:
            value = DateTime.fromMicrosecondsSinceEpoch(
              d.getInt64(p, Endian.little),
              isUtc: true,
            );
            p += 8;
          case _tBool:
            value = b[p++] != 0;
          default:
            int len = d.getUint32(p, Endian.little);
            p += 4;
            if (tag == _tBytes) {
              value = Uint8List.fromList(Uint8List.sublistView(b, p, p + len));
            } else {
              String text = _text(b, p, len);
              value = tag == _tJson
                  ? _json(text)
                  : tag == _tLongText
                  ? _maybeJson(text)
                  : text;
            }
            p += len;
        }
        row[names[c]] = value;
      }
    }
    return MysqlNativeResult(names, rows, affected, insertId);
  }

  /// true when [b] from [start] to [end] has the bytes of [head]
  static bool _same(Uint8List head, Uint8List b, int start, int end) {
    if (head.length != end - start) return false;
    for (int i = 0; i < head.length; i++) {
      if (head[i] != b[start + i]) return false;
    }
    return true;
  }

  static String _text(Uint8List b, int at, int len) {
    // a bad byte gives U+FFFD. A throw here would stop the whole batch of
    // results, and the request would never finish.
    return utf8.decode(
      Uint8List.sublistView(b, at, at + len),
      allowMalformed: true,
    );
  }

  /// A JSON value. Text that is not valid JSON stays text, like in
  /// [_maybeJson], so bad data from the server does not throw.
  static Object? _json(String text) {
    try {
      return jsonDecode(text);
    } on FormatException {
      return text;
    }
  }

  /// MariaDB: text that is a JSON object or array is decoded
  static Object? _maybeJson(String text) {
    String t = text.trimLeft();
    if (!t.startsWith('{') && !t.startsWith('[')) return text;
    try {
      return jsonDecode(text);
    } on FormatException {
      return text;
    }
  }
}

class _Pending {
  _Pending(this.sql, this.pool, this.tx, this.token);

  /// the sql of the request, to find the columns it had before
  final String sql;
  final int pool;
  final int tx;
  final MysqlCancelToken? token;
  int id = 0;

  /// a query or execute
  Completer<MysqlNativeResult>? completer;

  /// a stream
  StreamController<MysqlNativeResult>? controller;

  /// a stream: chunks the listener took while paused, to ask for on resume
  int owed = 0;

  /// a stream: a chunk was sent
  bool sent = false;
}

/// For tests only (not exported by the package): the next result of [sql]
/// with these column bytes (u16 length and name, for each column) can not be
/// read, so its record goes to `_Link._bad`.
void debugPoisonColumns(String sql, List<int> bytes) {
  _Link._columns[sql] = (Uint8List.fromList(bytes), const <String>[]);
}
