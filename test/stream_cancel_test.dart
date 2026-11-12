import 'dart:async';
import 'dart:io';

import 'package:dox_mysql_native/dox_mysql_native.dart';
import 'package:test/test.dart';

/// Tests of the pool settings, query cancel and row streams. Same env vars
/// as pool_test.dart.
String host = Platform.environment['DB_HOST'] ?? '127.0.0.1';
int port = int.parse(Platform.environment['DB_PORT'] ?? '3306');
String user = Platform.environment['DB_USER'] ?? 'root';

/// [cache] makes the settings (and so the pool) unique per test.
MysqlNativePool open({
  required int cache,
  int max = 10,
  Duration requestTimeout = const Duration(seconds: 300),
  Duration? maxConnectionAge,
  Duration? idleTimeout,
}) {
  return MysqlNativePool.open(
    host: host,
    port: port,
    user: user,
    password: 'password',
    database: 'dox-framework',
    maxConnections: max,
    statementCacheSize: cache,
    requestTimeout: requestTimeout,
    maxConnectionAge: maxConnectionAge,
    idleTimeout: idleTimeout,
  );
}

/// n rows (n = 10^digits) with a number and some padding, on MySQL and
/// MariaDB (no recursive CTE limit)
String rowsSql(int digits, {int pad = 10}) {
  String d =
      '(SELECT 0 AS d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL '
      'SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 '
      'UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9)';
  List<String> tables = <String>[for (int i = 0; i < digits; i++) '$d t$i'];
  String n = <String>[for (int i = 0; i < digits; i++) 't$i.d * ${_pow10(i)}']
      .join(' + ');
  return 'SELECT $n AS n, REPEAT(\'x\', $pad) AS pad '
      'FROM ${tables.join(' CROSS JOIN ')}';
}

int _pow10(int i) => i == 0 ? 1 : 10 * _pow10(i - 1);

Future<int> connectionId(MysqlNativePool pool) async =>
    (await pool.query('SELECT CONNECTION_ID() AS id')).rows.single['id'] as int;

void main() {
  test('a query over requestTimeout fails, and the pool still works', () async {
    MysqlNativePool pool = open(
      cache: 41,
      max: 1,
      requestTimeout: const Duration(seconds: 1),
    );
    Stopwatch watch = Stopwatch()..start();
    await expectLater(
      pool.query('SELECT SLEEP(5)'),
      throwsA(
        isA<MysqlException>().having(
          (MysqlException e) => e.message,
          'message',
          contains('timeout, no answer in 1 s'),
        ),
      ),
    );
    expect(watch.elapsed, lessThan(const Duration(seconds: 3)));
    expect((await pool.query('SELECT 1 AS a')).rows.single['a'], 1);
    pool.close();
  });

  test('cancel stops a long query on the server', () async {
    MysqlNativePool pool = open(cache: 42, max: 1);
    MysqlCancelToken token = MysqlCancelToken();
    Stopwatch watch = Stopwatch()..start();
    Future<MysqlNativeResult> slow = pool.query(
      'SELECT SLEEP(10)',
      cancelToken: token,
    );
    await Future<void>.delayed(const Duration(milliseconds: 300));
    token.cancel();
    await expectLater(slow, throwsA(isA<MysqlQueryCancelledException>()));
    expect(watch.elapsed, lessThan(const Duration(seconds: 5)));
    // one connection: the killed one is closed, and a new one opens
    for (int i = 0; i < 3; i++) {
      expect((await pool.query('SELECT 1 AS a')).rows.single['a'], 1);
    }
    pool.close();
  });

  test('cancel of a prepared query, and of one in a transaction', () async {
    MysqlNativePool pool = open(cache: 43, max: 2);
    MysqlCancelToken token = MysqlCancelToken();
    Future<MysqlNativeResult> slow = pool.query(
      'SELECT SLEEP(?)',
      parameters: <Object?>[10],
      cancelToken: token,
    );
    await Future<void>.delayed(const Duration(milliseconds: 300));
    token.cancel();
    await expectLater(slow, throwsA(isA<MysqlQueryCancelledException>()));

    int tx = await pool.begin();
    MysqlCancelToken t2 = MysqlCancelToken();
    Future<MysqlNativeResult> inTx = pool.query(
      'SELECT SLEEP(10)',
      transaction: tx,
      cancelToken: t2,
    );
    await Future<void>.delayed(const Duration(milliseconds: 300));
    t2.cancel();
    await expectLater(inTx, throwsA(isA<MysqlQueryCancelledException>()));
    // the transaction is still there
    expect(
      (await pool.query('SELECT 2 AS a', transaction: tx)).rows.single['a'],
      2,
    );
    await pool.commit(tx);
    pool.close();
  });

  test(
    'a cancelled token runs nothing, a late cancel changes nothing',
    () async {
      MysqlNativePool pool = open(cache: 44);
      await pool.execute('DROP TABLE IF EXISTS mn_cancel');
      await pool.execute('CREATE TABLE mn_cancel (v int)');
      MysqlCancelToken token = MysqlCancelToken()..cancel();
      await expectLater(
        pool.execute('INSERT INTO mn_cancel VALUES (1)', cancelToken: token),
        throwsA(isA<MysqlQueryCancelledException>()),
      );
      await expectLater(
        pool.stream('SELECT 1', cancelToken: token).toList(),
        throwsA(isA<MysqlQueryCancelledException>()),
      );
      MysqlCancelToken late = MysqlCancelToken();
      await pool.execute('INSERT INTO mn_cancel VALUES (2)', cancelToken: late);
      late.cancel();
      expect((await pool.query('SELECT v FROM mn_cancel')).rows, <Object>[
        <String, dynamic>{'v': 2},
      ]);
      await pool.execute('DROP TABLE mn_cancel');
      pool.close();
    },
  );

  test('cancel of a query that waits for a connection', () async {
    MysqlNativePool pool = open(cache: 45, max: 1);
    int tx = await pool.begin(); // holds the only connection
    MysqlCancelToken token = MysqlCancelToken();
    Future<MysqlNativeResult> queued = pool.query(
      'SELECT 1',
      cancelToken: token,
    );
    await Future<void>.delayed(const Duration(milliseconds: 100));
    token.cancel();
    await expectLater(queued, throwsA(isA<MysqlQueryCancelledException>()));
    await pool.rollback(tx);
    expect((await pool.query('SELECT 1 AS a')).rows.single['a'], 1);
    pool.close();
  });

  test('a stream of 100k rows comes in chunks, with bounded memory', () async {
    MysqlNativePool pool = open(cache: 46, max: 2);
    int rss = ProcessInfo.currentRss;
    int peak = rss;
    int rows = 0;
    int chunks = 0;
    int sum = 0;
    // 100k rows of 1 KB: about 100 MB in all
    await for (MysqlNativeResult chunk in pool.stream(rowsSql(5, pad: 1000))) {
      expect(chunk.columns, <String>['n', 'pad']);
      expect(chunk.rows, hasLength(1000));
      chunks++;
      rows += chunk.rows.length;
      for (Map<String, dynamic> row in chunk.rows) {
        sum += row['n'] as int;
      }
      if (ProcessInfo.currentRss > peak) peak = ProcessInfo.currentRss;
      // a slow listener: Rust must wait, not read all rows
      if (chunks % 20 == 0) {
        await Future<void>.delayed(const Duration(milliseconds: 20));
      }
    }
    expect((rows, chunks), (100000, 100));
    expect(sum, 99999 * 100000 ~/ 2);
    // GC makes this noisy. All rows at once (query) take over 300 MB.
    expect(peak - rss, lessThan(100 << 20));
    // the connection is free again
    expect((await pool.query('SELECT 1 AS a')).rows.single['a'], 1);
    pool.close();
  }, timeout: const Timeout(Duration(minutes: 2)));

  test('a paused stream leaves the rows on the server', () async {
    MysqlNativePool pool = open(cache: 54, max: 2);
    // 100 MB: much more than the socket buffers
    String sql = '/* mn_paused */ ${rowsSql(5, pad: 1000)}';
    StreamIterator<MysqlNativeResult> it = StreamIterator<MysqlNativeResult>(
      pool.stream(sql),
    );
    expect(await it.moveNext(), isTrue);
    // no listener asks now: the subscription is paused
    await Future<void>.delayed(const Duration(seconds: 1));
    // the server still sends rows: Rust did not read them all
    MysqlNativeResult running = await pool.query(
      'SELECT ID FROM information_schema.PROCESSLIST '
      'WHERE INFO LIKE ?',
      parameters: <Object?>['/* mn_paused */%'],
    );
    expect(running.rows, hasLength(1));
    await it.cancel();
    pool.close();
  });

  test('a paused stream reads no more, then goes on', () async {
    MysqlNativePool pool = open(cache: 47, max: 1);
    List<int> sizes = <int>[];
    Completer<void> done = Completer<void>();
    late StreamSubscription<MysqlNativeResult> sub;
    sub = pool.stream(rowsSql(3), chunkSize: 100).listen((MysqlNativeResult r) {
      sizes.add(r.rows.length);
      if (sizes.length == 1) {
        sub.pause(Future<void>.delayed(const Duration(milliseconds: 300)));
      }
    }, onDone: done.complete);
    await done.future;
    expect(sizes, List<int>.filled(10, 100));
    pool.close();
  });

  test('cancel of a stream subscription stops the query', () async {
    MysqlNativePool pool = open(cache: 48, max: 1);
    Stopwatch watch = Stopwatch()..start();
    int chunks = 0;
    // 10M rows: far too many to read to the end in the time limit
    await for (MysqlNativeResult _ in pool.stream(rowsSql(7), chunkSize: 500)) {
      if (++chunks == 3) break;
    }
    expect(chunks, 3);
    // one connection: it must come back clean
    for (int i = 0; i < 3; i++) {
      expect((await pool.query('SELECT 1 AS a')).rows.single['a'], 1);
    }
    expect(watch.elapsed, lessThan(const Duration(seconds: 10)));
    pool.close();
  });

  test('a cancel token stops a stream with an error', () async {
    MysqlNativePool pool = open(cache: 49, max: 1);
    MysqlCancelToken token = MysqlCancelToken();
    int chunks = 0;
    Object? error;
    try {
      await for (MysqlNativeResult _ in pool.stream(
        rowsSql(7),
        chunkSize: 500,
        cancelToken: token,
      )) {
        if (++chunks == 2) token.cancel();
      }
    } catch (e) {
      error = e;
    }
    expect(error, isA<MysqlQueryCancelledException>());
    expect((await pool.query('SELECT 1 AS a')).rows.single['a'], 1);
    pool.close();
  });

  test('a stream inside a transaction sees its rows', () async {
    MysqlNativePool pool = open(cache: 50, max: 2);
    await pool.execute('DROP TABLE IF EXISTS mn_stream');
    await pool.execute('CREATE TABLE mn_stream (v int)');
    int tx = await pool.begin();
    await pool.execute(
      'INSERT INTO mn_stream (v) SELECT n FROM (${rowsSql(3)}) x',
      transaction: tx,
    );
    List<MysqlNativeResult> chunks = await pool
        .stream(
          'SELECT v FROM mn_stream WHERE v >= ? ORDER BY v',
          parameters: <Object?>[10],
          transaction: tx,
          chunkSize: 400,
        )
        .toList();
    expect(chunks.map((MysqlNativeResult r) => r.rows.length), <int>[
      400,
      400,
      190,
    ]);
    expect(chunks.last.rows.last['v'], 999);
    await pool.rollback(tx);
    // not committed: other connections see nothing
    expect(
      (await pool.query('SELECT COUNT(*) AS c FROM mn_stream')).rows,
      <Object>[
        <String, dynamic>{'c': 0},
      ],
    );
    await pool.execute('DROP TABLE mn_stream');
    pool.close();
  });

  test('a stream of a statement with no rows gives one result', () async {
    MysqlNativePool pool = open(cache: 51);
    List<MysqlNativeResult> r = await pool.stream('DO 1').toList();
    expect(r, hasLength(1));
    expect(r.single.rows, isEmpty);
    await expectLater(
      pool.stream('SELECT * FROM mn_no_such_table').toList(),
      throwsA(isA<MysqlException>()),
    );
    pool.close();
  });

  test('idleTimeout closes a connection with no traffic', () async {
    MysqlNativePool pool = open(
      cache: 52,
      max: 1,
      idleTimeout: const Duration(seconds: 1),
    );
    int first = await connectionId(pool);
    expect(await connectionId(pool), first);
    await Future<void>.delayed(const Duration(milliseconds: 2500));
    expect(await connectionId(pool), isNot(first));
    pool.close();
  });

  test('maxConnectionAge closes an old connection between queries', () async {
    MysqlNativePool pool = open(
      cache: 53,
      max: 1,
      maxConnectionAge: const Duration(seconds: 1),
    );
    Set<int> ids = <int>{};
    Stopwatch watch = Stopwatch()..start();
    while (watch.elapsed < const Duration(milliseconds: 2500)) {
      ids.add(await connectionId(pool));
      await Future<void>.delayed(const Duration(milliseconds: 100));
    }
    expect(ids.length, greaterThan(1));
    pool.close();
  });
}
