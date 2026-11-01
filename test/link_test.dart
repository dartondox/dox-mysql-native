import 'dart:io';
import 'dart:isolate';

import 'package:dox_mysql_native/dox_mysql_native.dart';
import 'package:test/test.dart';

/// Tests of the Rust client on the isolate thread: pipelining, the
/// process-wide connection limit, fairness and cleanup. Same env vars as
/// pool_test.dart.
String host = Platform.environment['DB_HOST'] ?? '127.0.0.1';
int port = int.parse(Platform.environment['DB_PORT'] ?? '3306');
String user = Platform.environment['DB_USER'] ?? 'root';

/// [cache] also makes the settings (and so the pool) unique per test.
MysqlNativePool open({
  int max = 10,
  int cache = 32,
  String? name,
  String password = 'password',
}) {
  return MysqlNativePool.open(
    host: host,
    port: port,
    user: name ?? user,
    password: password,
    database: 'dox-framework',
    maxConnections: max,
    statementCacheSize: cache,
  );
}

void main() {
  test('2000 queries at once, errors mixed in, each answer to its own '
      'request', () async {
    MysqlNativePool pool = open(max: 3, cache: 5);
    Future<Object?> one(int i) {
      Future<MysqlNativeResult> q;
      if (i % 7 == 3) {
        q = pool.query('SELEC $i'); // text, syntax error
      } else if (i % 11 == 5) {
        // prepared, its PREPARE fails
        q = pool.query(
          'SELECT * FROM mn_no_such_table WHERE id = ?',
          parameters: <Object?>[i],
        );
      } else if (i % 13 == 6) {
        // wrong param count: a client error
        q = pool.query('SELECT ? AS a, ? AS b', parameters: <Object?>[i]);
      } else if (i.isEven) {
        q = pool.query('SELECT ? AS n', parameters: <Object?>[i]);
      } else if (i % 5 == 1) {
        q = pool.query('SELECT $i AS n, ? AS t', parameters: <Object?>['x$i']);
      } else {
        q = pool.query('SELECT $i AS n');
      }
      return q.then<Object?>(
        (MysqlNativeResult r) => r.rows.single,
        onError: (Object e) => e,
      );
    }

    List<Object?> out = await Future.wait(<Future<Object?>>[
      for (int i = 0; i < 2000; i++) one(i),
    ]).timeout(const Duration(seconds: 30));
    for (int i = 0; i < 2000; i++) {
      Object? r = out[i];
      if (i % 7 == 3) {
        expect((r as MysqlException).code, 1064, reason: '$i');
      } else if (i % 11 == 5) {
        expect((r as MysqlException).code, 1146, reason: '$i');
      } else if (i % 13 == 6) {
        expect(r, isA<MysqlException>(), reason: '$i');
        expect((r as MysqlException).code, isNull, reason: '$i');
      } else if (i.isEven) {
        expect(r, <String, dynamic>{'n': i}, reason: '$i');
      } else if (i % 5 == 1) {
        expect(r, <String, dynamic>{'n': i, 't': 'x$i'}, reason: '$i');
      } else {
        expect(r, <String, dynamic>{'n': i}, reason: '$i');
      }
    }
    pool.close();
  });

  test('the statement cache closes the oldest statements', () async {
    MysqlNativePool admin = open(cache: 31);
    Future<int> prepared() async {
      MysqlNativeResult r = await admin.query(
        "SHOW GLOBAL STATUS LIKE 'Prepared_stmt_count'",
      );
      return int.parse('${r.rows.single['Value']}');
    }

    int before = await prepared();
    MysqlNativePool pool = open(max: 1, cache: 2);
    for (int round = 0; round < 3; round++) {
      List<MysqlNativeResult> all = await Future.wait(
        <Future<MysqlNativeResult>>[
          for (int i = 0; i < 20; i++)
            pool.query('SELECT ? + $i AS n', parameters: <Object?>[round]),
        ],
      );
      for (int i = 0; i < 20; i++) {
        expect(all[i].rows.single['n'], round + i);
      }
    }
    expect(await prepared() - before, lessThanOrEqualTo(2));
    pool.close();
    admin.close();
  });

  test('the connection limit is for the whole process', () async {
    List<List<int>> ids = await Future.wait(<Future<List<int>>>[
      for (int i = 0; i < 4; i++) Isolate.run(() => _connectionIds(i)),
    ]).timeout(const Duration(seconds: 30));
    Set<int> all = <int>{for (List<int> l in ids) ...l};
    expect(all.length, lessThanOrEqualTo(3));
    for (List<int> l in ids) {
      expect(l, hasLength(200));
    }
  });

  test('a busy isolate does not starve another one', () async {
    // one connection for the process. The busy isolate sends queries all
    // the time. The other one must still get its turns.
    Future<int> busy = Isolate.run(_busy);
    await Future<void>.delayed(const Duration(milliseconds: 300));
    Stopwatch sw = Stopwatch()..start();
    int done = await Isolate.run(_fewQueries);
    sw.stop();
    expect(done, 20);
    expect(sw.elapsed, lessThan(const Duration(seconds: 3)));
    expect(await busy, greaterThan(0));
  });

  test('a dead isolate frees its connections', () async {
    // the isolate takes both connections in transactions and dies
    await Isolate.run(_holdAndDie);
    MysqlNativePool pool = open(max: 2, cache: 29);
    List<int> txs = await Future.wait(<Future<int>>[pool.begin(), pool.begin()])
        .timeout(const Duration(seconds: 5));
    for (int tx in txs) {
      await pool.rollback(tx);
    }
    pool.close();
  });

  test('caching_sha2_password: full login without TLS, then the fast '
      'one', () async {
    MysqlNativePool root = open(name: 'root', cache: 28);
    String version =
        '${(await root.query('SELECT VERSION() AS v')).rows.single['v']}';
    if (version.contains('MariaDB')) {
      root.close();
      markTestSkipped('MariaDB has no caching_sha2_password');
      return;
    }
    await root.execute("DROP USER IF EXISTS 'mn_sha2'@'%'");
    // a new user is not in the cache of the server: the first login is
    // the full one, with the RSA key of the server
    await root.execute(
      "CREATE USER 'mn_sha2'@'%' IDENTIFIED WITH caching_sha2_password "
      "BY 'sha2 pass'",
    );
    await root.execute("GRANT SELECT ON `dox-framework`.* TO 'mn_sha2'@'%'");
    for (int cache in <int>[27, 26]) {
      MysqlNativePool pool = open(
        name: 'mn_sha2',
        password: 'sha2 pass',
        cache: cache,
      );
      MysqlNativeResult r = await pool.query('SELECT CURRENT_USER() AS u');
      expect(r.rows.single['u'], 'mn_sha2@%');
      pool.close();
    }
    MysqlNativePool bad = open(name: 'mn_sha2', password: 'nope', cache: 25);
    await expectLater(
      bad.query('SELECT 1'),
      throwsA(
        isA<MysqlException>().having(
          (MysqlException e) => e.code,
          'code',
          1045,
        ),
      ),
    );
    bad.close();
    await root.execute("DROP USER 'mn_sha2'@'%'");
    root.close();
  });

  test('TLS', () async {
    // the docker images use a self-signed certificate
    MysqlNativePool tls = MysqlNativePool.open(
      host: host,
      port: port,
      user: user,
      password: 'password',
      database: 'dox-framework',
      secure: true,
      verifyCertificate: false,
    );
    MysqlNativeResult r;
    try {
      r = await tls.query("SHOW SESSION STATUS LIKE 'Ssl_cipher'");
    } on MysqlException catch (e) {
      tls.close();
      if (e.message.contains('does not support TLS')) {
        markTestSkipped('the server has no TLS');
        return;
      }
      rethrow;
    }
    expect('${r.rows.single['Value']}', isNotEmpty);
    // pipelined over TLS, with a big answer
    List<MysqlNativeResult> all = await Future.wait(<Future<MysqlNativeResult>>[
      for (int i = 0; i < 100; i++)
        tls.query(
          'SELECT ? AS n, REPEAT(\'x\', 5000) AS t',
          parameters: <Object?>[i],
        ),
    ]);
    for (int i = 0; i < 100; i++) {
      expect(all[i].rows.single['n'], i);
    }
    tls.close();

    // the certificate is checked by default: self-signed fails
    MysqlNativePool checked = MysqlNativePool.open(
      host: host,
      port: port,
      user: user,
      password: 'password',
      database: 'dox-framework',
      secure: true,
    );
    await expectLater(
      checked.query('SELECT 1'),
      throwsA(
        isA<MysqlException>().having(
          (MysqlException e) => e.code,
          'code',
          isNull,
        ),
      ),
    );
    checked.close();
  });

  test('requests after close fail', () async {
    MysqlNativePool pool = open(cache: 24);
    await pool.query('SELECT 1');
    pool.close();
    await expectLater(
      pool.query('SELECT 1'),
      throwsA(
        isA<MysqlException>().having(
          (MysqlException e) => e.message,
          'message',
          contains('closed'),
        ),
      ),
    );
  });
}

Future<List<int>> _connectionIds(int n) async {
  MysqlNativePool pool = open(max: 3, cache: 30);
  List<MysqlNativeResult> all = await Future.wait(<Future<MysqlNativeResult>>[
    for (int i = 0; i < 200; i++)
      pool.query('SELECT CONNECTION_ID() AS id, SLEEP(0.001) AS s'),
  ]);
  return <int>[for (MysqlNativeResult r in all) r.rows.single['id'] as int];
}

Future<int> _busy() async {
  MysqlNativePool pool = open(max: 1, cache: 23);
  Stopwatch sw = Stopwatch()..start();
  int n = 0;
  while (sw.elapsed < const Duration(seconds: 3)) {
    await Future.wait(<Future<MysqlNativeResult>>[
      for (int i = 0; i < 50; i++)
        pool.query('SELECT ? AS n', parameters: <Object?>[i]),
    ]);
    n += 50;
  }
  return n;
}

Future<int> _fewQueries() async {
  MysqlNativePool pool = open(max: 1, cache: 23);
  int n = 0;
  for (int i = 0; i < 20; i++) {
    MysqlNativeResult r = await pool.query('SELECT 1 AS one');
    n += r.rows.single['one'] as int;
  }
  return n;
}

Future<void> _holdAndDie() async {
  MysqlNativePool pool = open(max: 2, cache: 29);
  int a = await pool.begin();
  int b = await pool.begin();
  await pool.query('SELECT 1', transaction: a);
  await pool.query('SELECT 1', transaction: b);
  // no commit, no close: the isolate ends here
}
