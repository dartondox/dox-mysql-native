// Stress test: finds requests that never finish (a stall or a deadlock).
//
// Many isolates send many queries at once: single queries, bursts of 20,
// transactions, transactions used from another isolate, new SQL text
// (statement cache churn) and errors. A killer ends random connections
// with KILL <id>. Every request that takes over 2 s is a hang. Errors are
// fine, hangs are not.
//
//   dart run tool/stress.dart [seconds] [isolates] [workers per isolate]
//
// DB_HOST / DB_PORT / DB_USER pick the server (password `password`,
// database `dox-framework`, like the tests). To find what causes a hang:
// NO_KILL=1 turns the killer off, NO_CROSS=1 the cross-isolate
// transactions.
import 'dart:async';
import 'dart:io';
import 'dart:isolate';
import 'dart:math';

import 'package:dox_mysql_native/dox_mysql_native.dart';

String host = Platform.environment['DB_HOST'] ?? '127.0.0.1';
int port = int.parse(Platform.environment['DB_PORT'] ?? '3306');
String user = Platform.environment['DB_USER'] ?? 'root';
bool noKill = Platform.environment['NO_KILL'] == '1';
bool noCross = Platform.environment['NO_CROSS'] == '1';

/// [cache] also makes the settings (and so the pool) unique.
MysqlNativePool open({int max = 4, int cache = 32}) => MysqlNativePool.open(
  host: host,
  port: port,
  user: user,
  password: 'password',
  database: 'dox-framework',
  maxConnections: max,
  statementCacheSize: cache,
);

Future<void> main(List<String> args) async {
  int seconds = args.isNotEmpty ? int.parse(args[0]) : 60;
  int isolates = args.length > 1 ? int.parse(args[1]) : 4;
  int workers = args.length > 2 ? int.parse(args[2]) : 64;

  MysqlNativePool setup = open();
  await setup.execute('DROP TABLE IF EXISTS stress_world');
  await setup.execute(
    'CREATE TABLE stress_world (id INT PRIMARY KEY, n INT NOT NULL)',
  );
  // a recursive CTE stops at 1000 steps by default: 10 blocks of 1000
  for (int k = 0; k < 10; k++) {
    await setup.execute(
      'INSERT INTO stress_world (id, n) WITH RECURSIVE s(i) AS '
      '(SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 1000) '
      'SELECT i + ${k * 1000}, i FROM s',
    );
  }

  // runs transactions that other isolates began: query + commit
  ReceivePort crossPort = ReceivePort();
  SendPort cross = crossPort.sendPort;
  int handle = setup.handle;
  Isolate crossIsolate = await Isolate.spawn(_cross, <Object>[cross, handle]);
  SendPort toCross = await crossPort.first as SendPort;

  ReceivePort reports = ReceivePort();
  SendPort to = reports.sendPort;
  List<Future<Object?>> done = <Future<Object?>>[];
  for (int i = 0; i < isolates; i++) {
    // half the isolates share the pool of the main isolate
    int? shared = i.isEven ? setup.handle : null;
    done.add(
      Isolate.run(() => _isolate(i, shared, seconds, workers, to, toCross)),
    );
  }
  int hangs = 0;
  reports.listen((Object? m) {
    stdout.writeln(m);
    if ('$m'.startsWith('HANG')) hangs++;
  });

  // the killer: ends one random connection of the test every 100-500 ms.
  // Its own pool has one connection, and it never kills that one.
  MysqlNativePool killer = open(max: 1, cache: 7);
  Random rnd = Random();
  int kills = 0;
  Stopwatch clock = Stopwatch()..start();
  while (clock.elapsed.inSeconds < seconds) {
    await Future<void>.delayed(Duration(milliseconds: 100 + rnd.nextInt(400)));
    if (noKill) continue;
    try {
      MysqlNativeResult r = await killer
          .query(
            'SELECT ID AS id FROM information_schema.PROCESSLIST '
            "WHERE DB = 'dox-framework' AND ID <> CONNECTION_ID() "
            'ORDER BY RAND() LIMIT 1',
          )
          .timeout(const Duration(seconds: 2));
      if (r.rows.isEmpty) continue;
      await killer
          .execute('KILL ${r.rows.single['id']}')
          .timeout(const Duration(seconds: 2));
      kills++;
    } catch (_) {
      // the connection may be gone already
    }
  }
  List<int> totals = <int>[0, 0];
  for (Object? r in await Future.wait(done)) {
    List<int> t = r as List<int>;
    totals[0] += t[0];
    totals[1] += t[1];
  }
  await Future<void>.delayed(const Duration(milliseconds: 100));
  reports.close();
  crossIsolate.kill();
  crossPort.close();
  stdout.writeln(
    'done: ${totals[0]} requests, ${totals[1]} errors, $kills kills, '
    '$hangs hangs',
  );
  exit(hangs == 0 ? 0 : 1);
}

/// The isolate that finishes transactions of other isolates. Gets
/// `[tx, id, reply port]`, answers `null` or the error text.
Future<void> _cross(List<Object> args) async {
  SendPort main = args[0] as SendPort;
  MysqlNativePool pool = MysqlNativePool.fromHandle(args[1] as int);
  ReceivePort jobs = ReceivePort();
  main.send(jobs.sendPort);
  // each job runs on its own: one that waits for a row lock must not
  // hold up the commit of the transaction that has the lock
  jobs.listen((Object? m) async {
    List<Object?> job = m as List<Object?>;
    int tx = job[0] as int;
    SendPort reply = job[2] as SendPort;
    try {
      await pool.execute(
        'UPDATE stress_world SET n = n + 1 WHERE id = ?',
        parameters: <Object?>[job[1]],
        transaction: tx,
      );
      await pool.commit(tx);
      reply.send(null);
    } catch (e) {
      reply.send('$e');
    }
  });
}

Future<List<int>> _isolate(
  int index,
  int? shared,
  int seconds,
  int workers,
  SendPort report,
  SendPort toCross,
) async {
  MysqlNativePool pool = shared != null
      ? MysqlNativePool.fromHandle(shared)
      : open(cache: 8);
  Random rnd = Random(index);
  int requests = 0;
  int errors = 0;
  DateTime end = DateTime.now().add(Duration(seconds: seconds));

  /// runs one request and reports it when it takes over 2 s
  Future<T> watch<T>(String what, Future<T> Function() run) async {
    requests++;
    Stopwatch s = Stopwatch()..start();
    bool late = false;
    Timer t = Timer(const Duration(seconds: 2), () {
      late = true;
      report.send('HANG isolate $index: $what (over 2 s)');
    });
    try {
      return await run();
    } finally {
      t.cancel();
      if (late) {
        report.send('  late finish isolate $index: $what after ${s.elapsed}');
      }
    }
  }

  int id() => 1 + rnd.nextInt(10000);

  Future<void> find() => watch(
    'find',
    () => pool.query(
      'SELECT id, n FROM stress_world WHERE id = @id',
      parameters: <String, dynamic>{'id': id()},
    ),
  );

  Future<void> one() async {
    switch (rnd.nextInt(20)) {
      case < 9:
        await find();
      case < 14:
        for (int i = 0; i < 20; i++) {
          await find();
        }
      case < 16:
        int tx = await watch('begin', pool.begin);
        try {
          for (int i = 0; i < 3; i++) {
            await watch(
              'tx query',
              () => pool.execute(
                'UPDATE stress_world SET n = n + 1 WHERE id = @id',
                parameters: <String, dynamic>{'id': id()},
                transaction: tx,
              ),
            );
          }
          await watch('commit', () => pool.commit(tx));
        } catch (_) {
          await watch('rollback', () => pool.rollback(tx));
          rethrow;
        }
      case < 17 when !noCross:
        // begun here, finished by another isolate
        int tx = await watch('begin', pool.begin);
        await watch(
          'tx query',
          () => pool.execute(
            'UPDATE stress_world SET n = n + 1 WHERE id = ?',
            parameters: <Object?>[id()],
            transaction: tx,
          ),
        );
        String? error = await watch('cross commit', () async {
          ReceivePort reply = ReceivePort();
          toCross.send(<Object?>[tx, id(), reply.sendPort]);
          Object? answer = await reply.first;
          return answer as String?;
        });
        if (error != null) throw StateError(error);
      case < 19:
        // new SQL text: statement cache churn
        int k = rnd.nextInt(2000);
        await watch(
          'churn',
          () => pool.query('SELECT $k + ? AS v', parameters: <Object?>[k]),
        );
      case < 20 when rnd.nextBool():
        await watch('error', () => pool.query('SELECT * FROM nope_missing'));
      default:
        // requests after the end of a transaction
        int tx = await watch('begin', pool.begin);
        await Future.wait(
          <Future<void>>[
            watch('rollback', () => pool.rollback(tx)),
            watch('2nd rollback', () => pool.rollback(tx)),
            watch('late query', () => pool.query('SELECT 1', transaction: tx)),
          ].map((Future<Object?> f) => f.catchError((Object _) => null)),
        );
    }
  }

  Future<void> worker() async {
    while (DateTime.now().isBefore(end)) {
      try {
        await one();
      } catch (_) {
        errors++;
      }
    }
  }

  await Future.wait(<Future<void>>[for (int i = 0; i < workers; i++) worker()]);
  if (shared == null) pool.close();
  return <int>[requests, errors];
}
