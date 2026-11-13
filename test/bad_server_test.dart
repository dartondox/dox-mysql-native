import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:dox_mysql_native/dox_mysql_native.dart';
import 'package:dox_mysql_native/src/pool.dart' show debugPoisonColumns;
import 'package:test/test.dart';

/// A tiny server that needs no database: it accepts any login and answers
/// each query with the same result, built from bad data. It checks that bad
/// bytes from a server give a value or an error for this request, never a
/// request that does not end.

Uint8List _packet(int seq, List<int> payload) => Uint8List.fromList(<int>[
  payload.length & 0xFF,
  (payload.length >> 8) & 0xFF,
  (payload.length >> 16) & 0xFF,
  seq,
  ...payload,
]);

/// a length-encoded string (shorter than 251 bytes)
List<int> _str(List<int> b) => <int>[b.length, ...b];

/// a column definition with this name and type
List<int> _column(String name, int type) => <int>[
  ..._str('def'.codeUnits),
  ..._str('db'.codeUnits),
  ..._str('t'.codeUnits),
  ..._str('t'.codeUnits),
  ..._str(name.codeUnits),
  ..._str(name.codeUnits),
  0x0c,
  45, 0, // charset
  0, 0, 0, 0, // length
  type,
  0, 0, // flags
  0, 0,
];

/// Starts the server. [rows] are the packets after the column count and
/// the column definitions.
Future<ServerSocket> _serve(
  List<List<int>> Function() rows, {
  List<String>? queries,
}) async {
  ServerSocket server = await ServerSocket.bind(
    InternetAddress.loopbackIPv4,
    0,
  );
  server.listen((Socket s) {
    s.add(
      _packet(0, <int>[
        10,
        ...'8.4.0'.codeUnits,
        0,
        7, 0, 0, 0, // connection id
        ...'abcdefgh'.codeUnits,
        0,
        0x00, 0x80, // capabilities, low
        45,
        2, 0, // status
        0x08, 0x00, // capabilities, high (plugin auth)
        21,
        ...List<int>.filled(10, 0),
        ...'ijklmnopqrst'.codeUnits,
        0,
        ...'mysql_native_password'.codeUnits,
        0,
      ]),
    );
    // a client may close while an answer is written (eg. a kill
    // connection that sends no KILL)
    s.done.then((_) {}, onError: (Object _) {});
    List<int> buf = <int>[];
    bool login = true;
    s.listen((List<int> data) {
      buf.addAll(data);
      while (buf.length >= 4) {
        int len = buf[0] | buf[1] << 8 | buf[2] << 16;
        if (buf.length < 4 + len) return;
        List<int> payload = buf.sublist(4, 4 + len);
        buf.removeRange(0, 4 + len);
        const List<int> ok = <int>[0, 0, 0, 2, 0, 0, 0];
        if (login) {
          login = false;
          s.add(_packet(2, ok));
        } else if (payload[0] == 0x03) {
          // COM_QUERY: one column of text and one of JSON
          String sql = String.fromCharCodes(payload.sublist(1));
          queries?.add(sql);
          int seq = 1;
          s.add(_packet(seq++, <int>[2]));
          s.add(_packet(seq++, _column('t', 253)));
          s.add(_packet(seq++, _column('j', 245)));
          for (List<int> row in rows()) {
            s.add(_packet(seq++, row));
          }
          // a query with HOLD never ends, like a long query
          if (!sql.contains('HOLD')) {
            s.add(_packet(seq++, <int>[0xFE, 0, 0, 2, 0, 0, 0]));
          }
        } else if (payload[0] == 0x1f) {
          s.add(_packet(1, ok)); // COM_RESET_CONNECTION
        } else if (payload[0] == 0x01) {
          s.destroy(); // COM_QUIT
        }
      }
    }, onError: (Object _) {});
  });
  return server;
}

void main() {
  test(
    'bad UTF-8 and bad JSON from a server do not stop the request',
    () async {
      ServerSocket server = await _serve(
        () => <List<int>>[
          // 'a', a byte that is not UTF-8, 'b'. And JSON that is not JSON.
          <int>[
            ..._str(<int>[0x61, 0xFF, 0x62]),
            ..._str('{bad'.codeUnits),
          ],
          <int>[..._str('ok'.codeUnits), ..._str('{"a":1}'.codeUnits)],
        ],
      );
      MysqlNativePool pool = MysqlNativePool.fromUrl(
        'mysql://u:pw@127.0.0.1:${server.port}/db',
      );
      MysqlNativeResult r = await pool
          .query('SELECT 1')
          .timeout(const Duration(seconds: 10));
      expect(r.rows[0]['t'], 'a�b');
      expect(r.rows[0]['j'], '{bad');
      expect(r.rows[1]['t'], 'ok');
      expect(r.rows[1]['j'], <String, dynamic>{'a': 1});
      // the pool still works after it
      MysqlNativeResult again = await pool
          .query('SELECT 2')
          .timeout(const Duration(seconds: 10));
      expect(again.rows, hasLength(2));
      pool.close();
      await server.close();
    },
  );

  group('a record Dart can not read', () {
    // the column bytes of the result of _serve (t and j), with no names
    // behind them, so the rows can not be read
    const List<int> columnBytes = <int>[1, 0, 0x74, 1, 0, 0x6a];
    List<List<int>> rows() => <List<int>>[
      for (int i = 0; i < 4; i++)
        <int>[..._str('x'.codeUnits), ..._str('{}'.codeUnits)],
    ];

    test('fails only that request, and the pool still works', () async {
      ServerSocket server = await _serve(rows);
      MysqlNativePool pool = MysqlNativePool.fromUrl(
        'mysql://u:pw@127.0.0.1:${server.port}/db',
      );
      debugPoisonColumns('SELECT 1', columnBytes);
      await expectLater(
        pool.query('SELECT 1').timeout(const Duration(seconds: 10)),
        throwsA(
          isA<MysqlException>().having(
            (MysqlException e) => e.message,
            'message',
            startsWith('bad result:'),
          ),
        ),
      );
      MysqlNativeResult r = await pool
          .query('SELECT 2')
          .timeout(const Duration(seconds: 10));
      expect(r.rows, hasLength(4));
      pool.close();
      await server.close();
    });

    test('a stream gets the error, closes and is cancelled', () async {
      List<String> queries = <String>[];
      ServerSocket server = await _serve(rows, queries: queries);
      MysqlNativePool pool = MysqlNativePool.fromUrl(
        'mysql://u:pw@127.0.0.1:${server.port}/db',
      );
      debugPoisonColumns('SELECT 3 /* HOLD */', columnBytes);
      List<Object> events = <Object>[];
      await pool
          .stream('SELECT 3 /* HOLD */', chunkSize: 1)
          .map<Object>((MysqlNativeResult r) => r)
          .handleError((Object e) => events.add(e))
          .toList()
          .timeout(const Duration(seconds: 10));
      expect(events.single, isA<MysqlException>());
      expect(
        (events.single as MysqlException).message,
        startsWith('bad result:'),
      );
      // the cancel reached Rust: it stops the query on the server with
      // KILL QUERY (the id of the connection in the greeting is 7)
      for (int i = 0; i < 100 && !queries.contains('KILL QUERY 7'); i++) {
        await Future<void>.delayed(const Duration(milliseconds: 50));
      }
      expect(queries, contains('KILL QUERY 7'));
      // the pool gives a connection again
      MysqlNativeResult r = await pool
          .query('SELECT 4')
          .timeout(const Duration(seconds: 10));
      expect(r.rows, hasLength(4));
      pool.close();
      await server.close();
    });
  });

  test('no KILL when the kill connection reaches another server', () async {
    // each query gets a new first value, so the server name of the kill
    // connection is not the one of its target: like a load balancer that
    // sends each connection to another server
    int n = 0;
    List<String> queries = <String>[];
    ServerSocket server = await _serve(
      () => <List<int>>[
        <int>[..._str('server-${n++}'.codeUnits), ..._str('{}'.codeUnits)],
      ],
      queries: queries,
    );
    MysqlNativePool pool = MysqlNativePool.fromUrl(
      'mysql://u:pw@127.0.0.1:${server.port}/db',
    );
    Completer<void> first = Completer<void>();
    StreamSubscription<MysqlNativeResult> sub = pool
        .stream('SELECT 5 /* HOLD */', chunkSize: 1)
        .listen((MysqlNativeResult _) {
          if (!first.isCompleted) first.complete();
        }, onError: (Object _) {});
    await first.future.timeout(const Duration(seconds: 10));
    unawaited(sub.cancel());
    // the kill connection logs in and asks the server name
    bool asked() =>
        queries.where((String q) => q.contains('@@server_uuid')).length >= 2;
    for (int i = 0; i < 100 && !asked(); i++) {
      await Future<void>.delayed(const Duration(milliseconds: 50));
    }
    expect(asked(), isTrue);
    await Future<void>.delayed(const Duration(milliseconds: 300));
    expect(queries.where((String q) => q.startsWith('KILL')), isEmpty);
    pool.close();
    await server.close();
  });

  test('a cancel with no KILL (another server) returns at once', () async {
    int n = 0;
    List<String> queries = <String>[];
    ServerSocket server = await _serve(
      () => <List<int>>[
        <int>[..._str('server-${n++}'.codeUnits), ..._str('{}'.codeUnits)],
      ],
      queries: queries,
    );
    MysqlNativePool pool = MysqlNativePool.fromUrl(
      'mysql://u:pw@127.0.0.1:${server.port}/db',
    );
    MysqlCancelToken token = MysqlCancelToken();
    // a query that never ends on the server
    Future<MysqlNativeResult> slow = pool.query(
      'SELECT 6 /* HOLD */',
      cancelToken: token,
    );
    for (int i = 0; i < 100 && !queries.contains('SELECT 6 /* HOLD */'); i++) {
      await Future<void>.delayed(const Duration(milliseconds: 20));
    }
    token.cancel();
    await expectLater(
      slow.timeout(const Duration(seconds: 5)),
      throwsA(isA<MysqlQueryCancelledException>()),
    );
    expect(queries.where((String q) => q.startsWith('KILL')), isEmpty);
    pool.close();
    await server.close();
  });

  test('a timeout with no KILL (another server) fails on time', () async {
    int n = 0;
    List<String> queries = <String>[];
    ServerSocket server = await _serve(
      () => <List<int>>[
        <int>[..._str('server-${n++}'.codeUnits), ..._str('{}'.codeUnits)],
      ],
      queries: queries,
    );
    MysqlNativePool pool = MysqlNativePool.fromUrl(
      'mysql://u:pw@127.0.0.1:${server.port}/db?request_timeout_ms=1000',
    );
    Stopwatch watch = Stopwatch()..start();
    // a query that never ends on the server
    await expectLater(
      pool.query('SELECT 7 /* HOLD */').timeout(const Duration(seconds: 5)),
      throwsA(
        isA<MysqlException>().having(
          (MysqlException e) => e.message,
          'message',
          contains('timeout, no answer in 1 s'),
        ),
      ),
    );
    expect(watch.elapsed, lessThan(const Duration(seconds: 3)));
    // the kill connection logs in and asks the server name
    bool asked() =>
        queries.where((String q) => q.contains('@@server_uuid')).length >= 2;
    for (int i = 0; i < 100 && !asked(); i++) {
      await Future<void>.delayed(const Duration(milliseconds: 50));
    }
    expect(asked(), isTrue);
    await Future<void>.delayed(const Duration(milliseconds: 300));
    expect(queries.where((String q) => q.startsWith('KILL')), isEmpty);
    pool.close();
    await server.close();
  });
}
