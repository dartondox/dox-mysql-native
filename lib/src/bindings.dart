import 'dart:ffi';

/// FFI calls into the Rust library. The build hook (hook/build.dart) ships
/// the library as the code asset of this file.

/// The ABI version the Dart side expects. Must match `ABI_VERSION` in Rust.
const int abiVersion = 2;

@Native<Int32 Function()>(symbol: 'dox_mysql_version', isLeaf: true)
external int doxMysqlVersion();

@Native<Int32 Function(Pointer<Void>)>(symbol: 'dox_mysql_init')
external int doxMysqlInit(Pointer<Void> dlData);

@Native<Int64 Function(Pointer<Uint8>, Int64, Int32)>(
  symbol: 'dox_mysql_connect',
  isLeaf: true,
)
external int doxMysqlConnect(Pointer<Uint8> url, int length, int threads);

@Native<Void Function(Int64)>(symbol: 'dox_mysql_close', isLeaf: true)
external void doxMysqlClose(int pool);

@Native<Int64 Function(Int64)>(symbol: 'dox_mysql_client', isLeaf: true)
external int doxMysqlClient(int sendPort);

/// Sends a batch of requests. Returns 1 when results are ready already
/// (call [doxMysqlRun]).
@Native<Int64 Function(Int64, Pointer<Uint8>, Int64)>(
  symbol: 'dox_mysql_submit',
  isLeaf: true,
)
external int doxMysqlSubmit(int client, Pointer<Uint8> data, int length);

/// Writes the results that are ready into [buffer]. Returns the byte
/// count, or -n when the buffer needs n bytes.
@Native<Int64 Function(Int64, Pointer<Uint8>, Int64)>(
  symbol: 'dox_mysql_run',
  isLeaf: true,
)
external int doxMysqlRun(int client, Pointer<Uint8> buffer, int capacity);
