// Ships the native library of dox_mysql_native as a code asset.
//
// Where the library comes from, first match wins:
// 0. The `skip: true` user define: no library at all. `open` then throws.
//    In the root pubspec.yaml of the app:
//      hooks:
//        user_defines:
//          dox_mysql_native:
//            skip: true
// 1. The `lib_path` user define: a path to a ready library (for dev).
//    In the root pubspec.yaml of the app:
//      hooks:
//        user_defines:
//          dox_mysql_native:
//            lib_path: /path/to/libdox_mysql_native.so
// 2. A local build in `native/target/release` (for dev, same OS and CPU).
// 3. The ready-built library from the GitHub release, checked with sha256.
// 4. `cargo build` from source, when cargo is installed.
// Outside the pub cache (this repo, or a path or git dependency), 4 comes
// before 3, because the source may be newer than the release.
// When none works, no asset is added, and the build does not fail.
// `MysqlNativePool.isAvailable` is then false and says why.
import 'dart:io';

import 'package:code_assets/code_assets.dart';
import 'package:crypto/crypto.dart';
import 'package:hooks/hooks.dart';

const String _releases =
    'https://github.com/dartondox/dox-mysql-native/releases/download';

const int _attempts = 3;

/// Seconds to wait before the 2nd and the 3rd try.
const List<int> _waitSeconds = <int>[1, 3];

/// sha256 of each release asset. The release workflow
/// (.github/workflows/ci.yaml) fills it from `SHA256SUMS` of the release:
/// `dart run tool/native_hashes.dart v<version>`
const Map<String, String> _sha256 = <String, String>{
  'dox_mysql_native-linux-arm64.so':
      'edada01ed2f204ac50fe5ac7403afe753cea3e6fd72ef254561f08d9dac60dcf',
  'dox_mysql_native-linux-x64.so':
      '716eb23f7712b28dad25a9dd5e1c9a2c8faf5dff675e24e2c9e949223118b59c',
  'dox_mysql_native-macos-arm64.dylib':
      '4a3d83d70c022c7fbe963c69db466dd55f502b8e12e3747203988147b5d7f3d0',
  'dox_mysql_native-macos-x64.dylib':
      '52db088c3e0c93aa1bdb8784e7db88549964611d25d736a4bb5ac16ba0ec4c28',
  'dox_mysql_native-windows-x64.dll':
      '7ac4d35eb2dfc08c3cd612864724bd2a651453593228a0a0d42181fd74ebf2e6',
};

void main(List<String> args) async {
  await build(args, (BuildInput input, BuildOutputBuilder output) async {
    if (!input.config.buildCodeAssets) return;
    if (input.userDefines['skip'] == true) return; // no library, on purpose
    CodeConfig code = input.config.code;
    OS os = code.targetOS;
    Architecture arch = code.targetArchitecture;
    String fileName = os.dylibFileName('dox_mysql_native');
    bool host = os == OS.current && arch == Architecture.current;
    Uri native = input.packageRoot.resolve('native/');
    // From pub.dev the source matches the release, so download first. In the
    // repo, or as a path or git dependency, the source may be newer than the
    // release (eg. a new ABI), so build it with cargo first.
    bool published = input.packageRoot.pathSegments.contains('hosted');

    Uri? lib =
        _given(input) ??
        (host ? _local(native, fileName) : null) ??
        (published ? await _download(input, os, arch, fileName) : null) ??
        (host ? await _cargo(input, native, fileName, output) : null) ??
        (published ? null : await _download(input, os, arch, fileName));

    if (lib == null) {
      stderr.writeln(
        'dox_mysql_native: no native library for ${os.name}-${arch.name}.',
      );
      return;
    }
    output.dependencies.add(lib);
    output.assets.code.add(
      CodeAsset(
        package: input.packageName,
        name: 'src/bindings.dart',
        linkMode: DynamicLoadingBundled(),
        file: lib,
      ),
    );
  });
}

Uri? _given(BuildInput input) {
  String? path = input.userDefines['lib_path'] as String?;
  if (path == null || path.isEmpty) return null;
  File file = File(path);
  if (!file.existsSync()) {
    throw StateError('dox_mysql_native: $path does not exist');
  }
  return file.absolute.uri;
}

Uri? _local(Uri native, String fileName) {
  Uri lib = native.resolve('target/release/$fileName');
  return File.fromUri(lib).existsSync() ? lib : null;
}

Future<Uri?> _download(
  BuildInput input,
  OS os,
  Architecture arch,
  String fileName,
) async {
  String ext = fileName.substring(fileName.lastIndexOf('.'));
  String asset = 'dox_mysql_native-${os.name}-${arch.name}$ext';
  String? hash = _sha256[asset];
  if (hash == null) return null; // no release for this platform (yet)

  // the release tag has the package version, eg. dox_mysql_native-v3.0.0
  String pubspec = File.fromUri(input.packageRoot.resolve('pubspec.yaml'))
      .readAsStringSync();
  String version = RegExp(
    r'^version:\s*(\S+)',
    multiLine: true,
  ).firstMatch(pubspec)![1]!;
  File file = File.fromUri(input.outputDirectoryShared.resolve(asset));
  if (file.existsSync() && _sha(file.readAsBytesSync()) == hash) {
    return file.uri;
  }
  // GitHub sometimes answers 503 or 504. Try up to 3 times, with a short wait.
  for (int attempt = 1; attempt <= _attempts; attempt++) {
    if (attempt > 1) {
      await Future<void>.delayed(Duration(seconds: _waitSeconds[attempt - 2]));
    }
    String tried = '(attempt $attempt of $_attempts)';
    // a stalled download must not hang the build forever. Each try has one
    // time limit for the whole thing: connect, response head and body.
    DateTime deadline = DateTime.now().add(_tryLimit);
    HttpClient client = HttpClient()
      ..connectionTimeout = const Duration(seconds: 30);
    try {
      HttpClientRequest req = await client
          .getUrl(Uri.parse('$_releases/v$version/$asset'))
          .timeout(_left(deadline));
      HttpClientResponse res = await req.close().timeout(_left(deadline));
      if (res.statusCode != 200) {
        stderr.writeln(
          'dox_mysql_native: download $asset: ${res.statusCode} $tried',
        );
        if (shouldRetryStatus(res.statusCode)) continue;
        return null;
      }
      List<int> bytes = <int>[
        for (List<int> chunk in await res.toList().timeout(_left(deadline)))
          ...chunk,
      ];
      if (_sha(bytes) != hash) {
        // the body may be an error page, so try again
        stderr.writeln(
          'dox_mysql_native: sha256 of $asset does not match $tried',
        );
        continue;
      }
      file.parent.createSync(recursive: true);
      file.writeAsBytesSync(bytes);
      return file.uri;
    } on Exception catch (error) {
      stderr.writeln('dox_mysql_native: download $asset failed: $error $tried');
    } finally {
      client.close(force: true);
    }
  }
  return null;
}

/// The longest time one download try may take, from connect to the last byte.
const Duration _tryLimit = Duration(seconds: 60);

/// Time left until [deadline]. Zero or less makes the next wait time out now.
Duration _left(DateTime deadline) => deadline.difference(DateTime.now());

/// True when a new try may help: a server error (5xx) or "too many requests".
/// A 404 means the asset does not exist, so it is not tried again.
bool shouldRetryStatus(int status) => status == 429 || status >= 500;

String _sha(List<int> bytes) => sha256.convert(bytes).toString();

Future<Uri?> _cargo(
  BuildInput input,
  Uri native,
  String fileName,
  BuildOutputBuilder output,
) async {
  String home =
      Platform.environment['HOME'] ?? Platform.environment['USERPROFILE'] ?? '';
  String cargo = 'cargo';
  File inHome = File('$home/.cargo/bin/cargo');
  if (home.isNotEmpty && inHome.existsSync()) cargo = inHome.path;

  Uri target = input.outputDirectoryShared.resolve('cargo/');
  try {
    ProcessResult result = await Process.run(cargo, <String>[
      'build',
      '--release',
      '--manifest-path',
      File.fromUri(native.resolve('Cargo.toml')).path,
      '--target-dir',
      Directory.fromUri(target).path,
    ]);
    if (result.exitCode != 0) {
      stderr.writeln('dox_mysql_native: cargo build failed\n${result.stderr}');
      return null;
    }
  } on ProcessException {
    return null; // no cargo
  }
  output.dependencies.add(native.resolve('Cargo.toml'));
  for (String file in <String>['lib.rs', 'link.rs', 'proto.rs', 'tls.rs']) {
    output.dependencies.add(native.resolve('src/$file'));
  }
  Uri lib = target.resolve('release/$fileName');
  return File.fromUri(lib).existsSync() ? lib : null;
}
