//! Native MySQL and MariaDB driver for Dart (dox_mysql_native).
//!
//! A small MySQL client in Rust runs on the thread of each Dart isolate
//! (see `link.rs`). There is no runtime thread in between. Dart only
//! encodes requests and decodes results.
//!
//! - Pools live in a registry, so all isolates can share one pool (same
//!   URL = same pool). The max connections count for the whole process.
//! - Each isolate has one link: one mio poll and the connections it uses.
//!   Requests go in batches with one leaf FFI call (`dox_mysql_submit`),
//!   which writes them to the sockets at once. `dox_mysql_run` (a leaf
//!   call) reads the answers that are there and writes the result records
//!   into a Dart buffer. It never waits.
//! - Each connection keeps a prepared statement cache with an LRU limit.
//! - Each request has [TIMEOUT]. A transaction that gets no request for
//!   [TIMEOUT] is rolled back, so its connection goes back to the pool.
//!
//! Request record (little endian, 32 byte head): u64 id, u64 pool, u64 tx,
//! u8 op, 3 bytes reserved, u32 sql_len. Then u32 param count, sql, params.
//! A param is a u8 tag and a value: 0 null, 1 i64, 2 f64,
//! 3 text (u32 len + utf8), 4 bytes (u32 len + bytes).
//!
//! Result record (16 byte head): u64 id, u8 status (0 ok, 1 error),
//! 3 bytes reserved, u32 body_len. Then the body.
//! - ok: u64 affected rows, u64 insert id (the tx handle for `begin`),
//!   u32 column count, u32 row count, the names (u16 len + utf8), then each
//!   cell as a u8 tag and a value: 0 null, 1 i64, 2 f64, 3 text,
//!   4 bytes, 5 date (i64 microseconds since epoch, UTC), 6 bool (u8),
//!   7 json text, 8 long text (TEXT columns, MariaDB keeps JSON there).
//!   Text, bytes and json are a u32 len and the bytes.
//! - error: u16 server error code (0 for a client error), u8 kind
//!   (0 server, 1 client), 5 bytes sqlstate, then the message (utf8).
use std::collections::{HashMap, VecDeque};
use std::ffi::{CStr, c_char, c_int, c_void};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use slab::Slab;

mod link;
mod proto;
mod tls;

use proto::Value;

/// Bump when the FFI functions or the record layout change.
const ABI_VERSION: i32 = 2;

const OP_QUERY: u8 = 0;
const OP_EXECUTE: u8 = 1;
const OP_BEGIN: u8 = 2;
const OP_COMMIT: u8 = 3;
const OP_ROLLBACK: u8 = 4;

const T_NULL: u8 = 0;
const T_INT: u8 = 1;
const T_DOUBLE: u8 = 2;
const T_TEXT: u8 = 3;
const T_BYTES: u8 = 4;
const T_DATE: u8 = 5;
const T_BOOL: u8 = 6;
const T_JSON: u8 = 7;
const T_LONG_TEXT: u8 = 8;

/// charset id of binary data (BLOB, VARBINARY, BINARY)
const BINARY: u16 = 63;
/// column flag of ENUM
const ENUM_FLAG: u16 = 256;

/// Size of the head of a result record.
const RES_HEAD: usize = 16;

/// Longest wait for one request (pool slot, connect and query). Over it
/// the request fails with a clear error, so Dart never waits forever. The
/// same as the native Postgres driver.
/// ponytail: fixed. Make it a pool option when an app needs another value.
const TIMEOUT: Duration = Duration::from_secs(300);
const TIMED_OUT: &str = "dox_mysql_native: timeout, no answer in 300 s";
const POOL_CLOSED: &str = "the pool is closed";

// ---- Dart API DL -------------------------------------------------------
// `NativeApi.initializeApiDLData` points to this table. We only need
// `Dart_PostCObject`, so we read it here instead of compiling dart_api_dl.c.

#[repr(C)]
struct ApiEntry {
    name: *const c_char,
    function: *const c_void,
}

#[repr(C)]
struct DartApi {
    major: c_int,
    minor: c_int,
    functions: *const ApiEntry,
}

/// `Dart_CObject` for a `kTypedData` (Uint8) value. 64-bit layout.
#[repr(C)]
struct DartCObject {
    ty: i32,
    _pad: i32,
    td_type: i32,
    _pad2: i32,
    length: isize,
    values: *const u8,
}

type PostFn = unsafe extern "C" fn(i64, *mut DartCObject) -> bool;
static POST: OnceLock<PostFn> = OnceLock::new();

/// Posts a few bytes to a Dart port (a copy). False when the port is
/// closed. Only the idle helper threads post: never a leaf call.
fn post(port: i64, data: &[u8]) -> bool {
    let Some(f) = POST.get() else { return false };
    let mut obj = DartCObject {
        ty: 7, // Dart_CObject_kTypedData
        _pad: 0,
        td_type: 2, // Dart_TypedData_kUint8
        _pad2: 0,
        length: data.len() as isize,
        values: data.as_ptr(),
    };
    unsafe { f(port, &mut obj) }
}

// ---- options -----------------------------------------------------------

/// TLS settings from the URL.
#[derive(Clone, Copy, PartialEq, Debug)]
struct TlsOpts {
    verify_ca: bool,
    verify_identity: bool,
    built_in_roots: bool,
}

/// What a `mysql://` URL says, with the defaults of mysql_async.
#[derive(Debug)]
struct Opts {
    host: String,
    port: u16,
    user: String,
    pass: String,
    db: Option<String>,
    /// a Unix socket file instead of TCP
    socket: Option<String>,
    max: usize,
    stmt_cache: usize,
    /// a connection with no IO for this long is closed, not used
    /// (`conn_ttl`, else `wait_timeout`, like mysql_async)
    conn_ttl: Option<Duration>,
    /// a connection older than this (plus a random part of `abs_jitter`)
    /// is closed once it has no command
    abs_ttl: Option<Duration>,
    abs_jitter: Option<Duration>,
    /// an idle connection over `min` closes after this long. 0: never.
    inactive_ttl: Duration,
    min: usize,
    /// COM_RESET_CONNECTION when a connection has no command left
    reset: bool,
    keepalive: Option<Duration>,
    tls: Option<TlsOpts>,
    found_rows: bool,
    nodelay: bool,
    cleartext: bool,
}

/// Reads a `mysql://user:password@host:port/db?options` URL. It takes the
/// options of mysql_async (see README.md), so old URLs still work. None:
/// a bad URL or an unknown option, like mysql_async.
fn parse_url(text: &str) -> Option<Opts> {
    let url = url::Url::parse(text).ok()?;
    if url.scheme() != "mysql" || url.cannot_be_a_base() {
        return None;
    }
    let decode = |s: &str| {
        percent_encoding::percent_decode_str(s)
            .decode_utf8_lossy()
            .into_owned()
    };
    let host = match url.host()? {
        url::Host::Ipv6(a) => a.to_string(),
        h => h.to_string(),
    };
    let db = url
        .path_segments()
        .and_then(|mut s| s.next())
        .map(decode)
        .filter(|d| !d.is_empty());
    let mut o = Opts {
        host,
        port: url.port().unwrap_or(3306),
        user: decode(url.username()),
        pass: decode(url.password().unwrap_or("")),
        db,
        socket: None,
        max: 100,
        stmt_cache: 32,
        conn_ttl: None,
        abs_ttl: None,
        abs_jitter: None,
        inactive_ttl: Duration::ZERO,
        min: 10,
        reset: true,
        keepalive: None,
        tls: None,
        found_rows: false,
        nodelay: true,
        cleartext: false,
    };
    let mut wait_timeout = None;
    let mut tls = TlsOpts {
        verify_ca: true,
        verify_identity: true,
        built_in_roots: true,
    };
    let mut require_ssl = false;
    for (k, v) in url.query_pairs() {
        let flag = || v.parse::<bool>().ok();
        let num = || v.parse::<u64>().ok();
        match &*k {
            "pool_min" => o.min = num()? as usize,
            "pool_max" => o.max = num()? as usize,
            "stmt_cache_size" => o.stmt_cache = num()? as usize,
            "conn_ttl" => o.conn_ttl = Some(Duration::from_secs(num()?)),
            "socket" => o.socket = Some(v.into_owned()),
            "require_ssl" => require_ssl = flag()?,
            "verify_ca" => tls.verify_ca = flag()?,
            "verify_identity" => tls.verify_identity = flag()?,
            "built_in_roots" => tls.built_in_roots = flag()?,
            "client_found_rows" => o.found_rows = flag()?,
            "tcp_nodelay" => o.nodelay = flag()?,
            "enable_cleartext_plugin" => o.cleartext = flag()?,
            "reset_connection" => o.reset = flag()?,
            "wait_timeout" => wait_timeout = Some(Duration::from_secs(num()?)),
            "abs_conn_ttl" => o.abs_ttl = Some(Duration::from_secs(num()?)),
            "abs_conn_ttl_jitter" => o.abs_jitter = Some(Duration::from_secs(num()?)),
            "inactive_connection_ttl" => o.inactive_ttl = Duration::from_secs(num()?),
            "tcp_keepalive" => o.keepalive = Some(Duration::from_millis(num()?)),
            // known to mysql_async, not used here (see README.md)
            "prefer_socket" | "secure_auth" => {
                flag()?;
            }
            "ttl_check_interval" | "max_allowed_packet" => {
                num()?;
            }
            "compression" => {}
            _ => return None,
        }
    }
    if o.max == 0 || o.min > o.max {
        return None;
    }
    o.conn_ttl = o.conn_ttl.or(wait_timeout);
    if require_ssl {
        o.tls = Some(tls);
    }
    Some(o)
}

// ---- pools -------------------------------------------------------------

/// A pool, shared by all isolates. The connections live in the links of
/// the isolates that use them. A link gives back a connection with no
/// command to `idle` when another isolate waits for one.
struct Shared {
    /// a number that is never used again, so a link can tell two pools
    /// apart that got the same handle
    id: u64,
    url: String,
    opts: Opts,
    /// None without TLS
    tls: Option<Arc<rustls::ClientConfig>>,
    /// the addresses of the host, looked up when the pool opens and again
    /// on a helper thread after a connect fails (so the isolate never
    /// waits for DNS)
    addrs: Mutex<Vec<std::net::SocketAddr>>,
    resolving: AtomicBool,
    state: Mutex<PoolState>,
    /// how many links wait for a connection (`state.waiters.len()`)
    waiting: AtomicUsize,
    closed: AtomicBool,
}

#[derive(Default)]
struct PoolState {
    /// connections of this pool in the whole process (also the ones that
    /// connect, and the ones in `idle`)
    total: usize,
    /// connections no link uses, with no command
    idle: Vec<link::Conn>,
    /// links that wait for a connection, first come first served
    waiters: VecDeque<Weak<link::Link>>,
    /// links that hold connections of this pool, and how many: they give
    /// one back when another link waits
    holders: HashMap<u64, (Weak<link::Link>, usize)>,
}

struct PoolEntry {
    shared: Arc<Shared>,
    /// how many opens use this pool. The last close closes it.
    refs: usize,
}

static POOLS: Mutex<Slab<PoolEntry>> = Mutex::new(Slab::new());
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// The pool of a handle (slab key + 1, so 0 is never a handle).
fn pool_of(handle: u64) -> Option<Arc<Shared>> {
    let key = handle.checked_sub(1)? as usize;
    POOLS.lock().unwrap().get(key).map(|e| e.shared.clone())
}

// ---- requests ----------------------------------------------------------

#[derive(Clone, PartialEq, Debug)]
enum Param {
    Null,
    Int(i64),
    Double(f64),
    /// text and bytes, both sent as VAR_STRING like mysql_async did
    Bytes(Vec<u8>),
}

struct Request {
    id: u64,
    pool: u64,
    tx: u64,
    op: u8,
    sql: String,
    params: Vec<Param>,
}

/// Reads little endian values. Every read checks the length.
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        if self.0.len() < n {
            return None;
        }
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Some(a)
    }
    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }
    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }
    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }
}

fn read_request(r: &mut Reader) -> Option<Request> {
    let id = r.u64()?;
    let pool = r.u64()?;
    let tx = r.u64()?;
    let op = r.u8()?;
    r.take(3)?;
    let sql_len = r.u32()? as usize;
    let count = r.u32()? as usize;
    let sql = String::from_utf8_lossy(r.take(sql_len)?).into_owned();
    let mut params = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        params.push(match r.u8()? {
            T_NULL => Param::Null,
            T_INT => Param::Int(r.u64()? as i64),
            T_DOUBLE => Param::Double(f64::from_bits(r.u64()?)),
            _ => {
                let len = r.u32()? as usize;
                Param::Bytes(r.take(len)?.to_vec())
            }
        });
    }
    Some(Request {
        id,
        pool,
        tx,
        op,
        sql,
        params,
    })
}

#[derive(Clone, Debug)]
enum Fail {
    /// an ERR packet of the server
    Server {
        code: u16,
        state: [u8; 5],
        message: String,
    },
    Client(String),
}

impl Fail {
    fn client(msg: &str) -> Fail {
        Fail::Client(msg.to_string())
    }
}

/// The start of an ok record: room for the record head, then the ok body
/// head. The rows are added after it, so the record is not copied later.
fn ok_head(affected: u64, insert_id: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(256);
    out.extend_from_slice(&[0; RES_HEAD]);
    out.extend_from_slice(&affected.to_le_bytes());
    out.extend_from_slice(&insert_id.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    out
}

/// The whole result record. An ok body already has room for the head.
fn record(id: u64, result: Result<Vec<u8>, Fail>) -> Vec<u8> {
    let (status, mut out) = match result {
        Ok(out) => (0u8, out),
        Err(fail) => {
            let (code, kind, state, msg) = match fail {
                Fail::Server {
                    code,
                    state,
                    message,
                } => (code, 0u8, state, message),
                Fail::Client(msg) => (0, 1, *b"HY000", msg),
            };
            let mut out = Vec::with_capacity(RES_HEAD + 8 + msg.len());
            out.extend_from_slice(&[0; RES_HEAD]);
            out.extend_from_slice(&code.to_le_bytes());
            out.push(kind);
            out.extend_from_slice(&state);
            out.extend_from_slice(msg.as_bytes());
            (1, out)
        }
    };
    let len = (out.len() - RES_HEAD) as u32;
    out[0..8].copy_from_slice(&id.to_le_bytes());
    out[8..12].copy_from_slice(&[status, 0, 0, 0]);
    out[12..16].copy_from_slice(&len.to_le_bytes());
    out
}

// ---- values ------------------------------------------------------------

/// What Dart gets for a column.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Kind {
    Text,
    Bool,
    Int,
    Double,
    Date,
    Json,
    LongText,
    Bytes,
    Time,
}

/// The kind of a column from its definition: type, charset, length, flags.
fn kind_of(ty: u8, charset: u16, length: u32, flags: u16) -> Kind {
    let binary = charset == BINARY;
    match ty {
        // TINYINT(1), used by boolean()
        1 if length == 1 => Kind::Bool,
        // TINY, SHORT, LONG, LONGLONG, INT24, YEAR
        1 | 2 | 3 | 8 | 9 | 13 => Kind::Int,
        // FLOAT, DOUBLE, DECIMAL, NEWDECIMAL
        4 | 5 | 0 | 246 => Kind::Double,
        // DATE, NEWDATE, DATETIME, DATETIME2, TIMESTAMP, TIMESTAMP2
        10 | 14 | 12 | 18 | 7 | 17 => Kind::Date,
        // TIME, TIME2
        11 | 19 => Kind::Time,
        245 => Kind::Json,
        // TINY_BLOB, MEDIUM_BLOB, LONG_BLOB, BLOB
        249..=252 => {
            if binary {
                Kind::Bytes
            } else {
                Kind::LongText
            }
        }
        // BIT, GEOMETRY
        16 | 255 => Kind::Bytes,
        _ if binary && flags & ENUM_FLAG == 0 => Kind::Bytes,
        _ => Kind::Text,
    }
}

fn put_bytes(out: &mut Vec<u8>, tag: u8, b: &[u8]) {
    out.push(tag);
    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
    out.extend_from_slice(b);
}

fn put_int(out: &mut Vec<u8>, v: i64) {
    out.push(T_INT);
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_double(out: &mut Vec<u8>, v: f64) {
    out.push(T_DOUBLE);
    out.extend_from_slice(&v.to_bits().to_le_bytes());
}

fn put_text(out: &mut Vec<u8>, s: &str) {
    put_bytes(out, T_TEXT, s.as_bytes());
}

fn cell(out: &mut Vec<u8>, kind: Kind, value: Value) {
    match (kind, value) {
        (_, Value::Null) => out.push(T_NULL),
        (Kind::Bool, Value::Int(v)) => out.extend_from_slice(&[T_BOOL, (v != 0) as u8]),
        (Kind::Bool, Value::UInt(v)) => out.extend_from_slice(&[T_BOOL, (v != 0) as u8]),
        (Kind::Bool, Value::Bytes(b)) => out.extend_from_slice(&[T_BOOL, (b != b"0") as u8]),
        (_, Value::Int(v)) => put_int(out, v),
        (_, Value::UInt(v)) => match i64::try_from(v) {
            Ok(v) => put_int(out, v),
            Err(_) => put_text(out, &v.to_string()),
        },
        (_, Value::Float(v)) => put_double(out, f64::from(v)),
        (_, Value::Double(v)) => put_double(out, v),
        (_, Value::Date(y, mo, d, h, mi, s, us)) => date(out, y, mo, d, h, mi, s, us),
        (_, Value::Time(neg, days, h, m, s, us)) => {
            put_text(out, &time_text(neg, days, h, m, s, us))
        }
        (Kind::Int, Value::Bytes(b)) => {
            match std::str::from_utf8(b).ok().and_then(|s| s.parse().ok()) {
                Some(v) => put_int(out, v),
                None => put_bytes(out, T_TEXT, b),
            }
        }
        (Kind::Double, Value::Bytes(b)) => {
            match std::str::from_utf8(b).ok().and_then(|s| s.parse().ok()) {
                Some(v) => put_double(out, v),
                None => put_bytes(out, T_TEXT, b),
            }
        }
        (Kind::Date, Value::Bytes(b)) => match parse_date(b) {
            Some((y, mo, d, h, mi, s, us)) => date(out, y, mo, d, h, mi, s, us),
            None => put_bytes(out, T_TEXT, b),
        },
        (Kind::Json, Value::Bytes(b)) => put_bytes(out, T_JSON, b),
        (Kind::LongText, Value::Bytes(b)) => put_bytes(out, T_LONG_TEXT, b),
        (Kind::Bytes, Value::Bytes(b)) => put_bytes(out, T_BYTES, b),
        (_, Value::Bytes(b)) => put_bytes(out, T_TEXT, b),
    }
}

#[allow(clippy::too_many_arguments)]
fn date(out: &mut Vec<u8>, y: u16, mo: u8, d: u8, h: u8, mi: u8, s: u8, us: u32) {
    if mo == 0 || d == 0 {
        // a zero date like 0000-00-00 is not a DateTime, keep the text
        let text = format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}");
        return put_text(out, &text);
    }
    let days = days_from_civil(i64::from(y), i64::from(mo), i64::from(d));
    let secs = days * 86_400 + i64::from(h) * 3600 + i64::from(mi) * 60 + i64::from(s);
    out.push(T_DATE);
    out.extend_from_slice(&(secs * 1_000_000 + i64::from(us)).to_le_bytes());
}

/// days since 1970-01-01 (Howard Hinnant's algorithm)
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

type DateParts = (u16, u8, u8, u8, u8, u8, u32);

/// `YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS` and `YYYY-MM-DD HH:MM:SS.ffffff`
fn parse_date(b: &[u8]) -> Option<DateParts> {
    let s = std::str::from_utf8(b).ok()?;
    let num = |r: std::ops::Range<usize>| s.get(r)?.parse::<u32>().ok();
    let y = num(0..4)? as u16;
    let mo = num(5..7)? as u8;
    let d = num(8..10)? as u8;
    if s.len() == 10 {
        return Some((y, mo, d, 0, 0, 0, 0));
    }
    let h = num(11..13)? as u8;
    let mi = num(14..16)? as u8;
    let sec = num(17..19)? as u8;
    let mut us = 0;
    if let Some(frac) = s.get(20..).filter(|f| !f.is_empty()) {
        let digits = &frac[..frac.len().min(6)];
        us = digits.parse::<u32>().ok()? * 10u32.pow(6 - digits.len() as u32);
    }
    Some((y, mo, d, h, mi, sec, us))
}

fn time_text(neg: bool, days: u32, h: u8, m: u8, s: u8, us: u32) -> String {
    let sign = if neg { "-" } else { "" };
    let hours = days * 24 + u32::from(h);
    if us == 0 {
        format!("{sign}{hours:02}:{m:02}:{s:02}")
    } else {
        format!("{sign}{hours:02}:{m:02}:{s:02}.{us:06}")
    }
}

// ---- FFI ---------------------------------------------------------------

/// ABI version of this library. Dart checks it at load.
#[unsafe(no_mangle)]
pub extern "C" fn dox_mysql_version() -> i32 {
    ABI_VERSION
}

/// Reads `Dart_PostCObject` from `NativeApi.initializeApiDLData`.
/// Returns 0 on success.
///
/// # Safety
/// `data` must be `NativeApi.initializeApiDLData`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dox_mysql_init(data: *mut c_void) -> i32 {
    if data.is_null() {
        return -1;
    }
    let api = unsafe { &*(data as *const DartApi) };
    if api.major != 2 {
        return -2; // DART_API_DL_MAJOR_VERSION
    }
    let mut entry = api.functions;
    unsafe {
        while !(*entry).name.is_null() {
            if CStr::from_ptr((*entry).name).to_bytes() == b"Dart_PostCObject" {
                let f: PostFn = std::mem::transmute((*entry).function);
                let _ = POST.set(f);
                return 0;
            }
            entry = entry.add(1);
        }
    }
    -3
}

/// Opens a pool for a `mysql://` URL (it takes the options of mysql_async,
/// see README.md). It connects lazily, on the first query. The same URL
/// gives the same pool, so isolates share it. Each open needs one
/// [dox_mysql_close]. `_threads` is not used any more (there is no runtime
/// thread). Returns a handle (> 0), or -1 for a bad URL.
///
/// # Safety
/// `url` must point to `len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dox_mysql_connect(url: *const u8, len: i64, _threads: i32) -> i64 {
    let bytes = unsafe { std::slice::from_raw_parts(url, len as usize) };
    let Ok(url) = std::str::from_utf8(bytes) else {
        return -1;
    };
    let mut pools = POOLS.lock().unwrap();
    if let Some((key, entry)) = pools.iter_mut().find(|(_, e)| e.shared.url == url) {
        entry.refs += 1;
        return key as i64 + 1;
    }
    let Some(opts) = parse_url(url) else {
        return -1;
    };
    let tls = match opts.tls {
        Some(t) => match tls::config(t) {
            Some(c) => Some(c),
            None => return -1,
        },
        None => None,
    };
    // the one DNS lookup that blocks: when the pool opens. Empty when it
    // fails: the first connect then looks up again on a helper thread.
    let addrs = match opts.socket {
        Some(_) => Vec::new(),
        None => link::resolve(&opts.host, opts.port).unwrap_or_default(),
    };
    let key = pools.insert(PoolEntry {
        shared: Arc::new(Shared {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            url: url.to_string(),
            opts,
            tls,
            addrs: Mutex::new(addrs),
            resolving: AtomicBool::new(false),
            state: Mutex::new(PoolState::default()),
            waiting: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
        }),
        refs: 1,
    });
    key as i64 + 1
}

/// Gives back one open of a pool. The last one closes the pool for every
/// isolate: its idle connections close now, the others once their running
/// queries finish. New requests fail with "the pool is closed".
#[unsafe(no_mangle)]
pub extern "C" fn dox_mysql_close(handle: i64) {
    let Some(key) = (handle as u64).checked_sub(1) else {
        return;
    };
    let mut pools = POOLS.lock().unwrap();
    let Some(entry) = pools.get_mut(key as usize) else {
        return;
    };
    entry.refs -= 1;
    if entry.refs > 0 {
        return;
    }
    let shared = pools.remove(key as usize).shared;
    drop(pools);
    link::close_pool(&shared);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_pool_closes_on_last_close() {
        let url = b"mysql://root@127.0.0.1:1/close_test";
        let open = || unsafe { dox_mysql_connect(url.as_ptr(), url.len() as i64, 1) };
        let (a, b) = (open(), open());
        assert_eq!(a, b);
        let alive = || POOLS.lock().unwrap().contains((a - 1) as usize);
        dox_mysql_close(a);
        assert!(alive());
        dox_mysql_close(b);
        assert!(!alive());
        // one more close does nothing
        dox_mysql_close(a);
    }

    #[test]
    fn urls() {
        let o = parse_url(
            "mysql://us%40er:p%40ss%2Bw@[::1]:3307/my%2Ddb?pool_min=2&pool_max=5\
             &stmt_cache_size=8&conn_ttl=60&require_ssl=true&verify_ca=false\
             &reset_connection=false&prefer_socket=false",
        )
        .unwrap();
        assert_eq!((o.user.as_str(), o.pass.as_str()), ("us@er", "p@ss+w"));
        assert_eq!((o.host.as_str(), o.port), ("::1", 3307));
        assert_eq!(o.db.as_deref(), Some("my-db"));
        assert_eq!((o.max, o.stmt_cache), (5, 8));
        assert_eq!(o.conn_ttl, Some(Duration::from_secs(60)));
        assert!(!o.reset);
        assert!(!o.tls.unwrap().verify_ca && o.tls.unwrap().verify_identity);
        let o = parse_url(
            "mysql://u@h/d?wait_timeout=7&abs_conn_ttl=9&tcp_keepalive=1500\
             &inactive_connection_ttl=3&compression=fast",
        )
        .unwrap();
        assert_eq!(o.conn_ttl, Some(Duration::from_secs(7)));
        assert_eq!(o.abs_ttl, Some(Duration::from_secs(9)));
        assert_eq!(o.keepalive, Some(Duration::from_millis(1500)));
        assert_eq!(o.inactive_ttl, Duration::from_secs(3));
        assert!(o.reset);
        let o = parse_url("mysql://root@localhost/db?socket=/tmp/my.sock").unwrap();
        assert_eq!(o.socket.as_deref(), Some("/tmp/my.sock"));
        assert_eq!((o.port, o.max, o.tls), (3306, 100, None));
        // like mysql_async: unknown options and bad values are errors
        assert!(parse_url("mysql://root@localhost/db?nope=1").is_none());
        assert!(parse_url("mysql://root@localhost/db?pool_max=x").is_none());
        assert!(parse_url("mysql://root@localhost/db?pool_min=5&pool_max=2").is_none());
        assert!(parse_url("postgres://root@localhost/db").is_none());
    }

    #[test]
    fn records_have_the_head_in_front() {
        let ok = record(7, Ok(ok_head(3, 4)));
        assert_eq!(u64::from_le_bytes(ok[0..8].try_into().unwrap()), 7);
        assert_eq!(ok[8], 0);
        assert_eq!(u32::from_le_bytes(ok[12..16].try_into().unwrap()), 24);
        assert_eq!(u64::from_le_bytes(ok[16..24].try_into().unwrap()), 3);
        let err = record(8, Err(Fail::client("boom")));
        assert_eq!(err[8], 1);
        assert_eq!(u32::from_le_bytes(err[12..16].try_into().unwrap()), 12);
        assert_eq!(&err[16 + 8..], b"boom");
    }

    #[test]
    fn civil_days() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(2000, 3, 1), 11_017);
        assert_eq!(days_from_civil(1969, 12, 31), -1);
    }

    #[test]
    fn dates() {
        assert_eq!(parse_date(b"2024-02-29"), Some((2024, 2, 29, 0, 0, 0, 0)));
        assert_eq!(
            parse_date(b"2024-02-29 13:14:15.5"),
            Some((2024, 2, 29, 13, 14, 15, 500_000))
        );
        assert_eq!(parse_date(b"nope"), None);
    }

    #[test]
    fn kinds() {
        assert_eq!(kind_of(1, 63, 1, 0), Kind::Bool);
        assert_eq!(kind_of(1, 63, 4, 0), Kind::Int);
        assert_eq!(kind_of(246, 63, 10, 0), Kind::Double);
        assert_eq!(kind_of(252, 45, 0, 0), Kind::LongText);
        assert_eq!(kind_of(252, 63, 0, 0), Kind::Bytes);
        assert_eq!(kind_of(254, 63, 0, 0), Kind::Bytes);
        assert_eq!(kind_of(254, 63, 0, ENUM_FLAG), Kind::Text);
        assert_eq!(kind_of(253, 45, 0, 0), Kind::Text);
    }

    #[test]
    fn requests() {
        let mut b = Vec::new();
        b.extend_from_slice(&7u64.to_le_bytes());
        b.extend_from_slice(&1u64.to_le_bytes());
        b.extend_from_slice(&0u64.to_le_bytes());
        b.extend_from_slice(&[OP_QUERY, 0, 0, 0]);
        b.extend_from_slice(&8u32.to_le_bytes());
        b.extend_from_slice(&2u32.to_le_bytes());
        b.extend_from_slice(b"SELECT ?");
        b.push(T_INT);
        b.extend_from_slice(&5i64.to_le_bytes());
        b.push(T_TEXT);
        b.extend_from_slice(&2u32.to_le_bytes());
        b.extend_from_slice(b"hi");
        let mut r = Reader(&b);
        let req = read_request(&mut r).unwrap();
        assert_eq!((req.id, req.pool, req.op), (7, 1, OP_QUERY));
        assert_eq!(req.sql, "SELECT ?");
        assert_eq!(
            req.params,
            vec![Param::Int(5), Param::Bytes(b"hi".to_vec())]
        );
        assert!(read_request(&mut r).is_none());
        // a cut record is ignored
        let mut r = Reader(&b[..b.len() - 1]);
        assert!(read_request(&mut r).is_none());
    }
}
