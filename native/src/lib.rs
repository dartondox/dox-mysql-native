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
//! - Each request has a timeout (`request_timeout_ms`, 300 s by default).
//!   A transaction that gets no request for that long is rolled back, so
//!   its connection goes back to the pool.
//! - A stream request (op 5) sends its rows in chunks. Each chunk is an ok
//!   record with status 2, the last one has status 0. Rust holds at most
//!   [CREDIT] chunks that Dart did not take yet: a more record (op 7, params:
//!   the id and a count) lets it read on. A cancel record (op 6, param: the
//!   id) stops a request.
//!
//! Request record (little endian, 32 byte head): u64 id, u64 pool, u64 tx,
//! u8 op, 3 bytes reserved, u32 sql_len. Then u32 param count, sql, params.
//! A stream request has the chunk size as one more int param at the end.
//! A param is a u8 tag and a value: 0 null, 1 i64, 2 f64,
//! 3 text (u32 len + utf8), 4 bytes (u32 len + bytes).
//!
//! Result record (16 byte head): u64 id, u8 status (0 ok, 1 error, 2 a
//! chunk of a stream, more follow),
//! 3 bytes reserved, u32 body_len. Then the body.
//! - ok: u64 affected rows, u64 insert id (the tx handle for `begin`),
//!   u32 column count, u32 row count, the names (u16 len + utf8), then each
//!   cell as a u8 tag and a value: 0 null, 1 i64, 2 f64, 3 text,
//!   4 bytes, 5 date (i64 microseconds since epoch, UTC), 6 bool (u8),
//!   7 json text, 8 long text (TEXT columns, MariaDB keeps JSON there).
//!   Text, bytes and json are a u32 len and the bytes.
//! - error: u16 server error code (0 for a client error), u8 kind
//!   (0 server, 1 client, 2 cancelled), 5 bytes sqlstate, then the message
//!   (utf8).
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

// the entry points of the fuzz targets in `fuzz/`, see `proto::fuzz` and
// `link::fuzz`
#[cfg(fuzzing)]
#[doc(hidden)]
pub mod fuzz {
    #[cfg(unix)]
    pub use crate::link::fuzz::conn;
    pub use crate::proto::fuzz::*;
}

/// Bump when the FFI functions or the record layout change.
const ABI_VERSION: i32 = 3;

const OP_QUERY: u8 = 0;
const OP_EXECUTE: u8 = 1;
const OP_BEGIN: u8 = 2;
const OP_COMMIT: u8 = 3;
const OP_ROLLBACK: u8 = 4;
const OP_STREAM: u8 = 5;
const OP_CANCEL: u8 = 6;
const OP_MORE: u8 = 7;

/// How many chunks of a stream Rust reads ahead of Dart. More would only
/// cost memory: Dart asks for the next one as soon as its listener took one.
const CREDIT: u32 = 2;

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

/// The default longest wait for one request (pool slot, connect and
/// query): `request_timeout_ms`. Over it the request fails with a clear
/// error, so Dart never waits forever. The same as the native Postgres
/// driver.
const TIMEOUT: Duration = Duration::from_secs(300);

/// The error of a request over its timeout `t`.
fn timed_out(t: Duration) -> Fail {
    let t = if t.subsec_millis() == 0 {
        format!("{} s", t.as_secs())
    } else {
        format!("{} ms", t.as_millis())
    };
    Fail::Client(format!("dox_mysql_native: timeout, no answer in {t}"))
}
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
#[derive(Clone, PartialEq, Debug)]
struct TlsOpts {
    verify_ca: bool,
    verify_identity: bool,
    built_in_roots: bool,
    /// a PEM file with more CA certificates to trust (`ssl_ca`)
    ca: Option<String>,
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
    /// the longest wait for one request (`request_timeout_ms`)
    timeout: Duration,
    /// stop a cancelled or timed out query on the server, with KILL on a
    /// new connection (`server_kill`)
    server_kill: bool,
}

const BAD_URL: &str = "not a mysql:// URL, or an unknown option or a bad option value";

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
        timeout: TIMEOUT,
        server_kill: true,
    };
    let mut wait_timeout = None;
    let mut tls = TlsOpts {
        verify_ca: true,
        verify_identity: true,
        built_in_roots: true,
        ca: None,
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
            "ssl_ca" => tls.ca = Some(v.into_owned()),
            "client_found_rows" => o.found_rows = flag()?,
            "tcp_nodelay" => o.nodelay = flag()?,
            "enable_cleartext_plugin" => o.cleartext = flag()?,
            "reset_connection" => o.reset = flag()?,
            "wait_timeout" => wait_timeout = Some(Duration::from_secs(num()?)),
            "abs_conn_ttl" => o.abs_ttl = Some(Duration::from_secs(num()?)),
            "abs_conn_ttl_jitter" => o.abs_jitter = Some(Duration::from_secs(num()?)),
            "inactive_connection_ttl" => o.inactive_ttl = Duration::from_secs(num()?),
            "tcp_keepalive" => o.keepalive = Some(Duration::from_millis(num()?)),
            "server_kill" => o.server_kill = flag()?,
            "request_timeout_ms" => o.timeout = Duration::from_millis(num()?.max(1)),
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
    /// how many links want more connections (`state.hungry.len()`)
    hungry: AtomicUsize,
    /// the fair share of one link: max / active links, at least 1
    share: AtomicUsize,
    closed: AtomicBool,
    /// goes up for each CREATE, ALTER, DROP or RENAME sent on this pool.
    /// It is part of the statement cache key, so a statement prepared
    /// before is not used again (see `Conn::query`).
    schema: AtomicU64,
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
    /// links that hold fewer than their fair share, are busy, and found
    /// the pool full: links over their share give back to them
    hungry: Vec<u64>,
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
    /// the request was cancelled from Dart
    Cancelled,
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

/// The body length of a record of `total` bytes, if the 4 byte length
/// field of the head can hold it.
fn body_len(total: usize) -> Option<u32> {
    u32::try_from(total - RES_HEAD).ok()
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
                Fail::Cancelled => (
                    0,
                    2,
                    *b"70100",
                    "dox_mysql_native: the query was cancelled".into(),
                ),
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
    let Some(len) = body_len(out.len()) else {
        // the length field has 4 bytes. Cold path: only a result this big.
        return record(
            id,
            Err(Fail::client("dox_mysql_native: the result is over 4 GiB")),
        );
    };
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
        // get: the cut can be inside a multi-byte char of a bad text
        let digits = frac.get(..frac.len().min(6))?;
        us = digits.parse::<u32>().ok()? * 10u32.pow(6 - digits.len() as u32);
    }
    Some((y, mo, d, h, mi, sec, us))
}

fn time_text(neg: bool, days: u32, h: u8, m: u8, s: u8, us: u32) -> String {
    let sign = if neg { "-" } else { "" };
    // days is a u32 from the server: u32 would overflow over 178956970 days
    let hours = u64::from(days) * 24 + u64::from(h);
    if us == 0 {
        format!("{sign}{hours:02}:{m:02}:{s:02}")
    } else {
        format!("{sign}{hours:02}:{m:02}:{s:02}.{us:06}")
    }
}

// ---- FFI ---------------------------------------------------------------

/// The `len` bytes at `ptr`. Empty when `ptr` is null or `len` is 0 or
/// negative, so a bad length from Dart can not read wild memory.
///
/// # Safety
/// When `ptr` is not null and `len` > 0, `ptr` must point to `len`
/// readable bytes.
unsafe fn bytes<'a>(ptr: *const u8, len: i64) -> &'a [u8] {
    if ptr.is_null() || len <= 0 {
        return &[];
    }
    unsafe { std::slice::from_raw_parts(ptr, len as usize) }
}

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
    if entry.is_null() {
        return -3;
    }
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
/// thread). Returns a handle (> 0), or -1 when the pool can not open: then
/// the reason is in `err` (UTF-8, cut at `err_cap` bytes, the rest
/// untouched).
///
/// # Safety
/// `url` must point to `len` bytes (a null `url` or a negative `len` is
/// an empty URL). `err` must point to `err_cap` writable bytes (a null
/// `err` gets no reason).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dox_mysql_connect(
    url: *const u8,
    len: i64,
    _threads: i32,
    err: *mut u8,
    err_cap: i64,
) -> i64 {
    let fail = |text: &str| {
        if !err.is_null() {
            let n = text.len().min(err_cap.max(0) as usize);
            unsafe { std::ptr::copy_nonoverlapping(text.as_ptr(), err, n) };
        }
        -1
    };
    let bytes = unsafe { bytes(url, len) };
    let Ok(url) = std::str::from_utf8(bytes) else {
        return fail("the URL is not valid UTF-8");
    };
    // one more open of a pool with this URL
    let reuse = |pools: &mut Slab<PoolEntry>| {
        let (key, entry) = pools.iter_mut().find(|(_, e)| e.shared.url == url)?;
        entry.refs += 1;
        Some(key as i64 + 1)
    };
    if let Some(handle) = reuse(&mut POOLS.lock().unwrap()) {
        return handle;
    }
    // no lock from here: the DNS lookup and the CA file can be slow, and
    // every isolate needs POOLS
    let Some(opts) = parse_url(url) else {
        return fail(BAD_URL);
    };
    let tls = match &opts.tls {
        Some(t) => match tls::config(t) {
            Ok(c) => Some(c),
            Err(e) => return fail(&e),
        },
        None => None,
    };
    // the one DNS lookup that blocks: when the pool opens. Empty when it
    // fails: the first connect then looks up again on a helper thread.
    let addrs = match opts.socket {
        Some(_) => Vec::new(),
        None => link::resolve(&opts.host, opts.port).unwrap_or_default(),
    };
    let opts_max = opts.max;
    let mut pools = POOLS.lock().unwrap();
    // another isolate may have opened the same URL in the meantime
    if let Some(handle) = reuse(&mut pools) {
        return handle;
    }
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
            hungry: AtomicUsize::new(0),
            share: AtomicUsize::new(opts_max),
            closed: AtomicBool::new(false),
            schema: AtomicU64::new(0),
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

    unsafe fn connect(url: &[u8]) -> i64 {
        unsafe { dox_mysql_connect(url.as_ptr(), url.len() as i64, 1, std::ptr::null_mut(), 0) }
    }

    /// The handle, or the reason Dart gets.
    fn connect_error(url: &str) -> Result<i64, String> {
        let mut err = [0u8; 256];
        let h =
            unsafe { dox_mysql_connect(url.as_ptr(), url.len() as i64, 1, err.as_mut_ptr(), 256) };
        let end = err.iter().position(|&b| b == 0).unwrap_or(err.len());
        if h > 0 {
            Ok(h)
        } else {
            Err(String::from_utf8_lossy(&err[..end]).into_owned())
        }
    }

    #[test]
    fn connect_errors_say_why() {
        assert_eq!(connect_error("mysql://u@h/d?nope=1"), Err(BAD_URL.into()));
        // TLS with no root to trust and the chain check on
        assert_eq!(
            connect_error(
                "mysql://u@h/d?require_ssl=true&built_in_roots=false&verify_identity=false"
            ),
            Err(tls::NO_ROOTS.into())
        );
        // the reason is cut at err_cap
        let url = "mysql://u@h/d?nope=1";
        let mut err = [0u8; 4];
        let h =
            unsafe { dox_mysql_connect(url.as_ptr(), url.len() as i64, 1, err.as_mut_ptr(), 4) };
        assert_eq!((h, &err), (-1, b"not "));
    }

    #[test]
    fn shared_pool_closes_on_last_close() {
        let url = b"mysql://root@127.0.0.1:1/close_test";
        let open = || unsafe { connect(url) };
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
    fn bad_ffi_lengths_and_null_pointers_are_empty() {
        let b = b"abc";
        unsafe {
            assert_eq!(bytes(b.as_ptr(), 3), b"abc");
            assert!(bytes(b.as_ptr(), -1).is_empty());
            assert!(bytes(b.as_ptr(), i64::MIN).is_empty());
            assert!(bytes(std::ptr::null(), 3).is_empty());
            // a negative length or a null URL is a bad URL, not a crash
            let none = std::ptr::null_mut();
            assert_eq!(dox_mysql_connect(b.as_ptr(), -1, 1, none, 0), -1);
            assert_eq!(dox_mysql_connect(std::ptr::null(), 10, 1, none, 0), -1);
            assert_eq!(dox_mysql_init(std::ptr::null_mut()), -1);
        }
    }

    #[test]
    fn opens_at_the_same_time_share_one_pool() {
        let url = "mysql://root@localhost:1/race_test";
        let handles: Vec<i64> = std::thread::scope(|s| {
            let open = || s.spawn(|| unsafe { connect(url.as_bytes()) });
            let all: Vec<_> = (0..8).map(|_| open()).collect();
            all.into_iter().map(|t| t.join().unwrap()).collect()
        });
        assert!(handles[0] > 0 && handles.iter().all(|&h| h == handles[0]));
        let key = (handles[0] - 1) as usize;
        assert_eq!(POOLS.lock().unwrap()[key].refs, 8);
        for h in handles {
            dox_mysql_close(h);
        }
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
        let t = o.tls.unwrap();
        assert!(!t.verify_ca && t.verify_identity && t.ca.is_none());
        let o = parse_url("mysql://u@h/d?require_ssl=true&ssl_ca=/etc/ca%20b.pem").unwrap();
        assert_eq!(o.tls.unwrap().ca.as_deref(), Some("/etc/ca b.pem"));
        let o = parse_url(
            "mysql://u@h/d?wait_timeout=7&abs_conn_ttl=9&tcp_keepalive=1500\
             &inactive_connection_ttl=3&compression=fast",
        )
        .unwrap();
        assert_eq!(o.conn_ttl, Some(Duration::from_secs(7)));
        assert_eq!(o.abs_ttl, Some(Duration::from_secs(9)));
        assert_eq!(o.keepalive, Some(Duration::from_millis(1500)));
        assert_eq!(o.inactive_ttl, Duration::from_secs(3));
        assert_eq!(o.timeout, TIMEOUT);
        let o = parse_url("mysql://u@h/d?request_timeout_ms=1500").unwrap();
        assert_eq!(o.timeout, Duration::from_millis(1500));
        assert!(o.server_kill);
        assert!(
            !parse_url("mysql://u@h/d?server_kill=false")
                .unwrap()
                .server_kill
        );
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
    fn timeout_and_cancel_errors() {
        let Fail::Client(m) = timed_out(TIMEOUT) else {
            panic!()
        };
        assert_eq!(m, "dox_mysql_native: timeout, no answer in 300 s");
        let Fail::Client(m) = timed_out(Duration::from_millis(1500)) else {
            panic!()
        };
        assert_eq!(m, "dox_mysql_native: timeout, no answer in 1500 ms");
        let err = record(9, Err(Fail::Cancelled));
        assert_eq!((err[8], err[16 + 2]), (1, 2));
        assert_eq!(&err[16 + 3..16 + 8], b"70100");
    }

    #[test]
    fn a_body_over_4_gib_is_refused() {
        assert_eq!(body_len(RES_HEAD), Some(0));
        #[cfg(target_pointer_width = "64")]
        {
            assert_eq!(body_len(RES_HEAD + u32::MAX as usize), Some(u32::MAX));
            assert_eq!(body_len(RES_HEAD + u32::MAX as usize + 1), None);
        }
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
        // a cut of the fraction inside a multi-byte char must not panic
        assert_eq!(
            parse_date("2024-02-29 13:14:15.12345\u{2bf}".as_bytes()),
            None
        );
    }

    #[test]
    fn time_text_with_many_days() {
        assert_eq!(time_text(true, 1, 2, 3, 4, 0), "-26:03:04");
        // the days of a binary TIME is a u32: the hours must not overflow
        assert_eq!(
            time_text(false, u32::MAX, 5, 6, 7, 8),
            "103079215085:06:07.000008"
        );
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
