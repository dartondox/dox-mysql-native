//! The client loop. It runs on the thread of the Dart isolate, like the
//! server loop of dox_http_native (run.rs).
//!
//! Each isolate has one link: one mio `Poll` and the connections it uses
//! (non-blocking sockets).
//! - `dox_mysql_submit` (leaf call) takes a batch of request records (see
//!   lib.rs). Each request is encoded and written to a connection at once.
//!   The bytes the socket does not take wait in the out buffer of the
//!   connection, and go when it is writable. It returns 1 when results are
//!   ready already (eg. "the pool is closed"): then Dart calls run.
//! - `dox_mysql_run` (leaf call) does one poll turn that does not wait,
//!   reads the sockets that have data, runs the timers, and writes the
//!   result records of the finished requests into Dart's buffer. It
//!   returns the byte count, or -n when the buffer is too small.
//! - When requests wait and Dart is idle, the idle helper (one small thread
//!   per link) watches the poll file (epoll or kqueue) and posts `[1]` to
//!   the Dart port when it has an event or a timer is due. It never takes
//!   the core mutex for that, so Dart never waits for it. The flag `armed`
//!   makes sure only one side tells Dart: the one that clears it. Every
//!   [PROBE] it also checks that the isolate is alive (an empty post). When
//!   it is gone, the link closes its connections and is freed.
//!   Leaf calls never post to a Dart port: only the helper does.
//!
//! A Dart isolate may move to another OS thread between two event loop
//! turns. So all state is `Send` and lives behind the mutex. Nothing uses
//! thread locals.
//!
//! The connections of a pool are shared by the whole process (see
//! `Shared` in lib.rs). A link opens a new one while the pool is under its
//! limit. At the limit it sends more commands down its own connections
//! (pipelining), or it waits in the `waiters` list of the pool. Then each
//! link that holds connections of the pool gets a poke: a connection that
//! has no command left goes to the `idle` list of the pool (it leaves one
//! poll and joins another), and the first waiter gets a poke. A link
//! marks one busy connection to give back too, and sends no new command
//! to it, so a busy link can not starve the others.
//!
//! The split is fair too. The pool keeps a fair share (max / links that
//! hold or wait, at least 1). A busy link under its share that finds the
//! pool full goes to the `hungry` list. A link over its share then gives
//! back connections as they drain, and takes no new one while others are
//! hungry. The hot path only reads atomics: the pool lock is taken when a
//! link starts or stops holding, waiting or being hungry.
//!
//! Commands on a connection are pipelined: the server answers them in
//! order. A query without params goes as text (COM_QUERY), one with params
//! as a prepared statement (binary protocol). A statement is prepared once
//! per connection and set of param types, and kept in an LRU cache. Its first EXECUTE waits behind
//! the answer of its PREPARE (it needs the statement id), and the commands
//! after it wait too, so the order stays. Each command fails on its own.
//!
//! A transaction holds one connection until commit or rollback. A
//! transaction with no request for [TIMEOUT] is rolled back. Its id works
//! in every isolate: a request from another isolate goes to the mail of
//! the link that owns it (`TXS`), and that link gets a poke. It runs the
//! request and puts the result record in the mail of the sender, which
//! gets a poke too. Pokes only wake the helper threads, so no leaf call
//! touches the Dart API.
//!
//! DNS: the host is looked up when the pool opens. After a failed connect
//! a helper thread looks it up again. The isolate never waits for DNS.
use std::collections::{HashMap, VecDeque};
use std::fmt::Write as _;
use std::io::{self, ErrorKind, Read, Write};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex, Weak};
use std::time::{Duration, Instant};

use mio::event::Source;
use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Registry, Token, Waker};
use slab::Slab;

use crate::proto::{self, Column};
use crate::{
    CREDIT, Fail, OP_BEGIN, OP_CANCEL, OP_COMMIT, OP_EXECUTE, OP_MORE, OP_QUERY, OP_ROLLBACK,
    OP_STREAM, POOL_CLOSED, POST, Param, RES_HEAD, Reader, Request, Shared, ok_head, pool_of, post,
    read_request, record, timed_out,
};

const WAKER: Token = Token(usize::MAX);
/// How often the helper checks that the isolate is alive.
const PROBE: Duration = Duration::from_millis(100);
/// Smallest free room in a read buffer before a read.
const READ_ROOM: usize = 16 * 1024;
/// A buffer bigger than this is freed once it is empty.
const BIG_BUF: usize = 1 << 20;
const TX_CLOSED: &str = "transaction is closed";

/// Capability flags we always want.
const CAPS: u32 = proto::LONG_PASSWORD
    | proto::PROTOCOL_41
    | proto::TRANSACTIONS
    | proto::SECURE_CONNECTION
    | proto::MULTI_STATEMENTS
    | proto::MULTI_RESULTS
    | proto::PS_MULTI_RESULTS
    | proto::PLUGIN_AUTH
    | proto::PLUGIN_AUTH_LENENC
    | proto::DEPRECATE_EOF;

/// Who owns each open transaction, so a request from another isolate
/// finds the link that holds its connection.
static TXS: LazyLock<Mutex<HashMap<u64, Weak<Link>>>> = LazyLock::new(Default::default);

/// The transaction is over (commit, rollback, a closed connection).
fn end_tx(tx: u64) {
    TXS.lock().unwrap().remove(&tx);
}

/// Id bit of a request from another isolate, so its record never mixes
/// with the ids of this isolate's Dart side (they count up from 1).
const FOREIGN: u64 = 1 << 63;

/// `abs_conn_ttl` plus a random part of `abs_conn_ttl_jitter`.
fn max_age(o: &crate::Opts) -> Option<Duration> {
    let ttl = o.abs_ttl?;
    let jitter = o.abs_jitter.map_or(0, |j| j.as_millis() as u64);
    if jitter == 0 {
        return Some(ttl);
    }
    let mut b = [0u8; 8];
    let _ = ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut b);
    Some(ttl + Duration::from_millis(u64::from_le_bytes(b) % jitter))
}

// ---- sockets -----------------------------------------------------------

enum Sock {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(mio::net::UnixStream),
}

impl Sock {
    fn source(&mut self) -> &mut dyn Source {
        match self {
            Sock::Tcp(s) => s,
            #[cfg(unix)]
            Sock::Unix(s) => s,
        }
    }

    /// TCP_NODELAY and keepalive (`tcp_keepalive`, like mysql_async).
    fn tune(&self, nodelay: bool, keepalive: Option<Duration>) {
        match self {
            Sock::Tcp(s) => {
                let _ = s.set_nodelay(nodelay);
                if let Some(t) = keepalive {
                    let ka = socket2::TcpKeepalive::new().with_time(t);
                    let _ = socket2::SockRef::from(s).set_tcp_keepalive(&ka);
                }
            }
            #[cfg(unix)]
            Sock::Unix(_) => {}
        }
    }

    /// true when a non-blocking connect is done. Err: it failed.
    fn connected(&self) -> io::Result<bool> {
        let (err, peer) = match self {
            Sock::Tcp(s) => (s.take_error(), s.peer_addr().map(|_| ())),
            #[cfg(unix)]
            Sock::Unix(s) => (s.take_error(), s.peer_addr().map(|_| ())),
        };
        if let Some(e) = err? {
            return Err(e);
        }
        match peer {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == ErrorKind::NotConnected => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// true when the server did not close the socket and sent nothing
    /// (for a connection that waited in the idle list).
    fn quiet(&self) -> bool {
        let mut b = [0u8; 1];
        match self {
            Sock::Tcp(s) => matches!(s.peek(&mut b), Err(e) if e.kind() == ErrorKind::WouldBlock),
            #[cfg(unix)]
            Sock::Unix(s) => {
                use std::os::fd::AsRawFd;
                // SAFETY: a 1 byte peek into a 1 byte buffer
                let n = unsafe {
                    libc::recv(
                        s.as_raw_fd(),
                        b.as_mut_ptr() as *mut libc::c_void,
                        1,
                        libc::MSG_PEEK | libc::MSG_DONTWAIT,
                    )
                };
                n < 0 && io::Error::last_os_error().kind() == ErrorKind::WouldBlock
            }
        }
    }
}

impl Read for Sock {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Sock::Tcp(s) => s.read(buf),
            #[cfg(unix)]
            Sock::Unix(s) => s.read(buf),
        }
    }
}

impl Write for Sock {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Sock::Tcp(s) => s.write(buf),
            #[cfg(unix)]
            Sock::Unix(s) => s.write(buf),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn io_fail(e: impl std::fmt::Display) -> Fail {
    Fail::Client(format!("dox_mysql_native: {e}"))
}

// ---- commands ----------------------------------------------------------

/// What a sent command waits for.
enum What {
    /// a query or an execute. `rows`: Dart wants the rows (OP_QUERY).
    Result { rows: bool, binary: bool },
    /// START TRANSACTION of a begin: Dart gets the transaction id
    Begin(u64),
    /// COMMIT or ROLLBACK from Dart. The connection is free after it.
    End,
    /// a ROLLBACK nobody waits for (an idle transaction, a failed commit)
    Silent,
    /// COM_STMT_PREPARE of a statement slot
    Prepare(usize),
    /// COM_RESET_CONNECTION: nobody waits, an error is ignored
    Reset,
    /// KILL QUERY on a kill connection: nobody waits, an error is ignored
    Kill,
}

/// Where the answer of a command is.
enum St {
    Head,
    Cols(usize),
    Rows,
    /// prepare: definitions to skip
    Skip(usize),
}

/// A command that waits for its answer.
struct Wait {
    id: u64,
    what: What,
    deadline: Instant,
    st: St,
    /// still in the first result set: the one Dart gets
    first: bool,
    cols: Vec<Column>,
    /// the result record, with room for the head
    out: Vec<u8>,
    count: u32,
    fail: Option<Fail>,
    /// a CREATE, ALTER, DROP or RENAME: the pool counts a schema change
    /// again when it is done
    ddl: bool,
    /// Dart cancelled it: its rows are dropped, and it ends with a
    /// cancelled error
    cancel: bool,
    /// a KILL QUERY for it was started
    killing: bool,
    /// a stream: the rows go to Dart in chunks of this many. 0: no stream.
    chunk: u32,
    /// a stream: how many more chunks Dart takes now. At 0 the connection
    /// stops reading.
    credit: u32,
    /// where the rows start in `out` (after the column names)
    head_len: usize,
}

impl Wait {
    fn new(id: u64, what: What, deadline: Instant) -> Wait {
        let out = match what {
            What::Result { .. } => ok_head(0, 0),
            _ => Vec::new(),
        };
        Wait {
            id,
            what,
            deadline,
            st: St::Head,
            first: true,
            cols: Vec::new(),
            out,
            count: 0,
            fail: None,
            ddl: false,
            cancel: false,
            killing: false,
            chunk: 0,
            credit: 0,
            head_len: 0,
        }
    }

    /// The wait of a query, execute or stream request. A stream request
    /// has its chunk size as the last param.
    fn of(req: &mut Request, deadline: Instant) -> Wait {
        let chunk = if req.op == OP_STREAM {
            match req.params.pop() {
                Some(Param::Int(n)) => n.clamp(1, i64::from(u32::MAX)) as u32,
                _ => 1000,
            }
        } else {
            0
        };
        let what = What::Result {
            rows: req.op != OP_EXECUTE,
            binary: !req.params.is_empty(),
        };
        let mut w = Wait::new(req.id, what, deadline);
        w.chunk = chunk;
        w.credit = CREDIT;
        w
    }
}

/// A kill connection: it only sends `KILL QUERY` for a cancelled request,
/// or `KILL` for a connection that timed out.
struct Kill {
    /// the connection (key) and the request (id) to stop. None: the
    /// connection timed out and is closed here, so `KILL` its server
    /// thread (nothing else of ours can run there any more).
    target: Option<(usize, u64)>,
    /// the connection id on the server
    thread: u32,
    /// KILL QUERY is sent
    sent: bool,
    /// give up when it is not sent by then
    until: Instant,
}

/// A command that is not sent yet: the connection logs in, or a command
/// in front of it waits for its statement id.
enum Cmd {
    Query(Wait, String),
    Exec(Wait, usize, Vec<Param>),
    Prepare(usize, String, Instant),
    Close(usize),
    Reset(Instant),
}

impl Cmd {
    fn deadline(&self) -> Option<Instant> {
        match self {
            Cmd::Query(w, _) | Cmd::Exec(w, ..) => Some(w.deadline),
            Cmd::Prepare(_, _, d) | Cmd::Reset(d) => Some(*d),
            Cmd::Close(_) => None,
        }
    }
}

/// A statement of the sql starts with CREATE, ALTER, DROP or RENAME,
/// after spaces and comments (`/* */`, `-- `, `#`): it may change the type
/// of a column. Each statement of a multi-statement query counts, eg.
/// `SELECT 1; ALTER TABLE t ...`. A `;` inside a string or a comment does
/// not start a statement.
// ponytail: an executable comment (`/*! CREATE ... */`) is skipped as a
// comment, so DDL inside it keeps the old statements.
//
// The driver does not know if the sql_mode has NO_BACKSLASH_ESCAPES (it can
// change in the middle of a pipeline, eg. with SET). With it, a backslash
// is a plain char, so `'a\'` is a whole string. So sql with a backslash is
// read both ways, and DDL in either way counts. A wrong count only makes
// the connections prepare their statements again, which is safe. A missed
// one can keep old param types.
fn is_ddl(sql: &str) -> bool {
    scan_ddl(sql, true) || (sql.contains('\\') && scan_ddl(sql, false))
}

/// [is_ddl] for one sql_mode: `escapes` is false with NO_BACKSLASH_ESCAPES.
fn scan_ddl(sql: &str, escapes: bool) -> bool {
    let s = sql.as_bytes();
    let mut i = 0;
    // at the start of a statement
    let mut start = true;
    while i < s.len() {
        let c = s[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        let rest = &s[i..];
        if rest.starts_with(b"/*") {
            // not closed: a comment to the end
            let Some(p) = rest[2..].windows(2).position(|w| w == b"*/") else {
                return false;
            };
            i += p + 4;
            continue;
        }
        // MySQL needs a space or a control char after `--`
        let dashes = rest.starts_with(b"--")
            && rest
                .get(2)
                .is_none_or(|&b| b.is_ascii_whitespace() || b.is_ascii_control());
        if c == b'#' || dashes {
            let Some(p) = rest.iter().position(|&b| b == b'\n') else {
                return false;
            };
            i += p + 1;
            continue;
        }
        if start
            && [&b"CREATE"[..], b"ALTER", b"DROP", b"RENAME"]
                .iter()
                .any(|w| {
                    rest.get(..w.len())
                        .is_some_and(|h| h.eq_ignore_ascii_case(w))
                })
        {
            return true;
        }
        start = c == b';';
        if matches!(c, b'\'' | b'"' | b'`') {
            // skip the string or quoted name; a backslash escapes in
            // strings, and a doubled quote is two strings in a row
            let mut j = i + 1;
            loop {
                match s.get(j) {
                    None => return false,
                    Some(&b'\\') if escapes && c != b'`' => j += 2,
                    Some(&b) if b == c => break,
                    Some(_) => j += 1,
                }
            }
            i = j;
        }
        i += 1;
    }
    false
}

/// A prepared statement of one connection.
struct Stmt {
    /// the cache key: the sql plus the param types
    sql: String,
    /// None while its PREPARE has no answer
    id: Option<u32>,
    params: usize,
    /// its PREPARE failed: the commands that wait for it fail too
    failed: Option<Fail>,
    /// commands in `held` that use it
    refs: usize,
    /// when it was used last (a counter), for the LRU
    used: u64,
    /// in the cache (an evicted one waits for its COM_STMT_CLOSE)
    cached: bool,
}

enum Phase {
    /// a non-blocking connect runs. The next addresses, if it fails.
    Connect(Vec<SocketAddr>),
    Greeting,
    Auth {
        plugin: String,
        nonce: Vec<u8>,
        /// caching_sha2_password: asked the server for its RSA key
        key_asked: bool,
    },
    Ready,
}

// ---- connections -------------------------------------------------------

/// One MySQL connection: a state machine run by the loop.
pub(crate) struct Conn {
    sock: Sock,
    tls: Option<Box<rustls::ClientConnection>>,
    shared: Arc<Shared>,
    phase: Phase,
    /// bytes that go before TLS starts (the SSL request)
    raw: Vec<u8>,
    /// bytes not written yet, from `out_at`
    out: Vec<u8>,
    out_at: usize,
    /// waits for writable (only while bytes wait)
    writable: bool,
    /// read bytes, `in_s..in_e` not used yet
    inb: Vec<u8>,
    in_s: usize,
    in_e: usize,
    /// sequence id of the last packet of the login
    seq: u8,
    waits: VecDeque<Wait>,
    held: VecDeque<Cmd>,
    stmts: Slab<Stmt>,
    cache: HashMap<String, usize>,
    tick: u64,
    /// the server is MariaDB: TEXT columns may be JSON
    maria: bool,
    last_io: Instant,
    /// the last request of its transaction
    used: Instant,
    born: Instant,
    /// `abs_conn_ttl` plus its jitter. None: no limit.
    max_age: Option<Duration>,
    /// used since its last COM_RESET_CONNECTION
    dirty: bool,
    /// the transaction that holds it
    tx: Option<u64>,
    /// a COMMIT or ROLLBACK runs: not free yet
    ending: bool,
    /// give it back once it has no command: no new command goes to it
    release: bool,
    /// the connection id on the server, for KILL QUERY
    thread: u32,
    /// a KILL QUERY was sent for it: no new command goes to it, and it
    /// closes once it has no command
    doomed: bool,
    /// a kill connection (not counted in the pool)
    kill: Option<Kill>,
    /// the key of a kill connection that runs for this one: new commands
    /// wait until it has its answer, so the KILL can only hit the query it
    /// is for
    hold: Option<usize>,
}

/// The addresses of a host (a blocking DNS lookup: only when a pool opens,
/// or on a helper thread).
pub(crate) fn resolve(host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
    Ok((host, port).to_socket_addrs()?.collect())
}

impl Shared {
    /// Looks the host up again on a helper thread (eg. after a failover
    /// the name points to a new server). The next connect uses it.
    fn resolve_later(self: &Arc<Self>) {
        if self.opts.socket.is_some() || self.resolving.swap(true, Ordering::AcqRel) {
            return;
        }
        let shared = self.clone();
        let started = std::thread::Builder::new()
            .name("dox-mysql-dns".into())
            .spawn(move || {
                if let Ok(a) = resolve(&shared.opts.host, shared.opts.port)
                    && !a.is_empty()
                {
                    *shared.addrs.lock().unwrap() = a;
                }
                shared.resolving.store(false, Ordering::Release);
            });
        if started.is_err() {
            self.resolving.store(false, Ordering::Release);
        }
    }
}

/// A connect error that says the host and port, so a wrong one is easy to
/// see. Never the password.
fn dial_fail(o: &crate::Opts, e: impl std::fmt::Display) -> Fail {
    io_fail(format!(
        "can not connect to host {}, port {}: {e}",
        o.host, o.port
    ))
}

/// Opens a socket to the next address. The connect goes on in the loop.
fn connect_next(o: &crate::Opts, addrs: &mut Vec<SocketAddr>) -> Result<Sock, Fail> {
    let mut last = None;
    while !addrs.is_empty() {
        let a = addrs.remove(0);
        match TcpStream::connect(a) {
            Ok(s) => return Ok(Sock::Tcp(s)),
            Err(e) => last = Some(format!("{e} ({a})")),
        }
    }
    Err(dial_fail(o, last.unwrap_or_else(|| "no address".into())))
}

impl Conn {
    /// Starts a connection: a non-blocking connect to the addresses the
    /// pool looked up. No DNS lookup here.
    fn start(shared: Arc<Shared>, now: Instant) -> Result<Conn, Fail> {
        let o = &shared.opts;
        let (sock, phase) = if let Some(path) = &o.socket {
            #[cfg(unix)]
            {
                let s = mio::net::UnixStream::connect(path)
                    .map_err(|e| io_fail(format!("can not connect to {path}: {e}")))?;
                (Sock::Unix(s), Phase::Connect(Vec::new()))
            }
            #[cfg(not(unix))]
            {
                return Err(io_fail(format!("no Unix sockets on this system: {path}")));
            }
        } else {
            let mut addrs = shared.addrs.lock().unwrap().clone();
            if addrs.is_empty() {
                shared.resolve_later();
                return Err(io_fail(format!("can not find the host {}", o.host)));
            }
            let s = connect_next(o, &mut addrs).inspect_err(|_| shared.resolve_later())?;
            (s, Phase::Connect(addrs))
        };
        let max_age = max_age(o);
        Ok(Conn {
            sock,
            tls: None,
            shared,
            phase,
            raw: Vec::new(),
            out: Vec::with_capacity(4096),
            out_at: 0,
            writable: true,
            inb: Vec::new(),
            in_s: 0,
            in_e: 0,
            seq: 0,
            waits: VecDeque::new(),
            held: VecDeque::new(),
            stmts: Slab::new(),
            cache: HashMap::new(),
            tick: 0,
            maria: false,
            last_io: now,
            used: now,
            max_age,
            born: now,
            dirty: false,
            tx: None,
            ending: false,
            release: false,
            thread: 0,
            doomed: false,
            kill: None,
            hold: None,
        })
    }

    fn ready(&self) -> bool {
        matches!(self.phase, Phase::Ready)
    }

    /// commands that wait for an answer or to be sent
    fn load(&self) -> usize {
        self.waits.len() + self.held.len()
    }

    /// free for new commands of any request (not a transaction's)
    fn free(&self) -> bool {
        self.tx.is_none() && !self.ending && !self.release && !self.doomed
    }

    /// a stream at the front waits for Dart to take its chunks: do not read
    fn paused(&self) -> bool {
        self.waits
            .front()
            .is_some_and(|w| w.chunk > 0 && w.credit == 0 && !w.cancel)
    }

    /// free and with no command: can be given away or closed
    fn empty(&self) -> bool {
        self.tx.is_none() && !self.ending && self.load() == 0
    }

    fn unsent(&self) -> bool {
        !self.raw.is_empty()
            || self.out_at < self.out.len()
            || self.tls.as_ref().is_some_and(|t| t.wants_write())
    }

    fn deadline(&self) -> Option<Instant> {
        let a = self.waits.front().map(|w| w.deadline);
        let b = self.held.iter().find_map(Cmd::deadline);
        match (a, b) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    // ---- commands ----

    /// Sends a command, or holds it until it can go. A PREPARE may pass
    /// the held commands: it changes nothing the others see.
    fn put(&mut self, cmd: Cmd, ready: &mut Vec<u8>) {
        if self.ready()
            && self.hold.is_none()
            && (self.held.is_empty() || matches!(cmd, Cmd::Prepare(..)))
        {
            if let Some(cmd) = self.encode(cmd, ready) {
                self.held.push_back(cmd);
            }
        } else {
            self.held.push_back(cmd);
        }
    }

    /// Writes a command to `out`. Gives it back when its statement has no
    /// id yet (then it must wait).
    fn encode(&mut self, cmd: Cmd, ready: &mut Vec<u8>) -> Option<Cmd> {
        match cmd {
            Cmd::Query(w, sql) => {
                proto::query(&mut self.out, sql.as_bytes());
                self.waits.push_back(w);
            }
            Cmd::Prepare(slot, sql, deadline) => {
                proto::prepare(&mut self.out, sql.as_bytes());
                self.waits
                    .push_back(Wait::new(0, What::Prepare(slot), deadline));
            }
            Cmd::Reset(deadline) => {
                proto::reset(&mut self.out);
                self.waits.push_back(Wait::new(0, What::Reset, deadline));
            }
            Cmd::Close(slot) => {
                let st = self.stmts.remove(slot);
                if let Some(id) = st.id {
                    proto::close(&mut self.out, id);
                }
            }
            Cmd::Exec(w, slot, params) => {
                let st = &self.stmts[slot];
                if st.id.is_none() && st.failed.is_none() {
                    return Some(Cmd::Exec(w, slot, params));
                }
                if let Some(f) = &st.failed {
                    ready.extend_from_slice(&record(w.id, Err(f.clone())));
                } else if st.params != params.len() {
                    let msg = format!(
                        "dox_mysql_native: the statement takes {} parameters but {} were given",
                        st.params,
                        params.len()
                    );
                    ready.extend_from_slice(&record(w.id, Err(Fail::Client(msg))));
                } else {
                    proto::execute(&mut self.out, st.id.unwrap_or(0), &params);
                    self.waits.push_back(w);
                }
                let st = &mut self.stmts[slot];
                st.refs -= 1;
                if st.refs == 0 && st.failed.is_some() {
                    self.stmts.remove(slot);
                }
            }
        }
        None
    }

    /// Sends the held commands that can go now, in order.
    fn release_held(&mut self, ready: &mut Vec<u8>) {
        if self.hold.is_some() {
            return;
        }
        while let Some(cmd) = self.held.pop_front() {
            if let Some(cmd) = self.encode(cmd, ready) {
                self.held.push_front(cmd);
                return;
            }
        }
    }

    /// A query: as text without params, as a prepared statement with.
    fn query(&mut self, mut w: Wait, sql: String, params: Vec<Param>, ready: &mut Vec<u8>) {
        if is_ddl(&sql) {
            // counted when it is sent: the commands after it on this
            // connection run after it too. Counted again when it is done
            // (`done`): another connection may prepare a statement while
            // the DDL runs, and that one has the old types.
            self.shared.schema.fetch_add(1, Ordering::Relaxed);
            w.ddl = true;
        }
        if params.is_empty() {
            return self.put(Cmd::Query(w, sql), ready);
        }
        self.tick += 1;
        // The cache key is the sql plus the param types. MySQL 8.4 keeps
        // the types of the first EXECUTE of a statement: a later string
        // for a param first sent as a number is compared as a number
        // ('zzz' = 0). So each set of types gets its own statement.
        // The key also has the `schema` count of the pool. MySQL 8.4
        // keeps the param types of a statement when the table changes
        // under it: '007' sent to a column that was int and is now text
        // is stored as '7'. After a CREATE, ALTER, DROP or RENAME the key
        // is new, so the statement is prepared again. The old ones leave
        // the cache by the LRU.
        // ponytail: a schema change from another process is not seen.
        // MySQL gives no error for it (it prepares the statement again by
        // itself, with the old param types), so there is nothing to catch
        // and retry.
        let schema = self.shared.schema.load(Ordering::Relaxed);
        let mut key = String::with_capacity(sql.len() + 24 + params.len());
        key.push_str(&sql);
        key.push('\0');
        // ponytail: fmt of a u64 per prepared query, a few ns
        let _ = write!(key, "{schema}\0");
        for p in &params {
            key.push(match p {
                Param::Null => 'n',
                Param::Int(_) => 'i',
                Param::Double(_) => 'd',
                Param::Bytes(_) => 's',
            });
        }
        let slot = match self.cache.get(key.as_str()) {
            Some(&s) => s,
            None => {
                let s = self.stmts.insert(Stmt {
                    sql: key.clone(),
                    id: None,
                    params: 0,
                    failed: None,
                    refs: 0,
                    used: 0,
                    cached: true,
                });
                self.cache.insert(key, s);
                self.put(Cmd::Prepare(s, sql, w.deadline), ready);
                s
            }
        };
        let st = &mut self.stmts[slot];
        st.used = self.tick;
        st.refs += 1;
        self.put(Cmd::Exec(w, slot, params), ready);
        self.evict(ready);
    }

    /// Closes the statements used longest ago while the cache is over its
    /// size. A statement whose PREPARE has no answer yet stays.
    /// ponytail: a scan of the cache (32 by default) per new statement.
    fn evict(&mut self, ready: &mut Vec<u8>) {
        while self.cache.len() > self.shared.opts.stmt_cache {
            let old = self
                .cache
                .values()
                .copied()
                .filter(|&s| self.stmts[s].id.is_some())
                .min_by_key(|&s| self.stmts[s].used);
            let Some(slot) = old else { return };
            let st = &mut self.stmts[slot];
            st.cached = false;
            self.cache.remove(st.sql.as_str());
            self.put(Cmd::Close(slot), ready);
        }
    }

    // ---- IO ----

    /// Writes what the socket takes. It waits for writable only while
    /// bytes wait: a writable event while idle would wake the helper for
    /// nothing.
    fn flush(&mut self, registry: &Registry, token: Token) -> io::Result<()> {
        if matches!(self.phase, Phase::Connect(_)) {
            return Ok(());
        }
        let blocked = self.write_some()?;
        if blocked != self.writable {
            self.writable = blocked;
            let i = if blocked {
                Interest::READABLE | Interest::WRITABLE
            } else {
                Interest::READABLE
            };
            registry.reregister(self.sock.source(), token, i)?;
        }
        Ok(())
    }

    /// true: bytes wait for the socket.
    fn write_some(&mut self) -> io::Result<bool> {
        while !self.raw.is_empty() {
            match self.sock.write(&self.raw) {
                Ok(0) => return Err(ErrorKind::WriteZero.into()),
                Ok(n) => drop(self.raw.drain(..n)),
                Err(e) if e.kind() == ErrorKind::WouldBlock => return Ok(true),
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        if let Some(tls) = self.tls.as_mut() {
            if self.out_at < self.out.len() {
                tls.writer().write_all(&self.out[self.out_at..])?;
                self.out_at = self.out.len();
            }
            while tls.wants_write() {
                match tls.write_tls(&mut self.sock) {
                    Ok(0) => return Err(ErrorKind::WriteZero.into()),
                    Ok(_) => {}
                    Err(e) if e.kind() == ErrorKind::WouldBlock => return Ok(true),
                    Err(e) if e.kind() == ErrorKind::Interrupted => {}
                    Err(e) => return Err(e),
                }
            }
        }
        while self.out_at < self.out.len() {
            match self.sock.write(&self.out[self.out_at..]) {
                Ok(0) => return Err(ErrorKind::WriteZero.into()),
                Ok(n) => self.out_at += n,
                Err(e) if e.kind() == ErrorKind::WouldBlock => return Ok(true),
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        self.out.clear();
        self.out_at = 0;
        if self.out.capacity() > BIG_BUF {
            self.out = Vec::with_capacity(4096);
        }
        Ok(false)
    }

    /// Reads what the socket has. Ok(false): the server closed it.
    /// `closed`: the event said the server closed its side, so read on to
    /// the end (the close comes with no new event).
    fn fill(&mut self, closed: bool) -> io::Result<bool> {
        let Conn {
            sock,
            tls,
            inb,
            in_s,
            in_e,
            ..
        } = self;
        let Some(tls) = tls.as_mut() else {
            loop {
                room(inb, in_s, in_e, READ_ROOM);
                let free = inb.len() - *in_e;
                match sock.read(&mut inb[*in_e..]) {
                    Ok(0) => return Ok(false),
                    Ok(n) => {
                        *in_e += n;
                        // A short read took all there was. More data later
                        // is a new edge, so no read just to see WouldBlock.
                        // Not on Windows: mio wants reads until WouldBlock.
                        if cfg!(unix) && n < free && !closed {
                            return Ok(true);
                        }
                    }
                    Err(e) if e.kind() == ErrorKind::WouldBlock => return Ok(true),
                    Err(e) if e.kind() == ErrorKind::Interrupted => {}
                    Err(e) => return Err(e),
                }
            }
        };
        loop {
            let eof = match tls.read_tls(sock) {
                Ok(0) => true,
                Ok(_) => false,
                Err(e) if e.kind() == ErrorKind::WouldBlock => return Ok(true),
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            };
            let state = tls.process_new_packets().map_err(io::Error::other)?;
            let mut left = state.plaintext_bytes_to_read();
            while left > 0 {
                room(inb, in_s, in_e, left);
                let n = tls.reader().read(&mut inb[*in_e..])?;
                if n == 0 {
                    break;
                }
                *in_e += n;
                left = left.saturating_sub(n);
            }
            if eof || state.peer_has_closed() {
                return Ok(false);
            }
        }
    }

    /// Reads the whole packets in `inb`. Err: the connection is broken.
    fn process(&mut self, ready: &mut Vec<u8>, now: Instant) -> Result<(), Fail> {
        let inb = std::mem::take(&mut self.inb);
        let mut p = self.in_s;
        let mut res = Ok(());
        while !self.paused()
            && let Some((payload, seq, used)) = proto::next_packet(&inb[p..self.in_e])
        {
            p += used;
            self.seq = seq;
            res = if self.ready() {
                self.packet(&payload, ready, now)
            } else {
                self.login(&payload, ready)
            };
            if res.is_err() {
                break;
            }
        }
        self.inb = inb;
        self.in_s = p;
        if self.in_s == self.in_e {
            self.in_s = 0;
            self.in_e = 0;
            if self.inb.len() > BIG_BUF {
                self.inb = Vec::new();
            }
        }
        res
    }

    // ---- login ----

    fn reply(&mut self, payload: &[u8]) {
        self.seq = self.seq.wrapping_add(1);
        proto::packet(&mut self.out, self.seq, payload);
    }

    /// A packet of the login.
    fn login(&mut self, b: &[u8], ready: &mut Vec<u8>) -> Result<(), Fail> {
        let shared = self.shared.clone();
        let o = &shared.opts;
        let pw = &o.pass;
        // the password may go in clear over TLS or a Unix socket
        let secure = self.tls.is_some() || !matches!(self.sock, Sock::Tcp(_));
        match &mut self.phase {
            Phase::Greeting => {
                let g = proto::greeting(b)?;
                self.maria = g.maria;
                self.thread = g.thread;
                let mut caps = CAPS;
                if o.db.is_some() {
                    caps |= proto::CONNECT_WITH_DB;
                }
                if o.found_rows {
                    caps |= proto::FOUND_ROWS;
                }
                let tls = shared.tls.clone().filter(|_| o.socket.is_none());
                if tls.is_some() {
                    if g.caps & proto::SSL == 0 {
                        return Err(Fail::client(
                            "dox_mysql_native: the server does not support TLS",
                        ));
                    }
                    caps |= proto::SSL;
                }
                // MariaDB leaves out LONG_PASSWORD: then the login has its
                // extra capabilities (we ask for none)
                caps &= g.caps | proto::SSL;
                let plugin = match g.plugin.as_str() {
                    "caching_sha2_password" => "caching_sha2_password",
                    _ => "mysql_native_password",
                };
                let auth =
                    proto::scramble(plugin, pw.as_bytes(), &g.nonce, false).unwrap_or_default();
                let login = proto::login(caps, &o.user, &auth, o.db.as_deref(), plugin);
                if let Some(cfg) = tls {
                    let name = rustls::pki_types::ServerName::try_from(o.host.clone())
                        .map_err(|e| io_fail(format!("TLS name {}: {e}", o.host)))?;
                    let mut c = rustls::ClientConnection::new(cfg, name).map_err(io_fail)?;
                    c.set_buffer_limit(None);
                    let ssl = proto::ssl_request(caps);
                    self.seq = self.seq.wrapping_add(1);
                    proto::packet(&mut self.raw, self.seq, &ssl);
                    self.tls = Some(Box::new(c));
                }
                self.reply(&login);
                self.phase = Phase::Auth {
                    plugin: plugin.to_string(),
                    nonce: g.nonce,
                    key_asked: false,
                };
            }
            Phase::Auth {
                plugin,
                nonce,
                key_asked,
            } => match b.first() {
                Some(0x00) => {
                    self.phase = Phase::Ready;
                    self.release_held(ready);
                }
                Some(0xFF) => return Err(proto::err_packet(b)),
                Some(0xFE) => {
                    let (p, n) = proto::auth_switch(b);
                    let Some(auth) = proto::scramble(&p, pw.as_bytes(), &n, o.cleartext) else {
                        return Err(Fail::Client(format!(
                            "dox_mysql_native: the auth plugin {p} is not supported"
                        )));
                    };
                    *plugin = p;
                    *nonce = n;
                    *key_asked = false;
                    self.reply(&auth);
                }
                Some(0x01) if plugin == "caching_sha2_password" => {
                    if *key_asked {
                        let enc = proto::rsa_password(&b[1..], pw.as_bytes(), nonce)?;
                        self.reply(&enc);
                    } else if b.get(1) == Some(&4) {
                        // full login: the password in clear over TLS or a
                        // Unix socket, else RSA encrypted
                        if secure {
                            let mut a = pw.as_bytes().to_vec();
                            a.push(0);
                            self.reply(&a);
                        } else {
                            *key_asked = true;
                            self.reply(&[2]);
                        }
                    }
                    // 3: the fast login worked, an OK packet follows
                }
                _ => {
                    return Err(Fail::client(
                        "dox_mysql_native: unexpected packet in the login",
                    ));
                }
            },
            Phase::Connect(_) | Phase::Ready => {}
        }
        Ok(())
    }

    // ---- answers ----

    /// A packet of the answer to the first waiting command.
    fn packet(&mut self, b: &[u8], ready: &mut Vec<u8>, now: Instant) -> Result<(), Fail> {
        self.last_io = now;
        let maria = self.maria;
        let Some(w) = self.waits.front_mut() else {
            // eg. the server closes an idle connection
            if b.first() == Some(&0xFF) {
                return Err(proto::err_packet(b));
            }
            return Err(Fail::client("dox_mysql_native: an answer for no command"));
        };
        let bad = || Fail::client("dox_mysql_native: bad packet from the server");
        let head = b.first().copied().ok_or_else(bad)?;
        let mut finished = false;
        if let What::Prepare(slot) = w.what {
            match w.st {
                St::Head if head == 0xFF => {
                    let st = &mut self.stmts[slot];
                    st.failed = Some(proto::err_packet(b));
                    finished = true;
                }
                St::Head => {
                    let f = b.get(1..9).ok_or_else(bad)?;
                    let st = &mut self.stmts[slot];
                    st.id = Some(u32::from_le_bytes([f[0], f[1], f[2], f[3]]));
                    let cols = usize::from(u16::from_le_bytes([f[4], f[5]]));
                    st.params = usize::from(u16::from_le_bytes([f[6], f[7]]));
                    let left = cols + st.params;
                    if left == 0 {
                        finished = true;
                    } else {
                        w.st = St::Skip(left);
                    }
                }
                St::Skip(left) => {
                    if left == 1 {
                        finished = true;
                    } else {
                        w.st = St::Skip(left - 1);
                    }
                }
                _ => finished = true,
            }
            if finished {
                self.waits.pop_front();
                let st = &mut self.stmts[slot];
                if st.failed.is_some() {
                    if st.cached {
                        st.cached = false;
                        let sql = std::mem::take(&mut st.sql);
                        self.cache.remove(sql.as_str());
                    }
                    if st.refs == 0 {
                        self.stmts.remove(slot);
                    }
                }
                self.release_held(ready);
                self.evict(ready);
            }
            return Ok(());
        }
        let (rows, binary) = match w.what {
            What::Result { rows, binary } => (rows, binary),
            _ => (false, false),
        };
        match w.st {
            St::Head => match head {
                0x00 => {
                    let (affected, insert_id, status) = proto::ok_packet(b);
                    if w.first {
                        set_ok(&mut w.out, affected, insert_id);
                        w.first = false;
                    }
                    finished = status & proto::MORE_RESULTS == 0;
                }
                0xFF => {
                    w.fail.get_or_insert_with(|| proto::err_packet(b));
                    finished = true;
                }
                _ => {
                    let mut p = 0;
                    let n = proto::lenenc(b, &mut p).ok_or_else(bad)? as usize;
                    if w.first && rows {
                        let at = RES_HEAD + 16;
                        w.out[at..at + 4].copy_from_slice(&(n as u32).to_le_bytes());
                    }
                    w.st = if n == 0 { St::Rows } else { St::Cols(n) };
                    w.head_len = w.out.len();
                }
            },
            St::Cols(left) => {
                if w.first {
                    let mut c = proto::column(b).ok_or_else(bad)?;
                    if c.kind == crate::Kind::LongText && !maria {
                        c.kind = crate::Kind::Text;
                    }
                    if rows {
                        w.out
                            .extend_from_slice(&(c.name.len() as u16).to_le_bytes());
                        w.out.extend_from_slice(&c.name);
                    }
                    w.cols.push(c);
                    w.head_len = w.out.len();
                }
                w.st = if left == 1 {
                    St::Rows
                } else {
                    St::Cols(left - 1)
                };
            }
            St::Rows => {
                if head == 0xFF {
                    w.fail.get_or_insert_with(|| proto::err_packet(b));
                    finished = true;
                } else if proto::is_end(b) {
                    let (affected, insert_id, status) = proto::ok_packet(b);
                    if w.first {
                        set_ok(&mut w.out, affected, insert_id);
                        w.first = false;
                    }
                    if status & proto::MORE_RESULTS == 0 {
                        finished = true;
                    } else {
                        w.st = St::Head;
                    }
                } else if w.first && rows && !w.cancel {
                    let ok = if binary {
                        proto::binary_row(&mut w.out, b, &w.cols)
                    } else {
                        proto::text_row(&mut w.out, b, &w.cols)
                    };
                    ok.ok_or_else(bad)?;
                    w.count += 1;
                    if w.count == w.chunk {
                        w.deadline = now + self.shared.opts.timeout;
                        chunk(w, ready);
                    }
                }
            }
            St::Skip(_) => finished = true,
        }
        if finished {
            let w = self.waits.pop_front().unwrap();
            self.done(w, ready, now);
        }
        Ok(())
    }

    /// A command is answered: its record goes to Dart.
    fn done(&mut self, mut w: Wait, ready: &mut Vec<u8>, now: Instant) {
        if w.ddl {
            self.shared.schema.fetch_add(1, Ordering::Relaxed);
        }
        match w.what {
            What::Result { .. } => {
                if self.tx.is_some() {
                    self.used = now;
                }
                let r = match w.fail {
                    _ if w.cancel => Err(Fail::Cancelled),
                    Some(f) => Err(f),
                    None => {
                        let at = RES_HEAD + 20;
                        w.out[at..at + 4].copy_from_slice(&w.count.to_le_bytes());
                        Ok(w.out)
                    }
                };
                push(ready, record(w.id, r));
            }
            What::Begin(tx) => match w.fail {
                Some(f) => {
                    end_tx(tx);
                    self.tx = None;
                    push(ready, record(w.id, Err(f)));
                }
                None => {
                    self.used = now;
                    push(ready, record(w.id, Ok(ok_head(0, tx))));
                }
            },
            What::End => {
                self.ending = false;
                match w.fail {
                    Some(f) => {
                        push(ready, record(w.id, Err(f)));
                        // a failed commit: make sure nothing stays open
                        self.rollback(now, ready);
                    }
                    None => push(ready, record(w.id, Ok(ok_head(0, 0)))),
                }
            }
            What::Silent => self.ending = false,
            // 1094: unknown thread, the query or connection ended already
            What::Kill => match w.fail {
                Some(Fail::Server { code: 1094, .. }) | None => {}
                Some(f) => kill_failed(&f),
            },
            What::Prepare(_) | What::Reset => {}
        }
    }

    /// Resets the session (`reset_connection`, on by default in
    /// mysql_async): the server forgets session variables, temporary
    /// tables and prepared statements. Only for a connection with no
    /// command.
    fn reset(&mut self, now: Instant, ready: &mut Vec<u8>) {
        self.dirty = false;
        self.stmts.clear();
        self.cache.clear();
        self.put(Cmd::Reset(now + self.shared.opts.timeout), ready);
    }

    /// A ROLLBACK nobody waits for. The connection is free after it.
    fn rollback(&mut self, now: Instant, ready: &mut Vec<u8>) {
        if let Some(tx) = self.tx {
            end_tx(tx);
        }
        self.tx = None;
        self.ending = true;
        let w = Wait::new(0, What::Silent, now + self.shared.opts.timeout);
        self.put(Cmd::Query(w, "ROLLBACK".into()), ready);
    }

    /// Fails all its commands (the connection goes away).
    fn fail_all(&mut self, why: &Fail, ready: &mut Vec<u8>) {
        for w in self.waits.drain(..) {
            if w.id != 0 {
                push(ready, record(w.id, Err(why.clone())));
            }
        }
        for c in self.held.drain(..) {
            if let Cmd::Query(w, _) | Cmd::Exec(w, ..) = c
                && w.id != 0
            {
                push(ready, record(w.id, Err(why.clone())));
            }
        }
    }

    /// Says goodbye to the server (COM_QUIT), if the socket takes it now.
    fn quit(&mut self) {
        if self.ready() && self.tls.is_none() && !self.unsent() {
            let _ = self.sock.write(&[1, 0, 0, 0, 1]);
        }
    }
}

/// Room for at least `min` more bytes in a read buffer, `s..e` used.
fn room(b: &mut Vec<u8>, s: &mut usize, e: &mut usize, min: usize) {
    if b.len() - *e >= min {
        return;
    }
    if *s > 0 {
        b.copy_within(*s..*e, 0);
        *e -= *s;
        *s = 0;
    }
    if b.len() - *e < min {
        let size = (b.len() * 2).max(*e + min).max(READ_ROOM * 4);
        b.resize(size, 0);
    }
}

/// A KILL that did not work: the query may still run on the server. Said
/// on stderr, as there is no one to give the error to.
fn kill_failed(f: &Fail) {
    let why = match f {
        Fail::Server { code, message, .. } => format!("{message} (code {code})"),
        Fail::Client(m) => m.clone(),
        Fail::Cancelled => return,
    };
    eprintln!(
        "dox_mysql_native: warning: KILL of a cancelled or timed out query failed, \
         it may still run on the server: {why}"
    );
}

/// Sends the rows of a stream so far to Dart as a chunk record (status 2),
/// and starts the next chunk with the same column names.
fn chunk(w: &mut Wait, ready: &mut Vec<u8>) {
    let mut next = Vec::with_capacity(w.out.len());
    next.extend_from_slice(&w.out[..w.head_len]);
    let mut out = std::mem::replace(&mut w.out, next);
    let at = RES_HEAD + 20;
    out[at..at + 4].copy_from_slice(&w.count.to_le_bytes());
    let mut rec = record(w.id, Ok(out));
    rec[8] = 2;
    push(ready, rec);
    w.count = 0;
    w.credit = w.credit.saturating_sub(1);
}

/// Writes affected rows and insert id into an ok record (not for a
/// command with no record, eg. COMMIT).
fn set_ok(out: &mut [u8], affected: u64, insert_id: u64) {
    if out.len() < RES_HEAD + 16 {
        return;
    }
    out[RES_HEAD..RES_HEAD + 8].copy_from_slice(&affected.to_le_bytes());
    out[RES_HEAD + 8..RES_HEAD + 16].copy_from_slice(&insert_id.to_le_bytes());
}

/// Adds a record to the ready bytes. The first one moves, no copy.
fn push(ready: &mut Vec<u8>, rec: Vec<u8>) {
    if ready.is_empty() {
        *ready = rec;
    } else {
        ready.extend_from_slice(&rec);
    }
}

// ---- pools: what one link has of a pool ---------------------------------

/// A queued request: it waits for a connection.
struct Queued {
    req: Request,
    deadline: Instant,
}

struct Local {
    shared: Arc<Shared>,
    /// its connections in this link (keys of `Core::conns`)
    conns: Vec<usize>,
    queue: VecDeque<Queued>,
    /// in the `waiters` list of the pool
    waiter: bool,
    /// the pool had no connection to give in this turn: do not ask again
    full: bool,
    /// a new connection failed in this turn: the next requests get the
    /// same error, and do not open one more each
    failed: Option<Fail>,
    /// a request had to share a busy connection because the pool was full
    want: bool,
    /// in the `hungry` list of the pool
    hungry: bool,
}

/// How to get a connection from the pool.
enum Got {
    Idle(Box<Conn>),
    Open,
    None,
}

impl Shared {
    /// A connection for link `me`: an idle one, or room for a new one.
    /// Only the first waiter may take one while links wait.
    /// A link at its fair share takes none while others are hungry (the
    /// first waiter still may).
    fn acquire(&self, me: u64, link: &Weak<Link>) -> Got {
        let mut st = self.state.lock().unwrap();
        if self.closed.load(Ordering::Acquire) {
            return Got::None;
        }
        let front = st.waiters.front().map(|w| w.upgrade().map(|l| l.id));
        if front.is_some_and(|id| id != Some(me)) {
            return Got::None;
        }
        // the first waiter always may take one: else idle connections can
        // pile up while nobody may take them
        let mine = st.holders.get(&me).map_or(0, |h| h.1);
        if front.is_none()
            && st.hungry.iter().any(|&h| h != me)
            && mine >= self.share.load(Ordering::Acquire)
        {
            return Got::None;
        }
        let got = if let Some(c) = st.idle.pop() {
            Got::Idle(Box::new(c))
        } else if st.total < self.opts.max {
            st.total += 1;
            Got::Open
        } else {
            return Got::None;
        };
        st.holders.entry(me).or_insert_with(|| (link.clone(), 0)).1 += 1;
        self.update_share(&st);
        got
    }

    /// The fair share: max / (links that hold or wait), at least 1. Only
    /// changes when a link starts or stops holding or waiting.
    fn update_share(&self, st: &crate::PoolState) {
        let waiting = st
            .waiters
            .iter()
            .filter_map(Weak::upgrade)
            .filter(|l| !st.holders.contains_key(&l.id))
            .count();
        let active = (st.holders.len() + waiting).max(1);
        self.share
            .store((self.opts.max / active).max(1), Ordering::Release);
    }

    /// Adds or removes link `me` in the hungry list. A new hungry link
    /// pokes the links over their share, so they give back connections as
    /// these drain.
    fn set_hungry(&self, me: u64, on: bool) {
        let poke: Vec<Arc<Link>> = {
            let mut st = self.state.lock().unwrap();
            st.hungry.retain(|&h| h != me);
            let mut poke = Vec::new();
            if on {
                st.hungry.push(me);
                let share = self.share.load(Ordering::Acquire);
                poke = st
                    .holders
                    .iter()
                    .filter(|(id, h)| **id != me && h.1 > share)
                    .filter_map(|(_, (l, _))| l.upgrade())
                    .collect();
            }
            self.hungry.store(st.hungry.len(), Ordering::Release);
            poke
        };
        for l in poke {
            l.poke();
        }
    }

    /// One connection of link `me` is gone (closed, or `back` in idle).
    /// The first waiter gets a poke.
    fn put_back(&self, me: u64, back: Option<Conn>) {
        let first = {
            let mut st = self.state.lock().unwrap();
            if let Some(h) = st.holders.get_mut(&me) {
                h.1 -= 1;
                if h.1 == 0 {
                    st.holders.remove(&me);
                    st.hungry.retain(|&h| h != me);
                    self.hungry.store(st.hungry.len(), Ordering::Release);
                    self.update_share(&st);
                }
            }
            match back {
                Some(c) if !self.closed.load(Ordering::Acquire) => st.idle.push(c),
                _ => st.total -= 1,
            }
            first_waiter(&mut st)
        };
        if let Some(l) = first {
            l.poke();
        }
    }

    /// Adds or removes link `me` in the waiters. A new waiter pokes the
    /// links that hold connections, so they give back the ones they do
    /// not use. A waiter that leaves pokes the next one.
    fn set_waiter(&self, me: u64, link: &Weak<Link>, wait: bool) {
        let poke: Vec<Arc<Link>> = {
            let mut st = self.state.lock().unwrap();
            let poke = if wait {
                st.waiters.push_back(link.clone());
                st.holders
                    .iter()
                    .filter(|(id, _)| **id != me)
                    .filter_map(|(_, (l, _))| l.upgrade())
                    .collect()
            } else {
                st.waiters
                    .retain(|w| w.upgrade().is_some_and(|l| l.id != me));
                first_waiter(&mut st).into_iter().collect()
            };
            self.waiting.store(st.waiters.len(), Ordering::Release);
            self.update_share(&st);
            poke
        };
        for l in poke {
            l.poke();
        }
    }
}

/// The first waiter that is alive (dead ones leave the list).
fn first_waiter(st: &mut crate::PoolState) -> Option<Arc<Link>> {
    while let Some(w) = st.waiters.front() {
        match w.upgrade() {
            Some(l) if !l.stopped.load(Ordering::Acquire) => return Some(l),
            _ => {
                st.waiters.pop_front();
            }
        }
    }
    None
}

/// The last close of a pool: its idle connections close now. The links
/// get a poke: they fail their queued requests, and close their
/// connections of the pool once these have no command.
pub(crate) fn close_pool(shared: &Shared) {
    shared.closed.store(true, Ordering::Release);
    let (idle, links): (Vec<Conn>, Vec<Arc<Link>>) = {
        let mut st = shared.state.lock().unwrap();
        let idle = std::mem::take(&mut st.idle);
        st.total -= idle.len();
        let links = st
            .waiters
            .iter()
            .chain(st.holders.values().map(|(l, _)| l))
            .filter_map(Weak::upgrade)
            .collect();
        (idle, links)
    };
    for mut c in idle {
        c.quit();
    }
    for l in links {
        l.poke();
    }
}

// ---- the link of one isolate -------------------------------------------

struct Core {
    poll: Poll,
    events: Events,
    /// Windows: the helper polled these events, they wait for `turn`
    stored: bool,
    /// by token
    conns: Slab<Conn>,
    /// by the id of the pool
    pools: HashMap<u64, Local>,
    /// result records not given to Dart yet
    ready: Vec<u8>,
    now: Instant,
    /// the next timer: the first deadline of a request, or of an idle
    /// transaction
    next: Option<Instant>,
    id: u64,
    me: Weak<Link>,
    /// scratch: the events of one turn
    evs: Vec<(usize, bool, bool, bool)>,
    mail: Arc<Mail>,
    /// requests from other isolates that run here: our id to (their id,
    /// their link)
    foreign: HashMap<u64, (u64, Weak<Link>)>,
    next_foreign: u64,
}

/// A request from another link, and who sent it.
type FromLink = (Request, Weak<Link>);

/// What other links send to a link.
#[derive(Default)]
struct Mail {
    /// requests for transactions of this link, and who sent them. true:
    /// the link is gone, it takes no more.
    reqs: Mutex<(bool, Vec<FromLink>)>,
    /// result records of requests this link sent to other links
    results: Mutex<Vec<u8>>,
}

static NEXT_TX: AtomicU64 = AtomicU64::new(1);
static NEXT_LINK: AtomicU64 = AtomicU64::new(1);

impl Core {
    /// The pool of a request, as this link sees it. `seen`: the pools
    /// already looked up in this batch, so the registry is locked once per
    /// pool and batch.
    fn local(&mut self, handle: u64, seen: &mut Vec<(u64, Option<u64>)>) -> Option<u64> {
        if let Some((_, id)) = seen.iter().find(|(h, _)| *h == handle) {
            return *id;
        }
        let id = pool_of(handle)
            .filter(|s| !s.closed.load(Ordering::Acquire))
            .map(|shared| {
                let id = shared.id;
                self.pools.entry(id).or_insert_with(|| Local {
                    shared,
                    conns: Vec::new(),
                    queue: VecDeque::new(),
                    waiter: false,
                    full: false,
                    failed: None,
                    want: false,
                    hungry: false,
                });
                id
            });
        seen.push((handle, id));
        id
    }

    fn fail(&mut self, id: u64, f: Fail) {
        push(&mut self.ready, record(id, Err(f)));
    }

    /// Reads a batch of request records and sends them.
    fn submit(&mut self, b: &[u8]) {
        self.now = Instant::now();
        for l in self.pools.values_mut() {
            l.full = false;
            l.failed = None;
        }
        let mut seen = Vec::new();
        let mut r = Reader(b);
        while let Some(req) = read_request(&mut r) {
            if req.op == OP_CANCEL || req.op == OP_MORE {
                self.control(req);
                continue;
            }
            if req.tx != 0 {
                self.in_tx(req);
                continue;
            }
            if req.op == OP_COMMIT || req.op == OP_ROLLBACK {
                self.fail(req.id, Fail::client(TX_CLOSED));
                continue;
            }
            let Some(pid) = self.local(req.pool, &mut seen) else {
                self.fail(req.id, Fail::client(POOL_CLOSED));
                continue;
            };
            let deadline = self.now + self.pools[&pid].shared.opts.timeout;
            if let Err(req) = self.place(pid, req, deadline) {
                self.pools
                    .get_mut(&pid)
                    .unwrap()
                    .queue
                    .push_back(Queued { req, deadline });
            }
        }
        let ids: Vec<u64> = seen.iter().filter_map(|(_, id)| *id).collect();
        for pid in ids {
            self.balance(pid);
        }
        self.kills();
        self.timers();
        self.flush_all();
        self.route();
    }

    /// A cancel or more record from Dart (its params: the id of the
    /// request, and for more a count). A request of another isolate's
    /// transaction: the record goes there too.
    fn control(&mut self, req: Request) {
        let Some(Param::Int(id)) = req.params.first() else {
            return;
        };
        let n = match req.params.get(1) {
            Some(Param::Int(n)) => (*n).clamp(0, i64::from(u32::MAX)) as u32,
            _ => 0,
        };
        if !self.control_here(req.op, *id as u64, n) && req.tx != 0 {
            self.forward(req);
        }
    }

    /// Cancels request `id` (op cancel), or lets its stream read `n` more
    /// chunks (op more). false: it is not in this link.
    fn control_here(&mut self, op: u8, id: u64, n: u32) -> bool {
        let found = self.conns.iter().find_map(|(k, c)| {
            let at = c.waits.iter().position(|w| w.id == id);
            at.map(|i| (k, i))
        });
        if let Some((k, i)) = found {
            let c = &mut self.conns[k];
            let timeout = c.shared.opts.timeout;
            let w = &mut c.waits[i];
            if op == OP_CANCEL {
                w.cancel = true;
                if !c.shared.opts.server_kill && matches!(w.what, What::Result { .. }) {
                    // no KILL: Dart gets the error now, and the query runs
                    // to its end with its rows dropped (its id is gone)
                    push(&mut self.ready, record(w.id, Err(Fail::Cancelled)));
                    w.id = 0;
                }
            } else {
                w.credit = w.credit.saturating_add(n);
                // progress: Dart takes the chunks
                w.deadline = self.now + timeout;
            }
            if i == 0 {
                // read on: a paused stream may go again. As if the socket
                // was closed, so it reads to WouldBlock (the edge is gone).
                self.event(k, true, false, true);
            }
            return true;
        }
        if op != OP_CANCEL {
            return false;
        }
        for l in self.pools.values_mut() {
            if let Some(i) = l.queue.iter().position(|q| q.req.id == id) {
                l.queue.remove(i);
                self.fail(id, Fail::Cancelled);
                return true;
            }
        }
        for (_, c) in self.conns.iter_mut() {
            let Some(i) = c
                .held
                .iter()
                .position(|cmd| matches!(cmd, Cmd::Query(w, _) | Cmd::Exec(w, ..) if w.id == id))
            else {
                continue;
            };
            if let Some(Cmd::Exec(_, slot, _)) = c.held.remove(i) {
                let st = &mut c.stmts[slot];
                st.refs -= 1;
                if st.refs == 0 && st.failed.is_some() {
                    c.stmts.remove(slot);
                }
            }
            push(&mut self.ready, record(id, Err(Fail::Cancelled)));
            return true;
        }
        false
    }

    /// Stops cancelled requests that run on the server, with `KILL QUERY`
    /// on a new connection (the server stops the query; the connection
    /// stays fine). Only when the request is alone on its connection:
    /// with a pipelined command behind it, a late KILL could stop that
    /// one instead. Then it drains: it runs to its end, its rows are
    /// dropped, and Dart gets the cancelled error.
    /// A pool connection that got a KILL takes no new command and closes
    /// once it has none, so no killed state can stay on it.
    // ponytail: one new connection per kill. Cancels are rare.
    fn kills(&mut self) {
        let now = self.now;
        let mut close = Vec::new();
        let mut send = Vec::new();
        for (k, c) in self.conns.iter() {
            let Some(kill) = &c.kill else { continue };
            if kill.sent {
                if c.load() == 0 {
                    close.push(k);
                }
            } else if now >= kill.until {
                kill_failed(&Fail::client("no answer in time"));
                close.push(k);
            } else if c.ready() {
                send.push(k);
            }
        }
        for k in send {
            let Some(kill) = &self.conns[k].kill else {
                continue;
            };
            let (target, thread) = (kill.target, kill.thread);
            // the request still runs, alone: else KILL could hit the next
            // command (in a transaction, more commands can come after the
            // kill started). Then it drains.
            // (new commands are held while the kill runs, see `hold`)
            let runs = target.is_none_or(|(target, id)| {
                self.conns
                    .get(target)
                    .is_some_and(|t| t.waits.len() == 1 && t.waits[0].id == id)
            });
            if !runs {
                close.push(k);
                continue;
            }
            let c = &mut self.conns[k];
            if let Some(kill) = c.kill.as_mut() {
                kill.sent = true;
            }
            let w = Wait::new(0, What::Kill, now + c.shared.opts.timeout);
            let sql = match target {
                Some(_) => format!("KILL QUERY {thread}"),
                None => format!("KILL {thread}"),
            };
            c.put(Cmd::Query(w, sql), &mut self.ready);
        }
        for k in close {
            self.close(k, None);
        }
        let mut start = Vec::new();
        for (k, c) in self.conns.iter_mut() {
            let alone = c.waits.len() == 1 && c.held.is_empty();
            if c.kill.is_some() || c.thread == 0 || !alone || !c.shared.opts.server_kill {
                continue;
            }
            let w = &mut c.waits[0];
            if w.cancel && !w.killing {
                w.killing = true;
                if c.tx.is_none() {
                    c.doomed = true;
                }
                start.push((k, w.id, c.thread, c.shared.clone()));
            }
        }
        for (target, id, thread, shared) in start {
            let kill = self.start_kill(shared, thread, Some((target, id)));
            self.conns[target].hold = kill;
        }
    }

    /// Opens a kill connection for server thread `thread` (see [Kill]).
    /// When it can not connect, nothing is killed: a cancelled request
    /// drains.
    /// Returns its key.
    fn start_kill(
        &mut self,
        shared: Arc<Shared>,
        thread: u32,
        target: Option<(usize, u64)>,
    ) -> Option<usize> {
        let now = self.now;
        let until = now + shared.opts.timeout;
        {
            let mut c = match Conn::start(shared, now) {
                Ok(c) => c,
                Err(f) => {
                    kill_failed(&f);
                    return None;
                }
            };
            c.kill = Some(Kill {
                target,
                thread,
                sent: false,
                until,
            });
            let entry = self.conns.vacant_entry();
            let i = Interest::READABLE | Interest::WRITABLE;
            if self
                .poll
                .registry()
                .register(c.sock.source(), Token(entry.key()), i)
                .is_ok()
            {
                let key = entry.key();
                entry.insert(c);
                return Some(key);
            }
            None
        }
    }

    /// A request in a transaction: it goes to the connection of the
    /// transaction. Commit and rollback end it.
    // ponytail: a linear scan of the connections of this link.
    fn in_tx(&mut self, mut req: Request) {
        let Some(key) = self
            .conns
            .iter()
            .find(|(_, c)| c.tx == Some(req.tx))
            .map(|(k, _)| k)
        else {
            return self.forward(req);
        };
        let c = &mut self.conns[key];
        c.used = self.now;
        c.dirty = true;
        let deadline = self.now + c.shared.opts.timeout;
        match req.op {
            OP_COMMIT | OP_ROLLBACK => {
                end_tx(req.tx);
                c.tx = None;
                c.ending = true;
                let sql = if req.op == OP_COMMIT {
                    "COMMIT"
                } else {
                    "ROLLBACK"
                };
                c.put(
                    Cmd::Query(Wait::new(req.id, What::End, deadline), sql.into()),
                    &mut self.ready,
                );
            }
            OP_QUERY | OP_EXECUTE | OP_STREAM => {
                let w = Wait::of(&mut req, deadline);
                c.query(w, req.sql, req.params, &mut self.ready);
            }
            op => self.fail(req.id, Fail::Client(format!("unknown op {op}"))),
        }
    }

    /// A request for a transaction of another isolate: it goes to the link
    /// that owns the transaction, which gets a poke. Its result comes back
    /// in our mail. The owner is gone: the transaction is closed (the
    /// server rolled it back when the connection closed).
    fn forward(&mut self, req: Request) {
        let owner = TXS.lock().unwrap().get(&req.tx).and_then(Weak::upgrade);
        let Some(owner) = owner.filter(|o| o.id != self.id) else {
            return self.fail(req.id, Fail::client(TX_CLOSED));
        };
        let mut q = owner.mail.reqs.lock().unwrap();
        if q.0 {
            drop(q);
            return self.fail(req.id, Fail::client(TX_CLOSED));
        }
        q.1.push((req, self.me.clone()));
        drop(q);
        owner.poke();
    }

    /// Runs the requests other isolates sent for our transactions, and
    /// takes the results of the ones we sent.
    fn read_mail(&mut self) {
        let results = std::mem::take(&mut *self.mail.results.lock().unwrap());
        if !results.is_empty() {
            self.ready.extend_from_slice(&results);
        }
        let reqs = std::mem::take(&mut self.mail.reqs.lock().unwrap().1);
        for (mut req, from) in reqs {
            if req.op == OP_CANCEL || req.op == OP_MORE {
                // their id of the request: find ours
                let theirs = match req.params.first() {
                    Some(Param::Int(id)) => *id as u64,
                    _ => continue,
                };
                let ours = self
                    .foreign
                    .iter()
                    .find(|(_, (t, f))| *t == theirs && f.ptr_eq(&from))
                    .map(|(k, _)| *k);
                if let Some(ours) = ours {
                    req.params[0] = Param::Int(ours as i64);
                    req.tx = 0; // never forwarded again
                    self.control(req);
                }
                continue;
            }
            self.next_foreign += 1;
            let id = FOREIGN | self.next_foreign;
            self.foreign.insert(id, (req.id, from));
            req.id = id;
            self.in_tx(req);
        }
    }

    /// Moves the records of requests from other isolates out of `ready`,
    /// to the mail of the isolate that sent them.
    fn route(&mut self) {
        if self.foreign.is_empty() || self.ready.is_empty() {
            return;
        }
        let all = std::mem::take(&mut self.ready);
        let mut out: Vec<(Weak<Link>, Vec<u8>)> = Vec::new();
        let mut p = 0;
        while p + RES_HEAD <= all.len() {
            let id = u64::from_le_bytes(all[p..p + 8].try_into().unwrap());
            let len = u32::from_le_bytes(all[p + 12..p + 16].try_into().unwrap()) as usize;
            let rec = &all[p..p + RES_HEAD + len];
            p += RES_HEAD + len;
            // a chunk of a stream (status 2): more records follow, so the
            // route stays (also for its more and cancel records)
            let route = if rec[8] == 2 {
                self.foreign.get(&id).cloned()
            } else {
                self.foreign.remove(&id)
            };
            let Some((theirs, from)) = route else {
                self.ready.extend_from_slice(rec);
                continue;
            };
            let at = match out.iter().position(|(l, _)| l.ptr_eq(&from)) {
                Some(i) => i,
                None => {
                    out.push((from, Vec::new()));
                    out.len() - 1
                }
            };
            let b = &mut out[at].1;
            b.extend_from_slice(&theirs.to_le_bytes());
            b.extend_from_slice(&rec[8..]);
        }
        for (to, b) in out {
            if let Some(l) = to.upgrade() {
                l.mail.results.lock().unwrap().extend_from_slice(&b);
                l.poke();
            }
        }
    }

    /// Sends a request of pool `pid` on a connection of this link. Gives
    /// it back when there is none (the pool is at its limit).
    fn place(&mut self, pid: u64, mut req: Request, deadline: Instant) -> Result<(), Request> {
        if !matches!(req.op, OP_QUERY | OP_EXECUTE | OP_BEGIN | OP_STREAM) {
            self.fail(req.id, Fail::Client(format!("unknown op {}", req.op)));
            return Ok(());
        }
        let begin = req.op == OP_BEGIN;
        // a begin or a stream gets a connection of its own: a stream can
        // run long, and nothing should wait behind it
        let alone = begin || req.op == OP_STREAM;
        let key = loop {
            let best = self.pools[&pid]
                .conns
                .iter()
                .copied()
                .filter(|&k| self.conns[k].free())
                .min_by_key(|&k| self.conns[k].load());
            if let Some(k) = best.filter(|&k| self.conns[k].load() == 0) {
                if self.fresh(k) {
                    break k;
                }
                continue; // it was too old and is closed now
            }
            if let Some(k) = self.more(pid) {
                break k;
            }
            if let Some(f) = self.pools[&pid].failed.clone() {
                self.fail(req.id, f);
                return Ok(());
            }
            // pipeline on the least busy one (not a begin or a stream)
            match best.filter(|_| !alone) {
                Some(k) => {
                    let l = self.pools.get_mut(&pid).unwrap();
                    l.want |= l.full;
                    break k;
                }
                None => return Err(req),
            }
        };
        let now = self.now;
        let c = &mut self.conns[key];
        c.dirty = true;
        if begin {
            let tx = NEXT_TX.fetch_add(1, Ordering::Relaxed);
            TXS.lock().unwrap().insert(tx, self.me.clone());
            c.tx = Some(tx);
            c.used = now;
            let w = Wait::new(req.id, What::Begin(tx), deadline);
            c.put(Cmd::Query(w, "START TRANSACTION".into()), &mut self.ready);
        } else {
            // no new command goes to a streaming connection
            c.release |= req.op == OP_STREAM;
            let w = Wait::of(&mut req, deadline);
            c.query(w, req.sql, req.params, &mut self.ready);
        }
        Ok(())
    }

    /// false when an idle connection was quiet for longer than `conn_ttl`:
    /// then it is closed.
    fn fresh(&mut self, key: usize) -> bool {
        let old = !self.fresh_check(key);
        if old {
            self.close(key, None);
        }
        !old
    }

    /// One more connection for pool `pid`: an idle one of the pool, or a
    /// new one. None at the limit.
    fn more(&mut self, pid: u64) -> Option<usize> {
        loop {
            let l = self.pools.get_mut(&pid)?;
            if l.full || l.failed.is_some() {
                return None;
            }
            let shared = l.shared.clone();
            let conn = match shared.acquire(self.id, &self.me) {
                Got::None => {
                    l.full = true;
                    return None;
                }
                Got::Idle(c) if c.sock.quiet() => *c,
                Got::Idle(_) => {
                    // the server closed it meanwhile
                    shared.put_back(self.id, None);
                    continue;
                }
                Got::Open => match Conn::start(shared.clone(), self.now) {
                    Ok(c) => c,
                    Err(f) => {
                        shared.put_back(self.id, None);
                        l.failed = Some(f);
                        return None;
                    }
                },
            };
            return self.adopt(pid, conn);
        }
    }

    /// Registers a connection in this poll.
    fn adopt(&mut self, pid: u64, mut c: Conn) -> Option<usize> {
        let entry = self.conns.vacant_entry();
        let key = entry.key();
        let i = if matches!(c.phase, Phase::Connect(_)) || c.unsent() {
            c.writable = true;
            Interest::READABLE | Interest::WRITABLE
        } else {
            c.writable = false;
            Interest::READABLE
        };
        if let Err(e) = self
            .poll
            .registry()
            .register(c.sock.source(), Token(key), i)
        {
            let shared = c.shared.clone();
            shared.put_back(self.id, None);
            self.pools.get_mut(&pid)?.failed = Some(io_fail(e));
            return None;
        }
        c.release = false;
        entry.insert(c);
        self.pools.get_mut(&pid)?.conns.push(key);
        Some(key)
    }

    /// Takes a connection out of this link: closes it (`why`: its commands
    /// fail with it), or gives it to the idle list of the pool.
    fn take(&mut self, key: usize) -> Conn {
        let mut c = self.conns.remove(key);
        let _ = self.poll.registry().deregister(c.sock.source());
        if let Some(l) = self.pools.get_mut(&c.shared.id) {
            l.conns.retain(|&k| k != key);
        }
        c
    }

    fn close(&mut self, key: usize, why: Option<Fail>) {
        let mut c = self.take(key);
        if let Some(tx) = c.tx {
            end_tx(tx);
        }
        // a DDL that got no answer (it timed out, or the connection
        // dropped): count the schema change again, as `done` does. Else a
        // statement that another connection prepared while it ran keeps
        // the old types.
        if c.waits.iter().any(|w| w.ddl) {
            c.shared.schema.fetch_add(1, Ordering::Relaxed);
        }
        if let Some(kill) = &c.kill {
            if let Some(f) = &why {
                kill_failed(f);
            }
            // the target may take new commands again
            if let Some((t, _)) = kill.target
                && let Some(t) = self.conns.get_mut(t)
                && t.hold == Some(key)
            {
                t.hold = None;
                t.release_held(&mut self.ready);
            }
        }
        if let Some(f) = why {
            c.fail_all(&f, &mut self.ready);
        }
        c.quit();
        let shared = c.shared.clone();
        let counted = c.kill.is_none();
        drop(c);
        if counted {
            shared.put_back(self.id, None);
        }
    }

    fn park(&mut self, key: usize) {
        let c = self.take(key);
        let shared = c.shared.clone();
        shared.put_back(self.id, Some(c));
    }

    /// Keeps the pool fair, after each batch and each turn:
    /// 1. a closed pool fails its queue, and its empty connections close,
    /// 2. empty connections close after `conn_ttl`, or go to the idle list
    ///    when another link waits,
    /// 3. queued requests go out,
    /// 4. this link joins or leaves the waiters,
    /// 5. while others wait (or a begin is queued), one busy connection is
    ///    marked: no new command goes to it, and it goes back once empty.
    fn balance(&mut self, pid: u64) {
        let Some(l) = self.pools.get(&pid) else {
            return;
        };
        let shared = l.shared.clone();
        let closed = shared.closed.load(Ordering::Acquire);
        if closed {
            let l = self.pools.get_mut(&pid).unwrap();
            let q: Vec<Queued> = l.queue.drain(..).collect();
            for q in q {
                self.fail(q.req.id, Fail::client(POOL_CLOSED));
            }
        }
        let others = |l: &Local| shared.waiting.load(Ordering::Acquire) > usize::from(l.waiter);
        // others are hungry and this link holds more than its share
        let over = |l: &Local| {
            shared.hungry.load(Ordering::Acquire) > usize::from(l.hungry)
                && l.conns.len() > shared.share.load(Ordering::Acquire)
        };
        let empty: Vec<usize> = self.pools[&pid]
            .conns
            .iter()
            .copied()
            .filter(|&k| self.conns[k].empty())
            .collect();
        for k in empty {
            let l = &self.pools[&pid];
            if closed || self.conns[k].doomed || !self.fresh_check(k) {
                self.close(k, None);
            } else if self.conns[k].dirty && shared.opts.reset {
                // it is not empty now: it may go back after the reset
                let now = self.now;
                self.conns[k].reset(now, &mut self.ready);
            } else if (others(l) && !(l.waiter && self.first_is_me(&shared))) || over(l) {
                self.park(k);
            } else {
                self.conns[k].release = false;
            }
        }
        if closed {
            let l = self.pools.get_mut(&pid).unwrap();
            if std::mem::take(&mut l.waiter) {
                shared.set_waiter(self.id, &self.me, false);
            }
            if std::mem::take(&mut l.hungry) {
                shared.set_hungry(self.id, false);
            }
            if l.conns.is_empty() {
                self.pools.remove(&pid);
            }
            return;
        }
        // 3. the queue, in order (a request that can not go stays)
        let n = self.pools[&pid].queue.len();
        for _ in 0..n {
            let q = self.pools.get_mut(&pid).unwrap().queue.pop_front().unwrap();
            if let Err(req) = self.place(pid, q.req, q.deadline) {
                self.pools.get_mut(&pid).unwrap().queue.push_back(Queued {
                    req,
                    deadline: q.deadline,
                });
            }
        }
        // 4.
        let l = self.pools.get_mut(&pid).unwrap();
        let wait = !l.queue.is_empty();
        if wait != l.waiter {
            l.waiter = wait;
            shared.set_waiter(self.id, &self.me, wait);
        }
        // hungry: busy, under the share, and the pool was full. It stays
        // hungry until it has its share or has no busy connection.
        let share = shared.share.load(Ordering::Acquire);
        let l = self.pools.get_mut(&pid).unwrap();
        let n = l.conns.len();
        let hungry = if l.hungry {
            n < share && l.conns.iter().any(|&k| self.conns[k].load() > 0)
        } else {
            l.want && n < share
        };
        l.want = false;
        if hungry != l.hungry {
            l.hungry = hungry;
            shared.set_hungry(self.id, hungry);
        }
        // 5.
        let l = &self.pools[&pid];
        let begin = l
            .queue
            .iter()
            .any(|q| matches!(q.req.op, OP_BEGIN | OP_STREAM));
        if (others(l) || begin || over(l)) && !l.conns.iter().any(|&k| self.conns[k].release) {
            let pick = l
                .conns
                .iter()
                .copied()
                .filter(|&k| self.conns[k].free())
                .min_by_key(|&k| self.conns[k].load());
            if let Some(k) = pick {
                self.conns[k].release = true;
            }
        }
    }

    /// Like `fresh`, but does not close.
    fn fresh_check(&self, key: usize) -> bool {
        let c = &self.conns[key];
        let o = &c.shared.opts;
        let quiet = self.now.saturating_duration_since(c.last_io);
        if o.conn_ttl.is_some_and(|ttl| quiet > ttl) {
            return false;
        }
        if c.max_age
            .is_some_and(|a| self.now.saturating_duration_since(c.born) > a)
        {
            return false;
        }
        // inactive_connection_ttl: only over pool_min connections
        !(o.inactive_ttl > Duration::ZERO
            && quiet > o.inactive_ttl
            && c.shared.state.lock().unwrap().total > o.min)
    }

    /// When an empty connection will be too old, for the timers.
    fn ttl_due(&self, c: &Conn) -> Option<Instant> {
        let o = &c.shared.opts;
        let inactive = (o.inactive_ttl > Duration::ZERO).then_some(o.inactive_ttl);
        [
            o.conn_ttl.map(|t| c.last_io + t),
            c.max_age.map(|a| c.born + a),
            inactive.map(|t| c.last_io + t),
        ]
        .into_iter()
        .flatten()
        .filter(|&t| t > self.now)
        .min()
    }

    fn first_is_me(&self, shared: &Shared) -> bool {
        let mut st = shared.state.lock().unwrap();
        first_waiter(&mut st).is_some_and(|l| l.id == self.id)
    }

    /// Runs the timers: a request over [TIMEOUT] fails (on a connection:
    /// the connection closes, the server may still run it), an idle
    /// transaction over [TIMEOUT] is rolled back. Sets `next`.
    fn timers(&mut self) {
        let now = self.now;
        let mut next: Option<Instant> = None;
        let mut soon = |t: Instant| next = Some(next.map_or(t, |n: Instant| n.min(t)));
        let mut late = Vec::new();
        for l in self.pools.values_mut() {
            let t = l.shared.opts.timeout;
            l.queue.retain(|q| {
                if q.deadline <= now {
                    late.push((q.req.id, t));
                    false
                } else {
                    soon(q.deadline);
                    true
                }
            });
        }
        for (id, t) in late {
            self.fail(id, timed_out(t));
        }
        // an empty connection that gets too old: wake then to close it
        for (_, c) in self.conns.iter() {
            if let Some(k) = c.kill.as_ref().filter(|k| !k.sent) {
                soon(k.until);
            } else if c.empty()
                && let Some(t) = self.ttl_due(c)
            {
                soon(t);
            }
        }
        let mut dead = Vec::new();
        for (k, c) in self.conns.iter_mut() {
            if let Some(d) = c.deadline() {
                if d <= now {
                    dead.push(k);
                    continue;
                }
                soon(d);
            }
            let timeout = c.shared.opts.timeout;
            if c.tx.is_some() && c.load() == 0 {
                let due = c.used + timeout;
                if due <= now {
                    c.rollback(now, &mut self.ready);
                    soon(now + timeout);
                } else {
                    soon(due);
                }
            }
        }
        for k in dead {
            let c = &self.conns[k];
            let t = c.shared.opts.timeout;
            // closing the socket does not stop the query: the server only
            // sees it when it writes. So kill its server thread.
            if c.kill.is_none() && c.thread != 0 && !c.waits.is_empty() && c.shared.opts.server_kill
            {
                let (shared, thread) = (c.shared.clone(), c.thread);
                self.start_kill(shared, thread, None);
                // wake to give up on it, if it can not log in
                let until = now + t;
                next = Some(next.map_or(until, |n| n.min(until)));
            }
            self.close(k, Some(timed_out(t)));
        }
        self.next = next;
    }

    /// Writes the out buffers that have bytes and do not wait for
    /// writable.
    fn flush_all(&mut self) {
        let mut broken = Vec::new();
        for (k, c) in self.conns.iter_mut() {
            if c.writable || !c.unsent() {
                continue;
            }
            if let Err(e) = c.flush(self.poll.registry(), Token(k)) {
                broken.push((k, e));
            }
        }
        for (k, e) in broken {
            self.close(k, Some(io_fail(format!("write failed: {e}"))));
        }
    }

    /// One poll turn that does not wait: connects, reads and writes what
    /// the events say.
    fn turn(&mut self) {
        if !self.stored {
            let Core { poll, events, .. } = self;
            if poll.poll(events, Some(Duration::ZERO)).is_err() {
                return;
            }
        }
        self.stored = false;
        let mut evs = std::mem::take(&mut self.evs);
        evs.clear();
        for ev in self.events.iter() {
            if ev.token() == WAKER {
                continue;
            }
            let closed = ev.is_read_closed() || ev.is_error();
            let read = ev.is_readable() || closed;
            let write = ev.is_writable() || ev.is_write_closed() || ev.is_error();
            evs.push((ev.token().0, read, write, closed));
        }
        for &(k, read, write, closed) in &evs {
            if self.conns.contains(k) {
                self.event(k, read, write, closed);
            }
        }
        self.evs = evs;
    }

    fn event(&mut self, k: usize, read: bool, write: bool, closed: bool) {
        let registry = self.poll.registry();
        let c = &mut self.conns[k];
        if let Phase::Connect(addrs) = &mut c.phase {
            if !(read || write) {
                return;
            }
            match c.sock.connected() {
                Ok(false) => return,
                Ok(true) => {
                    c.sock.tune(c.shared.opts.nodelay, c.shared.opts.keepalive);
                    c.phase = Phase::Greeting;
                    c.writable = false;
                    if let Err(e) =
                        registry.reregister(c.sock.source(), Token(k), Interest::READABLE)
                    {
                        return self.close(k, Some(io_fail(e)));
                    }
                    return;
                }
                Err(e) => {
                    let why = dial_fail(&c.shared.opts, e);
                    if addrs.is_empty() {
                        c.shared.resolve_later();
                        return self.close(k, Some(why));
                    }
                    let _ = registry.deregister(c.sock.source());
                    match connect_next(&c.shared.opts, addrs) {
                        Ok(s) => {
                            c.sock = s;
                            let i = Interest::READABLE | Interest::WRITABLE;
                            if let Err(e) = registry.register(c.sock.source(), Token(k), i) {
                                return self.close(k, Some(io_fail(e)));
                            }
                            return;
                        }
                        Err(f) => {
                            c.shared.resolve_later();
                            return self.close(k, Some(f));
                        }
                    }
                }
            }
        }
        if read && !c.paused() {
            let open = match c.fill(closed) {
                Ok(open) => open,
                Err(e) => return self.close(k, Some(io_fail(format!("read failed: {e}")))),
            };
            if let Err(f) = c.process(&mut self.ready, self.now) {
                return self.close(k, Some(f));
            }
            if !open {
                let why = Fail::client("dox_mysql_native: the server closed the connection");
                return self.close(k, Some(why));
            }
        }
        let c = &mut self.conns[k];
        if (write || c.unsent())
            && let Err(e) = c.flush(registry, Token(k))
        {
            self.close(k, Some(io_fail(format!("write failed: {e}"))));
        }
    }

    /// Reads, writes, runs the timers and keeps the pools fair.
    fn service(&mut self) {
        self.now = Instant::now();
        for l in self.pools.values_mut() {
            l.full = false;
            l.failed = None;
        }
        self.read_mail();
        self.turn();
        let ids: Vec<u64> = self.pools.keys().copied().collect();
        for pid in ids {
            self.balance(pid);
        }
        self.kills();
        self.timers();
        self.flush_all();
        self.route();
    }

    /// The isolate is gone: close all connections, leave all lists.
    fn shutdown(&mut self) {
        // requests from other isolates fail: the transaction is closed
        let reqs = {
            let mut q = self.mail.reqs.lock().unwrap();
            q.0 = true;
            std::mem::take(&mut q.1)
        };
        for (mut req, from) in reqs {
            self.next_foreign += 1;
            let id = FOREIGN | self.next_foreign;
            self.foreign.insert(id, (req.id, from));
            req.id = id;
            self.fail(id, Fail::client(TX_CLOSED));
        }
        let keys: Vec<usize> = self.conns.iter().map(|(k, _)| k).collect();
        for k in keys {
            self.close(k, Some(Fail::client(TX_CLOSED)));
        }
        self.route();
        for (_, l) in self.pools.drain() {
            if l.waiter {
                l.shared.set_waiter(self.id, &self.me, false);
            }
            if l.hungry {
                l.shared.set_hungry(self.id, false);
            }
        }
        self.ready = Vec::new();
    }
}

/// The link of one isolate. Dart holds an `Arc` of it as handle.
pub(crate) struct Link {
    core: Mutex<Core>,
    /// the poll file (epoll or kqueue), watched by the idle helper
    #[cfg(unix)]
    fd: i32,
    /// wakes the poll: another link gave a connection, or the pool closed
    waker: Waker,
    /// requests wait and Dart is idle: the helper watches, and tells Dart
    /// when an answer comes
    armed: AtomicBool,
    /// another link poked this one: Dart must run
    poked: AtomicBool,
    /// the next timer, in nanoseconds after `base`. `u64::MAX`: none.
    due: AtomicU64,
    base: Instant,
    /// wakes the helper: Dart went idle, or a poke
    kick: Mutex<bool>,
    cv: Condvar,
    stopped: AtomicBool,
    port: i64,
    id: u64,
    mail: Arc<Mail>,
}

impl Link {
    fn arm(&self) {
        self.armed.store(true, Ordering::Release);
        self.kick();
    }

    fn kick(&self) {
        *self.kick.lock().unwrap() = true;
        self.cv.notify_one();
    }

    /// Tells this link to run soon (from another link or a pool close).
    fn poke(&self) {
        self.poked.store(true, Ordering::Release);
        let _ = self.waker.wake();
        self.kick();
    }

    fn set_due(&self, at: Option<Instant>) {
        let ns = at.map_or(u64::MAX, |t| {
            t.saturating_duration_since(self.base)
                .as_nanos()
                .min(u128::from(u64::MAX - 1)) as u64
        });
        self.due.store(ns, Ordering::Release);
    }

    /// How long until the next timer. None: no timer.
    fn due_in(&self) -> Option<Duration> {
        let ns = self.due.load(Ordering::Acquire);
        (ns != u64::MAX).then(|| {
            (self.base + Duration::from_nanos(ns)).saturating_duration_since(Instant::now())
        })
    }

    /// true when the poll file has an event within `wait`. Does not read
    /// the event and does not take the core: `dox_mysql_run` does that.
    #[cfg(unix)]
    fn ready_within(&self, wait: Duration) -> bool {
        let mut p = libc::pollfd {
            fd: self.fd,
            events: libc::POLLIN,
            revents: 0,
        };
        // round up, so a timer a bit in the future does not spin
        let ms = wait.as_micros().div_ceil(1000).min(i32::MAX as u128) as i32;
        let r = unsafe { libc::poll(&mut p, 1, ms) };
        r > 0 && p.revents & libc::POLLIN != 0
    }

    /// Windows has no poll file to watch: take the core for at most 1 ms
    /// at a time, so Dart waits at most that long. The events wait in the
    /// core for the next `turn`.
    #[cfg(not(unix))]
    fn ready_within(&self, wait: Duration) -> bool {
        let mut core = self.core.lock().unwrap();
        if !core.stored {
            let Core { poll, events, .. } = &mut *core;
            let _ = poll.poll(events, Some(wait.min(Duration::from_millis(1))));
            core.stored = !core.events.is_empty();
        }
        core.stored
    }

    /// The isolate is gone: close its connections, and free the link. Dart
    /// will never call it again, so the reference Dart held goes too.
    fn gone(&self) {
        if self.stopped.swap(true, Ordering::AcqRel) {
            return;
        }
        self.core.lock().unwrap().shutdown();
        // SAFETY: Dart got this pointer from `Arc::into_raw` and its isolate
        // is gone. The helper still holds its own `Arc`.
        unsafe { Arc::decrement_strong_count(self as *const Link) };
    }

    /// The idle helper: watches the poll file while Dart is idle and
    /// requests wait. Also checks every [PROBE] that the isolate is alive.
    fn helper(&self) {
        let mut probed = Instant::now();
        loop {
            {
                let mut k = self.kick.lock().unwrap();
                while !*k {
                    k = self.cv.wait_timeout(k, PROBE).unwrap().0;
                    if !*k && probed.elapsed() >= PROBE {
                        probed = Instant::now();
                        if !post(self.port, &[]) {
                            drop(k);
                            return self.gone();
                        }
                    }
                }
                *k = false;
            }
            if self.poked.swap(false, Ordering::AcqRel) && !post(self.port, &[1]) {
                return self.gone();
            }
            while self.armed.load(Ordering::Acquire) {
                let wait = self.due_in().map_or(PROBE, |d| d.min(PROBE));
                if self.ready_within(wait) || self.due_in() == Some(Duration::ZERO) {
                    // only the side that clears `armed` tells Dart
                    if self
                        .armed
                        .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                        && !post(self.port, &[1])
                    {
                        return self.gone();
                    }
                    break;
                }
                if probed.elapsed() >= PROBE {
                    probed = Instant::now();
                    if !post(self.port, &[]) {
                        return self.gone();
                    }
                }
            }
        }
    }
}

// ---- FFI ---------------------------------------------------------------

/// Opens the link of one isolate. Its idle helper posts to `send_port`:
/// `[1]` to run, an empty message to check that the isolate is alive.
/// Returns a handle (> 0), -1 when `dox_mysql_init` did not run, or -2
/// when there is no poll or thread.
#[unsafe(no_mangle)]
pub extern "C" fn dox_mysql_client(send_port: i64) -> i64 {
    if POST.get().is_none() {
        return -1;
    }
    let Ok(poll) = Poll::new() else { return -2 };
    let Ok(waker) = Waker::new(poll.registry(), WAKER) else {
        return -2;
    };
    #[cfg(unix)]
    let fd = {
        use std::os::fd::AsRawFd;
        poll.as_raw_fd()
    };
    let id = NEXT_LINK.fetch_add(1, Ordering::Relaxed);
    let mail = Arc::new(Mail::default());
    let link = Arc::new_cyclic(|me| Link {
        core: Mutex::new(Core {
            poll,
            events: Events::with_capacity(256),
            stored: false,
            conns: Slab::new(),
            pools: HashMap::new(),
            ready: Vec::new(),
            now: Instant::now(),
            next: None,
            id,
            me: me.clone(),
            evs: Vec::new(),
            mail: mail.clone(),
            foreign: HashMap::new(),
            next_foreign: 0,
        }),
        #[cfg(unix)]
        fd,
        waker,
        armed: AtomicBool::new(false),
        poked: AtomicBool::new(false),
        due: AtomicU64::new(u64::MAX),
        base: Instant::now(),
        kick: Mutex::new(false),
        cv: Condvar::new(),
        stopped: AtomicBool::new(false),
        port: send_port,
        id,
        mail,
    });
    let for_helper = link.clone();
    if std::thread::Builder::new()
        .name("dox-mysql-idle".into())
        .spawn(move || for_helper.helper())
        .is_err()
    {
        return -2;
    }
    Arc::into_raw(link) as i64
}

/// Sends a batch of request records. Each request goes out on a socket of
/// this link at once. Copies what it needs, so Dart can reuse its buffer.
/// Returns 1 when results are ready already (call `dox_mysql_run`), else
/// 0: the helper tells Dart when answers come.
///
/// # Safety
/// `link` must come from `dox_mysql_client` (0 does nothing). `ptr` must
/// point to `len` readable bytes (a null `ptr` or a negative `len` sends
/// nothing). Only one call per link at a time (one isolate).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dox_mysql_submit(link: i64, ptr: *const u8, len: i64) -> i64 {
    if link == 0 {
        return 0;
    }
    let s = unsafe { &*(link as *const Link) };
    if s.stopped.load(Ordering::Acquire) {
        return 0;
    }
    let b = unsafe { crate::bytes(ptr, len) };
    let mut core = s.core.lock().unwrap();
    core.submit(b);
    let has_ready = !core.ready.is_empty();
    let watch = core.next.is_some();
    s.set_due(core.next);
    drop(core);
    if has_ready {
        return 1;
    }
    if watch && !s.armed.load(Ordering::Acquire) {
        s.arm();
    }
    0
}

/// Reads the sockets of this link and writes the result records of the
/// finished requests to `buf`. Never waits. Returns the byte count (0:
/// nothing yet, the idle helper tells Dart when an answer comes), or -n:
/// the results need a buffer of at least n bytes (call again with a
/// bigger one).
///
/// # Safety
/// `link` must come from `dox_mysql_client` (0 does nothing). `buf` must
/// point to `cap` writable bytes (a null `buf` has room for 0 bytes).
/// Only one call per link at a time (one isolate).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dox_mysql_run(link: i64, buf: *mut u8, cap: i64) -> i64 {
    if link == 0 {
        return 0;
    }
    let cap = if buf.is_null() {
        0
    } else {
        cap.max(0) as usize
    };
    let s = unsafe { &*(link as *const Link) };
    if s.stopped.load(Ordering::Acquire) {
        return 0;
    }
    let mut core = s.core.lock().unwrap();
    s.armed.store(false, Ordering::Release);
    core.service();
    let n = core.ready.len();
    s.set_due(core.next);
    if n > cap {
        return -(n as i64);
    }
    if n > 0 {
        unsafe { std::ptr::copy_nonoverlapping(core.ready.as_ptr(), buf, n) };
    }
    core.ready.clear();
    if core.ready.capacity() > BIG_BUF {
        core.ready = Vec::new(); // do not keep a big buffer forever
    }
    let watch = core.next.is_some();
    drop(core);
    if watch {
        s.arm();
    }
    n as i64
}

#[cfg(test)]
mod tests {
    use super::{dial_fail, is_ddl};
    use crate::{Fail, parse_url};

    #[test]
    fn connect_error_names_host_and_port() {
        let o = parse_url("mysql://u:secret@db.local:3307/d").unwrap();
        let Fail::Client(text) = dial_fail(&o, "Connection refused") else {
            panic!("not a client error");
        };
        assert_eq!(
            text,
            "dox_mysql_native: can not connect to host db.local, port 3307: Connection refused"
        );
        assert!(!text.contains("secret"));
    }

    #[test]
    fn schema_changes_are_seen() {
        assert!(is_ddl("CREATE TABLE t (v int)"));
        assert!(is_ddl("  alter table t add w int"));
        assert!(is_ddl("\nDrop Table t"));
        assert!(is_ddl("RENAME TABLE a TO b"));
        assert!(!is_ddl("INSERT INTO t VALUES (1)"));
        assert!(!is_ddl("SELECT 'CREATE'"));
        assert!(!is_ddl("DRO"));
        assert!(is_ddl("/* migration 7 */ ALTER TABLE t ADD w int"));
        assert!(is_ddl("-- add w\n# and more\n  /* x */\tdrop table t"));
        assert!(!is_ddl("/* CREATE */ SELECT 1"));
        assert!(!is_ddl("-- CREATE TABLE t"));
        assert!(!is_ddl("/* not closed CREATE"));
        // `--` with no space after it is not a comment
        assert!(!is_ddl("--x\nCREATE TABLE t"));
        assert!(!is_ddl("--\tCREATE TABLE t"));
        assert!(is_ddl("--\nCREATE TABLE t"));
        // each statement of a multi-statement query
        assert!(is_ddl("SELECT 1; ALTER TABLE t ADD w int"));
        assert!(is_ddl("SELECT 1;/* x */ -- y\n drop table t"));
        assert!(!is_ddl("SELECT ';ALTER'"));
        // NO_BACKSLASH_ESCAPES: `'a\'` is a whole string, so the ALTER after
        // it is a statement. Read both ways, so this counts too.
        assert!(is_ddl("SELECT 'a\\'; ALTER TABLE t ADD w int"));
        assert!(is_ddl("SELECT \"a\\\"; DROP TABLE t"));
        // the other way round: one string with backslash escapes. It also
        // counts (one extra prepare, which is safe).
        assert!(is_ddl("SELECT 'it\\';ALTER'"));
        // a backslash with no DDL in either reading
        assert!(!is_ddl("SELECT 'it\\'s', 'a\\\\'"));
        assert!(!is_ddl("SELECT 'a\\'"));
        assert!(!is_ddl("SELECT `a\\`; SELECT 1"));
        assert!(!is_ddl("SELECT `a;ALTER`"));
        assert!(!is_ddl("SELECT \"x\"\"; DROP\""));
        assert!(!is_ddl("SELECT 1 /* ; ALTER */"));
        assert!(!is_ddl("SELECT 'not closed; ALTER"));
    }
}
