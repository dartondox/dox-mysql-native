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
//! Commands on a connection are pipelined: the server answers them in
//! order. A query without params goes as text (COM_QUERY), one with params
//! as a prepared statement (binary protocol). A statement is prepared once
//! per connection and kept in an LRU cache. Its first EXECUTE waits behind
//! the answer of its PREPARE (it needs the statement id), and the commands
//! after it wait too, so the order stays. Each command fails on its own.
//!
//! A transaction holds one connection until commit or rollback. A
//! transaction with no request for [TIMEOUT] is rolled back.
use std::collections::{HashMap, VecDeque};
use std::io::{self, ErrorKind, Read, Write};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};

use mio::event::Source;
use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Registry, Token, Waker};
use slab::Slab;

use crate::proto::{self, Column};
use crate::{
    Fail, OP_BEGIN, OP_COMMIT, OP_EXECUTE, OP_QUERY, OP_ROLLBACK, POOL_CLOSED, POST, Param,
    RES_HEAD, Reader, Request, Shared, TIMED_OUT, TIMEOUT, ok_head, pool_of, post, read_request,
    record,
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

    fn nodelay(&self, on: bool) {
        match self {
            Sock::Tcp(s) => drop(s.set_nodelay(on)),
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
        }
    }
}

/// A command that is not sent yet: the connection logs in, or a command
/// in front of it waits for its statement id.
enum Cmd {
    Query(Wait, String),
    Exec(Wait, usize, Vec<Param>),
    Prepare(usize, String, Instant),
    Close(usize),
}

impl Cmd {
    fn deadline(&self) -> Option<Instant> {
        match self {
            Cmd::Query(w, _) | Cmd::Exec(w, ..) => Some(w.deadline),
            Cmd::Prepare(_, _, d) => Some(*d),
            Cmd::Close(_) => None,
        }
    }
}

/// A prepared statement of one connection.
struct Stmt {
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
    /// the transaction that holds it
    tx: Option<u64>,
    /// a COMMIT or ROLLBACK runs: not free yet
    ending: bool,
    /// give it back once it has no command: no new command goes to it
    release: bool,
}

/// Opens a socket to the next address. The connect goes on in the loop.
fn connect_next(addrs: &mut Vec<SocketAddr>) -> Result<Sock, Fail> {
    let mut last = None;
    while !addrs.is_empty() {
        let a = addrs.remove(0);
        match TcpStream::connect(a) {
            Ok(s) => return Ok(Sock::Tcp(s)),
            Err(e) => last = Some(format!("can not connect to {a}: {e}")),
        }
    }
    Err(io_fail(last.unwrap_or_else(|| "no address".into())))
}

impl Conn {
    /// Starts a connection: a non-blocking connect. DNS blocks.
    /// ponytail: a blocking lookup. An IP or localhost is instant.
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
            let mut addrs: Vec<SocketAddr> = (o.host.as_str(), o.port)
                .to_socket_addrs()
                .map_err(|e| io_fail(format!("{}:{}: {e}", o.host, o.port)))?
                .collect();
            let s = connect_next(&mut addrs)?;
            (s, Phase::Connect(addrs))
        };
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
            tx: None,
            ending: false,
            release: false,
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
        self.tx.is_none() && !self.ending && !self.release
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
        if self.ready() && (self.held.is_empty() || matches!(cmd, Cmd::Prepare(..))) {
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
        while let Some(cmd) = self.held.pop_front() {
            if let Some(cmd) = self.encode(cmd, ready) {
                self.held.push_front(cmd);
                return;
            }
        }
    }

    /// A query: as text without params, as a prepared statement with.
    fn query(&mut self, w: Wait, sql: String, params: Vec<Param>, ready: &mut Vec<u8>) {
        if params.is_empty() {
            return self.put(Cmd::Query(w, sql), ready);
        }
        self.tick += 1;
        let slot = match self.cache.get(sql.as_str()) {
            Some(&s) => s,
            None => {
                let s = self.stmts.insert(Stmt {
                    sql: sql.clone(),
                    id: None,
                    params: 0,
                    failed: None,
                    refs: 0,
                    used: 0,
                    cached: true,
                });
                self.cache.insert(sql.clone(), s);
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
    fn fill(&mut self) -> io::Result<bool> {
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
                        if cfg!(unix) && n < free {
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
        while let Some((payload, seq, used)) = proto::next_packet(&inb[p..self.in_e]) {
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
                } else if w.first && rows {
                    let ok = if binary {
                        proto::binary_row(&mut w.out, b, &w.cols)
                    } else {
                        proto::text_row(&mut w.out, b, &w.cols)
                    };
                    ok.ok_or_else(bad)?;
                    w.count += 1;
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
        match w.what {
            What::Result { .. } => {
                if self.tx.is_some() {
                    self.used = now;
                }
                let r = match w.fail {
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
            What::Prepare(_) => {}
        }
    }

    /// A ROLLBACK nobody waits for. The connection is free after it.
    fn rollback(&mut self, now: Instant, ready: &mut Vec<u8>) {
        self.tx = None;
        self.ending = true;
        let w = Wait::new(0, What::Silent, now + TIMEOUT);
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
    fn acquire(&self, me: u64, link: &Weak<Link>) -> Got {
        let mut st = self.state.lock().unwrap();
        if self.closed.load(Ordering::Acquire) {
            return Got::None;
        }
        if let Some(first) = st.waiters.front()
            && first.upgrade().map(|l| l.id) != Some(me)
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
        got
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
    evs: Vec<(usize, bool, bool)>,
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
        let deadline = self.now + TIMEOUT;
        let mut seen = Vec::new();
        let mut r = Reader(b);
        while let Some(req) = read_request(&mut r) {
            if req.tx != 0 {
                self.in_tx(req, deadline);
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
        self.timers();
        self.flush_all();
    }

    /// A request in a transaction: it goes to the connection of the
    /// transaction. Commit and rollback end it.
    // ponytail: a linear scan of the connections of this link. A
    // transaction id works only in the isolate that began it.
    fn in_tx(&mut self, req: Request, deadline: Instant) {
        let Some(key) = self
            .conns
            .iter()
            .find(|(_, c)| c.tx == Some(req.tx))
            .map(|(k, _)| k)
        else {
            return self.fail(req.id, Fail::client(TX_CLOSED));
        };
        let c = &mut self.conns[key];
        c.used = self.now;
        match req.op {
            OP_COMMIT | OP_ROLLBACK => {
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
            OP_QUERY | OP_EXECUTE => {
                let what = What::Result {
                    rows: req.op == OP_QUERY,
                    binary: !req.params.is_empty(),
                };
                c.query(
                    Wait::new(req.id, what, deadline),
                    req.sql,
                    req.params,
                    &mut self.ready,
                );
            }
            op => self.fail(req.id, Fail::Client(format!("unknown op {op}"))),
        }
    }

    /// Sends a request of pool `pid` on a connection of this link. Gives
    /// it back when there is none (the pool is at its limit).
    fn place(&mut self, pid: u64, req: Request, deadline: Instant) -> Result<(), Request> {
        if !matches!(req.op, OP_QUERY | OP_EXECUTE | OP_BEGIN) {
            self.fail(req.id, Fail::Client(format!("unknown op {}", req.op)));
            return Ok(());
        }
        let begin = req.op == OP_BEGIN;
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
            // pipeline on the least busy one (not a begin)
            match best.filter(|_| !begin) {
                Some(k) => break k,
                None => return Err(req),
            }
        };
        let now = self.now;
        let c = &mut self.conns[key];
        if begin {
            let tx = NEXT_TX.fetch_add(1, Ordering::Relaxed);
            c.tx = Some(tx);
            c.used = now;
            let w = Wait::new(req.id, What::Begin(tx), deadline);
            c.put(Cmd::Query(w, "START TRANSACTION".into()), &mut self.ready);
        } else {
            let what = What::Result {
                rows: req.op == OP_QUERY,
                binary: !req.params.is_empty(),
            };
            c.query(
                Wait::new(req.id, what, deadline),
                req.sql,
                req.params,
                &mut self.ready,
            );
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
        if let Some(f) = why {
            c.fail_all(&f, &mut self.ready);
        }
        c.quit();
        let shared = c.shared.clone();
        drop(c);
        shared.put_back(self.id, None);
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
        let empty: Vec<usize> = self.pools[&pid]
            .conns
            .iter()
            .copied()
            .filter(|&k| self.conns[k].empty())
            .collect();
        for k in empty {
            let l = &self.pools[&pid];
            if closed || !self.fresh_check(k) {
                self.close(k, None);
            } else if others(l) && !(l.waiter && self.first_is_me(&shared)) {
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
        // 5.
        let l = &self.pools[&pid];
        let begin = l.queue.iter().any(|q| q.req.op == OP_BEGIN);
        if (others(l) || begin) && !l.conns.iter().any(|&k| self.conns[k].release) {
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
        !c.shared
            .opts
            .conn_ttl
            .is_some_and(|ttl| self.now.saturating_duration_since(c.last_io) > ttl)
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
            l.queue.retain(|q| {
                if q.deadline <= now {
                    late.push(q.req.id);
                    false
                } else {
                    soon(q.deadline);
                    true
                }
            });
        }
        for id in late {
            self.fail(id, Fail::client(crate::TIMED_OUT));
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
            if c.tx.is_some() && c.load() == 0 {
                let due = c.used + TIMEOUT;
                if due <= now {
                    c.rollback(now, &mut self.ready);
                    soon(now + TIMEOUT);
                } else {
                    soon(due);
                }
            }
        }
        for k in dead {
            self.close(k, Some(Fail::client(TIMED_OUT)));
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
            let read = ev.is_readable() || ev.is_read_closed() || ev.is_error();
            let write = ev.is_writable() || ev.is_write_closed() || ev.is_error();
            evs.push((ev.token().0, read, write));
        }
        for &(k, read, write) in &evs {
            if self.conns.contains(k) {
                self.event(k, read, write);
            }
        }
        self.evs = evs;
    }

    fn event(&mut self, k: usize, read: bool, write: bool) {
        let registry = self.poll.registry();
        let c = &mut self.conns[k];
        if let Phase::Connect(addrs) = &mut c.phase {
            if !(read || write) {
                return;
            }
            match c.sock.connected() {
                Ok(false) => return,
                Ok(true) => {
                    c.sock.nodelay(c.shared.opts.nodelay);
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
                    let why = io_fail(format!("can not connect: {e}"));
                    if addrs.is_empty() {
                        return self.close(k, Some(why));
                    }
                    let _ = registry.deregister(c.sock.source());
                    match connect_next(addrs) {
                        Ok(s) => {
                            c.sock = s;
                            let i = Interest::READABLE | Interest::WRITABLE;
                            if let Err(e) = registry.register(c.sock.source(), Token(k), i) {
                                return self.close(k, Some(io_fail(e)));
                            }
                            return;
                        }
                        Err(f) => return self.close(k, Some(f)),
                    }
                }
            }
        }
        if read {
            let open = match c.fill() {
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
        self.turn();
        let ids: Vec<u64> = self.pools.keys().copied().collect();
        for pid in ids {
            self.balance(pid);
        }
        self.timers();
        self.flush_all();
    }

    /// The isolate is gone: close all connections, leave all lists.
    fn shutdown(&mut self) {
        let keys: Vec<usize> = self.conns.iter().map(|(k, _)| k).collect();
        for k in keys {
            self.close(k, None);
        }
        for (_, l) in self.pools.drain() {
            if l.waiter {
                l.shared.set_waiter(self.id, &self.me, false);
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
/// `link` must come from `dox_mysql_client`. `ptr` must point to `len`
/// readable bytes. Only one call per link at a time (one isolate).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dox_mysql_submit(link: i64, ptr: *const u8, len: i64) -> i64 {
    let s = unsafe { &*(link as *const Link) };
    if s.stopped.load(Ordering::Acquire) {
        return 0;
    }
    let b = unsafe { std::slice::from_raw_parts(ptr, len.max(0) as usize) };
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
/// `link` must come from `dox_mysql_client`. `buf` must point to `cap`
/// writable bytes. Only one call per link at a time (one isolate).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dox_mysql_run(link: i64, buf: *mut u8, cap: i64) -> i64 {
    let s = unsafe { &*(link as *const Link) };
    if s.stopped.load(Ordering::Acquire) {
        return 0;
    }
    let mut core = s.core.lock().unwrap();
    s.armed.store(false, Ordering::Release);
    core.service();
    let n = core.ready.len();
    s.set_due(core.next);
    if n > cap.max(0) as usize {
        return -(n as i64);
    }
    unsafe { std::ptr::copy_nonoverlapping(core.ready.as_ptr(), buf, n) };
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
