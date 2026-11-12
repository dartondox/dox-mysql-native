//! The MySQL wire protocol: packets, login, column definitions, rows and
//! the commands we send. Pure functions, no IO. `link.rs` runs them on the
//! sockets.
//!
//! A packet is a 3 byte length, a 1 byte sequence id and the payload. A
//! payload of 0xFFFFFF bytes or more goes in more packets: each full one
//! says "more follows".
use std::borrow::Cow;

use ring::digest;

use crate::{Fail, Kind, Param, T_NULL, cell, kind_of};

/// Capability flags we ask for.
pub(crate) const LONG_PASSWORD: u32 = 1;
pub(crate) const FOUND_ROWS: u32 = 1 << 1;
pub(crate) const CONNECT_WITH_DB: u32 = 1 << 3;
pub(crate) const PROTOCOL_41: u32 = 1 << 9;
pub(crate) const SSL: u32 = 1 << 11;
pub(crate) const TRANSACTIONS: u32 = 1 << 13;
pub(crate) const SECURE_CONNECTION: u32 = 1 << 15;
pub(crate) const MULTI_STATEMENTS: u32 = 1 << 16;
pub(crate) const MULTI_RESULTS: u32 = 1 << 17;
pub(crate) const PS_MULTI_RESULTS: u32 = 1 << 18;
pub(crate) const PLUGIN_AUTH: u32 = 1 << 19;
pub(crate) const PLUGIN_AUTH_LENENC: u32 = 1 << 21;
pub(crate) const DEPRECATE_EOF: u32 = 1 << 24;

/// Status flag of an OK packet: another result set follows.
pub(crate) const MORE_RESULTS: u16 = 0x0008;

pub(crate) const COM_QUERY: u8 = 0x03;
pub(crate) const COM_STMT_PREPARE: u8 = 0x16;
pub(crate) const COM_STMT_EXECUTE: u8 = 0x17;
pub(crate) const COM_STMT_CLOSE: u8 = 0x19;

/// utf8mb4_general_ci, like mysql_async
const CHARSET: u8 = 45;
const MAX_PACKET: u32 = 16 * 1024 * 1024;
#[cfg(not(fuzzing))]
const FULL: usize = 0xFF_FFFF;
/// In `cargo fuzz` builds only: a small size, so the fuzzer reaches the
/// code that joins packets with inputs of a few KB. The release library
/// always uses 0xFF_FFFF.
#[cfg(fuzzing)]
const FULL: usize = 0x400;

/// Column flags
const UNSIGNED: u16 = 32;

// ---- packets -----------------------------------------------------------

/// The next whole payload at the start of `b` and the bytes it used. None
/// when it is not all there yet. A payload in more packets is joined.
pub(crate) fn next_packet(b: &[u8]) -> Option<(Cow<'_, [u8]>, u8, usize)> {
    let mut p = 0;
    let mut joined: Option<Vec<u8>> = None;
    loop {
        if b.len() < p + 4 {
            return None;
        }
        let len = u32::from_le_bytes([b[p], b[p + 1], b[p + 2], 0]) as usize;
        let seq = b[p + 3];
        if b.len() < p + 4 + len {
            return None;
        }
        let part = &b[p + 4..p + 4 + len];
        p += 4 + len;
        if len < FULL && joined.is_none() {
            return Some((Cow::Borrowed(part), seq, p));
        }
        let all = joined.get_or_insert_with(Vec::new);
        all.extend_from_slice(part);
        if len < FULL {
            return Some((Cow::Owned(joined.unwrap()), seq, p));
        }
    }
}

/// Starts a packet in `out`. Give the returned mark to [end_packet].
pub(crate) fn start_packet(out: &mut Vec<u8>) -> usize {
    let at = out.len();
    out.extend_from_slice(&[0; 4]);
    at
}

/// Writes the head of the packet that starts at `at`. A big payload is
/// split into more packets.
pub(crate) fn end_packet(out: &mut Vec<u8>, at: usize, seq: u8) {
    let len = out.len() - at - 4;
    if len < FULL {
        out[at..at + 3].copy_from_slice(&(len as u32).to_le_bytes()[..3]);
        out[at + 3] = seq;
        return;
    }
    // rare: copy the payload out and write it again in parts
    let payload = out.split_off(at + 4);
    out.truncate(at);
    let mut seq = seq;
    let mut chunks = payload.chunks(FULL).peekable();
    let mut last_full = false;
    while let Some(c) = chunks.next() {
        out.extend_from_slice(&(c.len() as u32).to_le_bytes()[..3]);
        out.push(seq);
        out.extend_from_slice(c);
        seq = seq.wrapping_add(1);
        last_full = c.len() == FULL && chunks.peek().is_none();
    }
    if last_full {
        // a payload of exactly n * FULL bytes ends with an empty packet
        out.extend_from_slice(&[0, 0, 0, seq]);
    }
}

/// A whole packet with this payload.
pub(crate) fn packet(out: &mut Vec<u8>, seq: u8, payload: &[u8]) {
    let at = start_packet(out);
    out.extend_from_slice(payload);
    end_packet(out, at, seq);
}

// ---- reading values ----------------------------------------------------

pub(crate) fn lenenc(b: &[u8], p: &mut usize) -> Option<u64> {
    let first = *b.get(*p)?;
    let (v, n) = match first {
        0xFC => (
            u64::from(u16::from_le_bytes(b.get(*p + 1..*p + 3)?.try_into().ok()?)),
            3,
        ),
        0xFD => {
            let s = b.get(*p + 1..*p + 4)?;
            (u64::from(u32::from_le_bytes([s[0], s[1], s[2], 0])), 4)
        }
        0xFE => (
            u64::from_le_bytes(b.get(*p + 1..*p + 9)?.try_into().ok()?),
            9,
        ),
        v => (u64::from(v), 1),
    };
    *p += n;
    Some(v)
}

pub(crate) fn lenenc_bytes<'a>(b: &'a [u8], p: &mut usize) -> Option<&'a [u8]> {
    let len = lenenc(b, p)? as usize;
    let s = b.get(*p..p.checked_add(len)?)?;
    *p += len;
    Some(s)
}

fn put_lenenc(out: &mut Vec<u8>, v: u64) {
    if v < 251 {
        out.push(v as u8);
    } else if v < 1 << 16 {
        out.push(0xFC);
        out.extend_from_slice(&(v as u16).to_le_bytes());
    } else if v < 1 << 24 {
        out.push(0xFD);
        out.extend_from_slice(&(v as u32).to_le_bytes()[..3]);
    } else {
        out.push(0xFE);
        out.extend_from_slice(&v.to_le_bytes());
    }
}

fn put_lenenc_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_lenenc(out, b.len() as u64);
    out.extend_from_slice(b);
}

/// The text up to the next 0 byte (or the end).
fn nul_str<'a>(b: &'a [u8], p: &mut usize) -> &'a [u8] {
    let rest = b.get(*p..).unwrap_or(&[]);
    let end = rest.iter().position(|&c| c == 0).unwrap_or(rest.len());
    *p += (end + 1).min(rest.len());
    &rest[..end]
}

/// An OK packet (0x00, or 0xFE at the end of rows): affected rows,
/// insert id, status flags.
pub(crate) fn ok_packet(b: &[u8]) -> (u64, u64, u16) {
    let mut p = 1;
    let affected = lenenc(b, &mut p).unwrap_or(0);
    let insert_id = lenenc(b, &mut p).unwrap_or(0);
    let status = b
        .get(p..p + 2)
        .map_or(0, |s| u16::from_le_bytes([s[0], s[1]]));
    (affected, insert_id, status)
}

/// An ERR packet (0xFF): code, sqlstate, message.
pub(crate) fn err_packet(b: &[u8]) -> Fail {
    let code = b.get(1..3).map_or(0, |s| u16::from_le_bytes([s[0], s[1]]));
    let (state, msg) = if b.get(3) == Some(&b'#') && b.len() >= 9 {
        (&b[4..9], &b[9..])
    } else {
        (&b"HY000"[..], b.get(3..).unwrap_or(&[]))
    };
    let mut st = [b' '; 5];
    st.copy_from_slice(state);
    Fail::Server {
        code,
        state: st,
        message: String::from_utf8_lossy(msg).into_owned(),
    }
}

/// The end of rows: an OK packet with 0xFE in front (CLIENT_DEPRECATE_EOF).
/// A text row can start with 0xFE too, but it is then 9 bytes or longer
/// and only a huge one is FULL long.
pub(crate) fn is_end(b: &[u8]) -> bool {
    b.first() == Some(&0xFE) && b.len() < FULL
}

// ---- login -------------------------------------------------------------

/// The first packet of the server (protocol 10).
pub(crate) struct Greeting {
    pub caps: u32,
    pub nonce: Vec<u8>,
    pub plugin: String,
    pub maria: bool,
    /// the connection id, for `KILL QUERY`
    pub thread: u32,
}

pub(crate) fn greeting(b: &[u8]) -> Result<Greeting, Fail> {
    if b.first() == Some(&0xFF) {
        return Err(err_packet(b));
    }
    let bad = || Fail::Client("dox_mysql_native: bad greeting from the server".into());
    if b.first() != Some(&10) {
        return Err(bad());
    }
    let mut p = 1;
    let version = String::from_utf8_lossy(nul_str(b, &mut p)).into_owned();
    let thread = u32::from_le_bytes(b.get(p..p + 4).ok_or_else(bad)?.try_into().unwrap());
    p += 4;
    let mut nonce = b.get(p..p + 8).ok_or_else(bad)?.to_vec();
    p += 9; // nonce part 1 and a filler byte
    let lo = u16::from_le_bytes(b.get(p..p + 2).ok_or_else(bad)?.try_into().unwrap());
    p += 2;
    let mut caps = u32::from(lo);
    let mut plugin = String::new();
    if b.len() > p {
        p += 3; // charset, status
        let hi = u16::from_le_bytes(b.get(p..p + 2).ok_or_else(bad)?.try_into().unwrap());
        caps |= u32::from(hi) << 16;
        p += 2;
        let data_len = usize::from(*b.get(p).ok_or_else(bad)?);
        p += 11; // auth data length, 10 reserved
        if caps & SECURE_CONNECTION != 0 {
            let n = data_len.saturating_sub(8).max(13);
            let part = b.get(p..(p + n).min(b.len())).ok_or_else(bad)?;
            // the part ends with a 0 byte that is not in the nonce
            let part = part.strip_suffix(&[0]).unwrap_or(part);
            nonce.extend_from_slice(part);
            p += n;
        }
        if caps & PLUGIN_AUTH != 0 {
            plugin = String::from_utf8_lossy(nul_str(b, &mut p)).into_owned();
        }
    }
    Ok(Greeting {
        caps,
        nonce,
        plugin,
        maria: version.contains("MariaDB"),
        thread,
    })
}

fn sha1(parts: &[&[u8]]) -> Vec<u8> {
    let mut c = digest::Context::new(&digest::SHA1_FOR_LEGACY_USE_ONLY);
    for p in parts {
        c.update(p);
    }
    c.finish().as_ref().to_vec()
}

fn sha256(parts: &[&[u8]]) -> Vec<u8> {
    let mut c = digest::Context::new(&digest::SHA256);
    for p in parts {
        c.update(p);
    }
    c.finish().as_ref().to_vec()
}

fn xor(a: &mut [u8], b: &[u8]) {
    for (x, y) in a.iter_mut().zip(b) {
        *x ^= y;
    }
}

/// The answer to the nonce for an auth plugin. None: not supported.
/// `cleartext`: mysql_clear_password is allowed.
pub(crate) fn scramble(plugin: &str, pw: &[u8], nonce: &[u8], cleartext: bool) -> Option<Vec<u8>> {
    let nonce = &nonce[..nonce.len().min(20)];
    match plugin {
        _ if pw.is_empty() && plugin != "mysql_clear_password" => Some(Vec::new()),
        // SHA1(pw) XOR SHA1(nonce + SHA1(SHA1(pw)))
        "mysql_native_password" | "" => {
            let mut a = sha1(&[pw]);
            let b = sha1(&[nonce, &sha1(&[&a])]);
            xor(&mut a, &b);
            Some(a)
        }
        // SHA256(pw) XOR SHA256(SHA256(SHA256(pw)) + nonce)
        "caching_sha2_password" => {
            let mut a = sha256(&[pw]);
            let b = sha256(&[&sha256(&[&a]), nonce]);
            xor(&mut a, &b);
            Some(a)
        }
        "mysql_clear_password" if cleartext => {
            let mut a = pw.to_vec();
            a.push(0);
            Some(a)
        }
        _ => None,
    }
}

/// caching_sha2_password full login without TLS: the password (with a 0
/// byte) XOR the nonce, encrypted with the RSA key of the server (OAEP).
pub(crate) fn rsa_password(pem: &[u8], pw: &[u8], nonce: &[u8]) -> Result<Vec<u8>, Fail> {
    use rsa::pkcs8::DecodePublicKey;
    let bad = |e: String| Fail::Client(format!("dox_mysql_native: RSA key of the server: {e}"));
    let pem = std::str::from_utf8(pem).map_err(|e| bad(e.to_string()))?;
    let key = rsa::PublicKey::from_public_key_pem(pem.trim_end_matches('\0'))
        .or_else(|_| {
            use rsa::pkcs1::DecodeRsaPublicKey;
            rsa::PublicKey::from_pkcs1_pem(pem.trim_end_matches('\0'))
        })
        .map_err(|e| bad(e.to_string()))?;
    let nonce = &nonce[..nonce.len().min(20)];
    if nonce.is_empty() {
        // an auth switch can send an empty nonce
        return Err(bad("the nonce is empty".into()));
    }
    let mut data = pw.to_vec();
    data.push(0);
    for (i, b) in data.iter_mut().enumerate() {
        *b ^= nonce[i % nonce.len()];
    }
    key.oaep_encrypt::<sha1::Sha1>(&data)
        .map_err(|e| bad(e.to_string()))
}

/// The SSL request: the start of the login packet, before TLS.
pub(crate) fn ssl_request(caps: u32) -> Vec<u8> {
    let mut b = Vec::with_capacity(32);
    b.extend_from_slice(&caps.to_le_bytes());
    b.extend_from_slice(&MAX_PACKET.to_le_bytes());
    b.push(CHARSET);
    b.extend_from_slice(&[0; 23]);
    b
}

/// The login packet (HandshakeResponse41).
pub(crate) fn login(caps: u32, user: &str, auth: &[u8], db: Option<&str>, plugin: &str) -> Vec<u8> {
    let mut b = ssl_request(caps);
    b.extend_from_slice(user.as_bytes());
    b.push(0);
    if caps & PLUGIN_AUTH_LENENC != 0 {
        put_lenenc_bytes(&mut b, auth);
    } else {
        b.push(auth.len() as u8);
        b.extend_from_slice(auth);
    }
    if let Some(db) = db.filter(|_| caps & CONNECT_WITH_DB != 0) {
        b.extend_from_slice(db.as_bytes());
        b.push(0);
    }
    if caps & PLUGIN_AUTH != 0 {
        b.extend_from_slice(plugin.as_bytes());
        b.push(0);
    }
    b
}

/// An auth switch request (0xFE): the plugin and its nonce.
pub(crate) fn auth_switch(b: &[u8]) -> (String, Vec<u8>) {
    let mut p = 1;
    let plugin = String::from_utf8_lossy(nul_str(b, &mut p)).into_owned();
    let data = b.get(p..).unwrap_or(&[]);
    let data = data.strip_suffix(&[0]).unwrap_or(data);
    (plugin, data.to_vec())
}

// ---- results -----------------------------------------------------------

/// A result column: its name, what Dart gets, and how a binary row has it.
pub(crate) struct Column {
    pub name: Vec<u8>,
    pub kind: Kind,
    pub ty: u8,
    pub unsigned: bool,
}

/// A column definition (ColumnDefinition41).
pub(crate) fn column(b: &[u8]) -> Option<Column> {
    let mut p = 0;
    for _ in 0..4 {
        lenenc_bytes(b, &mut p)?; // catalog, schema, table, org_table
    }
    let name = lenenc_bytes(b, &mut p)?.to_vec();
    lenenc_bytes(b, &mut p)?; // org_name
    lenenc(b, &mut p)?; // length of the fixed fields (0x0c)
    let f = b.get(p..p + 9)?;
    let charset = u16::from_le_bytes([f[0], f[1]]);
    let length = u32::from_le_bytes([f[2], f[3], f[4], f[5]]);
    let ty = f[6];
    let flags = u16::from_le_bytes([f[7], f[8]]);
    Some(Column {
        name,
        kind: kind_of(ty, charset, length, flags),
        ty,
        unsigned: flags & UNSIGNED != 0,
    })
}

/// One value of a row, like mysql_async's `Value`.
pub(crate) enum Value<'a> {
    Null,
    Int(i64),
    UInt(u64),
    Float(f32),
    Double(f64),
    /// year, month, day, hour, minute, second, microsecond
    Date(u16, u8, u8, u8, u8, u8, u32),
    /// negative, days, hours, minutes, seconds, microseconds
    Time(bool, u32, u8, u8, u8, u32),
    Bytes(&'a [u8]),
}

/// A row of the text protocol: each value is text, or 0xFB for NULL.
pub(crate) fn text_row(out: &mut Vec<u8>, b: &[u8], cols: &[Column]) -> Option<()> {
    let mut p = 0;
    for c in cols {
        if b.get(p) == Some(&0xFB) {
            p += 1;
            out.push(T_NULL);
            continue;
        }
        cell(out, c.kind, Value::Bytes(lenenc_bytes(b, &mut p)?));
    }
    Some(())
}

/// A row of the binary protocol: 0x00, a NULL bitmap (offset 2), then the
/// values by column type.
pub(crate) fn binary_row(out: &mut Vec<u8>, b: &[u8], cols: &[Column]) -> Option<()> {
    let nulls = b.get(1..1 + (cols.len() + 9) / 8)?;
    let mut p = 1 + nulls.len();
    for (i, c) in cols.iter().enumerate() {
        let bit = i + 2;
        if nulls[bit / 8] & (1 << (bit % 8)) != 0 {
            out.push(T_NULL);
            continue;
        }
        let v = match c.ty {
            // TINY
            1 => {
                let x = *b.get(p)?;
                p += 1;
                Value::Int(if c.unsigned {
                    i64::from(x)
                } else {
                    i64::from(x as i8)
                })
            }
            // SHORT, YEAR
            2 | 13 => {
                let x = u16::from_le_bytes(b.get(p..p + 2)?.try_into().ok()?);
                p += 2;
                Value::Int(if c.unsigned || c.ty == 13 {
                    i64::from(x)
                } else {
                    i64::from(x as i16)
                })
            }
            // LONG, INT24
            3 | 9 => {
                let x = u32::from_le_bytes(b.get(p..p + 4)?.try_into().ok()?);
                p += 4;
                Value::Int(if c.unsigned {
                    i64::from(x)
                } else {
                    i64::from(x as i32)
                })
            }
            // LONGLONG
            8 => {
                let x = u64::from_le_bytes(b.get(p..p + 8)?.try_into().ok()?);
                p += 8;
                if c.unsigned {
                    Value::UInt(x)
                } else {
                    Value::Int(x as i64)
                }
            }
            // FLOAT
            4 => {
                let x = f32::from_le_bytes(b.get(p..p + 4)?.try_into().ok()?);
                p += 4;
                Value::Float(x)
            }
            // DOUBLE
            5 => {
                let x = f64::from_le_bytes(b.get(p..p + 8)?.try_into().ok()?);
                p += 8;
                Value::Double(x)
            }
            // TIMESTAMP, DATE, DATETIME, NEWDATE
            7 | 10 | 12 | 14 => {
                let n = usize::from(*b.get(p)?);
                let d = b.get(p + 1..p + 1 + n)?;
                p += 1 + n;
                let at = |i: usize| d.get(i).copied().unwrap_or(0);
                let year = if n >= 2 {
                    u16::from_le_bytes([d[0], d[1]])
                } else {
                    0
                };
                let us = if n >= 11 {
                    u32::from_le_bytes(d[7..11].try_into().ok()?)
                } else {
                    0
                };
                Value::Date(year, at(2), at(3), at(4), at(5), at(6), us)
            }
            // TIME
            11 => {
                let n = usize::from(*b.get(p)?);
                let d = b.get(p + 1..p + 1 + n)?;
                p += 1 + n;
                if n < 8 {
                    Value::Time(false, 0, 0, 0, 0, 0)
                } else {
                    let days = u32::from_le_bytes(d[1..5].try_into().ok()?);
                    let us = if n >= 12 {
                        u32::from_le_bytes(d[8..12].try_into().ok()?)
                    } else {
                        0
                    };
                    Value::Time(d[0] == 1, days, d[5], d[6], d[7], us)
                }
            }
            // NULL
            6 => Value::Null,
            // DECIMAL, strings, BLOB, JSON, ENUM, SET, BIT, GEOMETRY
            _ => Value::Bytes(lenenc_bytes(b, &mut p)?),
        };
        cell(out, c.kind, v);
    }
    Some(())
}

// ---- commands ----------------------------------------------------------

/// COM_QUERY with this sql.
pub(crate) fn query(out: &mut Vec<u8>, sql: &[u8]) {
    let at = start_packet(out);
    out.push(COM_QUERY);
    out.extend_from_slice(sql);
    end_packet(out, at, 0);
}

/// COM_STMT_PREPARE with this sql.
pub(crate) fn prepare(out: &mut Vec<u8>, sql: &[u8]) {
    let at = start_packet(out);
    out.push(COM_STMT_PREPARE);
    out.extend_from_slice(sql);
    end_packet(out, at, 0);
}

/// COM_RESET_CONNECTION. The server answers OK.
pub(crate) fn reset(out: &mut Vec<u8>) {
    packet(out, 0, &[0x1F]);
}

/// COM_STMT_CLOSE. The server sends no answer.
pub(crate) fn close(out: &mut Vec<u8>, id: u32) {
    let at = start_packet(out);
    out.push(COM_STMT_CLOSE);
    out.extend_from_slice(&id.to_le_bytes());
    end_packet(out, at, 0);
}

/// COM_STMT_EXECUTE with the params, typed like mysql_async does: i64 as
/// LONGLONG, f64 as DOUBLE, text and bytes as VAR_STRING.
pub(crate) fn execute(out: &mut Vec<u8>, id: u32, params: &[Param]) {
    let at = start_packet(out);
    out.push(COM_STMT_EXECUTE);
    out.extend_from_slice(&id.to_le_bytes());
    out.push(0); // no cursor
    out.extend_from_slice(&1u32.to_le_bytes()); // iteration count
    if !params.is_empty() {
        let bitmap = out.len();
        out.resize(bitmap + params.len().div_ceil(8), 0);
        for (i, p) in params.iter().enumerate() {
            if matches!(p, Param::Null) {
                out[bitmap + i / 8] |= 1 << (i % 8);
            }
        }
        out.push(1); // the types follow
        for p in params {
            let ty = match p {
                Param::Null => 6,
                Param::Int(_) => 8,
                Param::Double(_) => 5,
                Param::Bytes(_) => 253,
            };
            out.extend_from_slice(&[ty, 0]);
        }
        for p in params {
            match p {
                Param::Null => {}
                Param::Int(v) => out.extend_from_slice(&v.to_le_bytes()),
                Param::Double(v) => out.extend_from_slice(&v.to_le_bytes()),
                Param::Bytes(b) => put_lenenc_bytes(out, b),
            }
        }
    }
    end_packet(out, at, 0);
}

/// Entry points for the fuzz targets in `fuzz/`. Only in `cargo fuzz`
/// builds (`--cfg fuzzing`), not in the library.
#[cfg(fuzzing)]
#[doc(hidden)]
pub mod fuzz {
    use super::*;

    /// `FULL` in these builds, for the seeds and the harness.
    pub const FULL_PACKET: usize = FULL;

    fn all(b: &[u8]) -> Vec<(Vec<u8>, u8)> {
        let (mut p, mut v) = (0, Vec::new());
        while let Some((payload, seq, used)) = next_packet(&b[p..]) {
            assert!(used >= 4 && used <= b.len() - p);
            p += used;
            v.push((payload.into_owned(), seq));
        }
        v
    }

    /// `next_packet` in a loop, the way `Conn::process` calls it. Then the
    /// same bytes in random pieces, like socket reads: the packets must be
    /// the same.
    pub fn packets(data: &[u8]) {
        // the first byte seeds the piece sizes
        let (seed, data) = data.split_first().map_or((0, data), |(s, d)| (*s, d));
        let want = all(data);
        let (mut end, mut rng, mut p) = (0, u32::from(seed) | 1, 0);
        let mut got = Vec::new();
        while end < data.len() {
            rng ^= rng << 13;
            rng ^= rng >> 17;
            rng ^= rng << 5;
            end = (end + 1 + rng as usize % 700).min(data.len());
            while let Some((payload, seq, used)) = next_packet(&data[p..end]) {
                p += used;
                got.push((payload.into_owned(), seq));
            }
        }
        assert!(got == want);
    }

    /// The column parser on raw bytes. Then column definitions built from
    /// the input (a count, then 4 bytes for each column) and a row (the
    /// rest) in both row formats.
    pub fn rows(data: &[u8]) {
        // the parser on raw bytes
        let _ = column(data);
        let Some((&n, rest)) = data.split_first() else {
            return;
        };
        let n = usize::from(n % 9);
        let (spec, row) = rest.split_at((n * 4).min(rest.len()));
        let mut cols = Vec::new();
        for s in spec.as_chunks::<4>().0 {
            let mut def = Vec::new();
            for part in [&b""[..], b"", b"", b"", b"c", b"c"] {
                put_lenenc_bytes(&mut def, part);
            }
            def.push(0x0c);
            let charset: u16 = if s[3] & 1 == 0 { 45 } else { 63 };
            def.extend_from_slice(&charset.to_le_bytes());
            def.extend_from_slice(&u32::from(s[3] >> 1).to_le_bytes());
            def.push(s[0]);
            def.extend_from_slice(&[s[1], s[2], 0, 0]);
            let c = column(&def).expect("a valid column definition");
            assert!(c.ty == s[0] && c.name == b"c");
            assert_eq!(c.unsigned, u16::from_le_bytes([s[1], s[2]]) & UNSIGNED != 0);
            cols.push(c);
        }
        // a cell is the bytes of the row plus a small head or text
        let max = cols.len() * (row.len() + 64);
        let mut out = Vec::new();
        let text = text_row(&mut out, row, &cols);
        assert!(out.len() <= max);
        if text.is_some() {
            assert!(out.len() >= cols.len());
        }
        out.clear();
        let _ = binary_row(&mut out, row, &cols);
        assert!(out.len() <= max);
    }

    /// The small parsers of the login and of results.
    pub fn handshake(data: &[u8]) {
        if let Ok(g) = greeting(data) {
            assert!(g.nonce.len() <= data.len());
        }
        let (plugin, nonce) = auth_switch(data);
        assert!(plugin.len() <= data.len() * 3 && nonce.len() <= data.len());
        let _ = ok_packet(data);
        let _ = err_packet(data);
        assert_eq!(
            is_end(data),
            data.first() == Some(&0xFE) && data.len() < FULL
        );
        // read at every offset
        for start in 0..data.len().min(32) {
            let mut p = start;
            match lenenc(data, &mut p) {
                Some(_) => assert!(p > start && p <= data.len()),
                None => assert_eq!(p, start),
            }
            let mut p = start;
            match lenenc_bytes(data, &mut p) {
                Some(s) => assert!(p >= start && p <= data.len() && s.len() <= data.len()),
                None => assert!(p >= start),
            }
        }
    }
    /// A column definition with this type, flags, charset and length.
    fn col_def(ty: u8, flags: u16, charset: u16, length: u32) -> Vec<u8> {
        let mut def = Vec::new();
        for part in [&b""[..], b"", b"", b"", b"c", b"c"] {
            put_lenenc_bytes(&mut def, part);
        }
        def.push(0x0c);
        def.extend_from_slice(&charset.to_le_bytes());
        def.extend_from_slice(&length.to_le_bytes());
        def.push(ty);
        def.extend_from_slice(&flags.to_le_bytes());
        def.extend_from_slice(&[0, 0]);
        def
    }

    /// The bytes of the input, then 0 bytes.
    struct Take<'a>(&'a [u8]);

    impl Take<'_> {
        fn bytes(&mut self, n: usize) -> Vec<u8> {
            let k = n.min(self.0.len());
            let mut v = self.0[..k].to_vec();
            self.0 = &self.0[k..];
            v.resize(n, 0);
            v
        }
        fn byte(&mut self) -> u8 {
            self.bytes(1)[0]
        }
    }

    /// Things we write and read again must give the same thing: length
    /// numbers, packets (also split in parts), and rows that we build the
    /// way a server does, in the text and the binary format.
    pub fn roundtrip(data: &[u8]) {
        // length-encoded numbers: the shortest form, and back
        let mut t = Take(data);
        let v = u64::from_le_bytes(t.bytes(8).try_into().unwrap()) >> (t.byte() % 64);
        let mut out = Vec::new();
        put_lenenc(&mut out, v);
        let want = match v {
            0..=250 => 1,
            251..=0xFFFF => 3,
            0x1_0000..=0xFF_FFFF => 4,
            _ => 9,
        };
        assert_eq!(out.len(), want);
        let mut p = 0;
        assert_eq!(lenenc(&out, &mut p), Some(v));
        assert_eq!(p, out.len());
        let mut out = Vec::new();
        put_lenenc_bytes(&mut out, data);
        let mut p = 0;
        assert_eq!(lenenc_bytes(&out, &mut p), Some(data));
        assert_eq!(p, out.len());

        // packets: after other bytes, and with a payload over FULL bytes
        // (split in parts, joined again). The sequence id is the one of
        // the last part.
        let (seq, payload) = data.split_first().map_or((0, data), |(s, d)| (*s, d));
        let exact = vec![seq; FULL * (1 + usize::from(seq) % 3)];
        for payload in [
            payload,
            &payload.repeat(FULL / payload.len().max(1) * 2 + 1),
            &exact,
        ] {
            let mut out = vec![0xAA, 0xBB];
            packet(&mut out, seq, payload);
            assert_eq!(out[..2], [0xAA, 0xBB]);
            let (got, last, used) = next_packet(&out[2..]).expect("a whole packet");
            assert!(*got == *payload);
            assert_eq!(used, out.len() - 2);
            assert_eq!(last, seq.wrapping_add((payload.len() / FULL) as u8));
            // one byte less is not a whole packet
            assert!(next_packet(&out[2..out.len() - 1]).is_none());
            // the same packet written in 2 steps
            let mut out2 = vec![0xAA, 0xBB];
            let at = start_packet(&mut out2);
            out2.extend_from_slice(&payload[..payload.len() / 2]);
            out2.extend_from_slice(&payload[payload.len() / 2..]);
            end_packet(&mut out2, at, seq);
            assert!(out == out2);
        }

        // rows
        let mut t = Take(data);
        let n = 1 + usize::from(t.byte() % 6);
        let (mut defs, mut nulls, mut bin, mut txt) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let (mut want_bin, mut want_txt) = (Vec::new(), Vec::new());
        for _ in 0..n {
            let (sel, null) = (t.byte() % 15, t.byte() & 1 == 1);
            let mut b = Vec::new();
            let mut s = Vec::new();
            let (ty, flags, value): (u8, u16, Value) = match sel {
                0 => (1, 0, Value::Int(i64::from(t.byte() as i8))),
                1 => (1, UNSIGNED, Value::Int(i64::from(t.byte()))),
                2 => (
                    2,
                    0,
                    Value::Int(i64::from(i16::from_le_bytes(
                        t.bytes(2).try_into().unwrap(),
                    ))),
                ),
                3 => (
                    2,
                    UNSIGNED,
                    Value::Int(i64::from(u16::from_le_bytes(
                        t.bytes(2).try_into().unwrap(),
                    ))),
                ),
                4 => (
                    3,
                    0,
                    Value::Int(i64::from(i32::from_le_bytes(
                        t.bytes(4).try_into().unwrap(),
                    ))),
                ),
                5 => (
                    3,
                    UNSIGNED,
                    Value::Int(i64::from(u32::from_le_bytes(
                        t.bytes(4).try_into().unwrap(),
                    ))),
                ),
                6 => (
                    8,
                    0,
                    Value::Int(i64::from_le_bytes(t.bytes(8).try_into().unwrap())),
                ),
                7 => (
                    8,
                    UNSIGNED,
                    Value::UInt(u64::from_le_bytes(t.bytes(8).try_into().unwrap())),
                ),
                8 => (
                    4,
                    0,
                    Value::Float(f32::from_le_bytes(t.bytes(4).try_into().unwrap())),
                ),
                9 => (
                    5,
                    0,
                    Value::Double(f64::from_le_bytes(t.bytes(8).try_into().unwrap())),
                ),
                10 => {
                    let len = usize::from(t.byte() % 20);
                    s = t.bytes(len);
                    (253, 0, Value::Null)
                }
                11 => {
                    let k = usize::from(t.byte() % 4);
                    let d = t.bytes([0, 4, 7, 11][k]);
                    let at = |i: usize| d.get(i).copied().unwrap_or(0);
                    let year = if d.len() >= 2 {
                        u16::from_le_bytes([d[0], d[1]])
                    } else {
                        0
                    };
                    let us = if d.len() == 11 {
                        u32::from_le_bytes([d[7], d[8], d[9], d[10]])
                    } else {
                        0
                    };
                    let v = Value::Date(year, at(2), at(3), at(4), at(5), at(6), us);
                    (12, 0, v)
                }
                12 => {
                    let k = usize::from(t.byte() % 3);
                    let d = t.bytes([0, 8, 12][k]);
                    let v = if d.is_empty() {
                        Value::Time(false, 0, 0, 0, 0, 0)
                    } else {
                        let us = if d.len() == 12 {
                            u32::from_le_bytes([d[8], d[9], d[10], d[11]])
                        } else {
                            0
                        };
                        Value::Time(
                            d[0] == 1,
                            u32::from_le_bytes([d[1], d[2], d[3], d[4]]),
                            d[5],
                            d[6],
                            d[7],
                            us,
                        )
                    };
                    (11, 0, v)
                }
                13 => (
                    13,
                    0,
                    Value::Int(i64::from(u16::from_le_bytes(
                        t.bytes(2).try_into().unwrap(),
                    ))),
                ),
                _ => (6, 0, Value::Null),
            };
            let value = if sel == 10 { Value::Bytes(&s) } else { value };
            // what a server writes for this value in the binary format
            match &value {
                Value::Int(v) => match ty {
                    1 => b.push(*v as u8),
                    2 | 13 => b.extend_from_slice(&(*v as u16).to_le_bytes()),
                    3 => b.extend_from_slice(&(*v as u32).to_le_bytes()),
                    _ => b.extend_from_slice(&v.to_le_bytes()),
                },
                Value::UInt(v) => b.extend_from_slice(&v.to_le_bytes()),
                Value::Float(v) => b.extend_from_slice(&v.to_le_bytes()),
                Value::Double(v) => b.extend_from_slice(&v.to_le_bytes()),
                Value::Bytes(v) => put_lenenc_bytes(&mut b, v),
                Value::Date(y, mo, d, h, mi, s, us) => {
                    // the shortest form that has all the parts
                    let n = if *us != 0 {
                        11
                    } else if (*h, *mi, *s) != (0, 0, 0) {
                        7
                    } else if (*y, *mo, *d) != (0, 0, 0) {
                        4
                    } else {
                        0
                    };
                    b.push(n);
                    let all = [y.to_le_bytes()[0], y.to_le_bytes()[1], *mo, *d, *h, *mi, *s];
                    b.extend_from_slice(&all[..n.min(7) as usize]);
                    if n == 11 {
                        b.extend_from_slice(&us.to_le_bytes());
                    }
                }
                Value::Time(neg, days, h, m, s, us) => {
                    let n = if *us != 0 {
                        12
                    } else if (*days, *h, *m, *s) != (0, 0, 0, 0) || *neg {
                        8
                    } else {
                        0
                    };
                    b.push(n);
                    if n > 0 {
                        b.push(u8::from(*neg));
                        b.extend_from_slice(&days.to_le_bytes());
                        b.extend_from_slice(&[*h, *m, *s]);
                    }
                    if n == 12 {
                        b.extend_from_slice(&us.to_le_bytes());
                    }
                }
                Value::Null => {}
            }
            let def = col_def(ty, flags, 45, 0);
            let col = column(&def).expect("a valid column definition");
            nulls.push(null);
            if null {
                cell(&mut want_bin, col.kind, Value::Null);
            } else {
                bin.extend_from_slice(&b);
                cell(&mut want_bin, col.kind, value);
            }
            // the text format: the text of the value is what the server
            // sends. We have no text of the number, so use these bytes.
            let k = usize::from(t.byte() % 12);
            let text = t.bytes(k);
            if null {
                txt.push(0xFB);
                cell(&mut want_txt, col.kind, Value::Null);
            } else {
                put_lenenc_bytes(&mut txt, &text);
                cell(&mut want_txt, col.kind, Value::Bytes(&text));
            }
            defs.push(col);
        }
        let mut row = vec![0u8; 1 + (n + 9) / 8];
        for (i, null) in nulls.iter().enumerate() {
            if *null {
                row[1 + (i + 2) / 8] |= 1 << ((i + 2) % 8);
            }
        }
        row.extend_from_slice(&bin);
        for (row, txt_row, want, cut_ok) in [
            (&row, false, &want_bin, true),
            (&txt, true, &want_txt, true),
        ] {
            let _ = cut_ok;
            let mut out = Vec::new();
            let r = if txt_row {
                text_row(&mut out, row, &defs)
            } else {
                binary_row(&mut out, row, &defs)
            };
            assert!(r.is_some());
            assert!(out == *want);
            // bytes after the row are not read
            let mut longer = row.clone();
            longer.push(0xAB);
            let mut out = Vec::new();
            let _ = if txt_row {
                text_row(&mut out, &longer, &defs)
            } else {
                binary_row(&mut out, &longer, &defs)
            };
            assert!(out == *want);
            // a row that is cut is never read as a whole
            for cut in 0..row.len() {
                let mut out = Vec::new();
                let r = if txt_row {
                    text_row(&mut out, &row[..cut], &defs)
                } else {
                    binary_row(&mut out, &row[..cut], &defs)
                };
                assert!(
                    r.is_none(),
                    "a row cut to {cut} of {} bytes was read",
                    row.len()
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rsa_password_is_oaep_sha1() {
        use rsa::Generate;
        use rsa::pkcs1::EncodeRsaPublicKey;
        use rsa::pkcs8::{EncodePublicKey, LineEnding};
        let private = rsa::PrivateKey::generate();
        let public = private.to_public_key();
        let spki = public.to_public_key_pem(LineEnding::LF).unwrap();
        let pkcs1 = public.to_pkcs1_pem(LineEnding::LF).unwrap();
        let nonce: Vec<u8> = (1..=20).collect();
        let mut want = b"secret".to_vec();
        want.push(0);
        for (i, b) in want.iter_mut().enumerate() {
            *b ^= nonce[i];
        }
        // the server sends the PEM with a 0 byte at the end
        let pkcs1_pem = pkcs1.clone();
        for pem in [format!("{spki}\0"), pkcs1] {
            let enc = rsa_password(pem.as_bytes(), b"secret", &nonce).unwrap();
            assert_eq!(private.oaep_decrypt::<sha1::Sha1>(&enc).unwrap(), want);
        }
        assert!(rsa_password(b"not a key", b"secret", &nonce).is_err());
        // an auth switch can send an empty nonce: an error, not a panic
        assert!(rsa_password(pkcs1_pem.as_bytes(), b"secret", &[]).is_err());
    }

    #[test]
    fn packets_split_and_join() {
        let mut out = Vec::new();
        packet(&mut out, 3, b"abc");
        assert_eq!(out, [3, 0, 0, 3, b'a', b'b', b'c']);
        let (p, seq, used) = next_packet(&out).unwrap();
        assert_eq!((&*p, seq, used), (&b"abc"[..], 3, 7));
        assert!(next_packet(&out[..6]).is_none());

        let big = vec![7u8; FULL + 10];
        let mut out = Vec::new();
        packet(&mut out, 0, &big);
        assert_eq!(out.len(), big.len() + 8);
        let (p, _, used) = next_packet(&out).unwrap();
        assert_eq!((p.len(), used), (big.len(), out.len()));

        // exactly FULL bytes: one full packet and an empty one
        let mut out = Vec::new();
        packet(&mut out, 0, &vec![1u8; FULL]);
        assert_eq!(out.len(), FULL + 8);
        assert_eq!(next_packet(&out).unwrap().0.len(), FULL);
    }

    #[test]
    fn lenenc_values() {
        for v in [0u64, 250, 251, 65_535, 65_536, 1 << 24, u64::MAX] {
            let mut out = Vec::new();
            put_lenenc(&mut out, v);
            let mut p = 0;
            assert_eq!(lenenc(&out, &mut p), Some(v));
            assert_eq!(p, out.len());
        }
    }

    #[test]
    fn ok_and_err() {
        assert_eq!(
            ok_packet(&[0, 3, 0xFC, 0x10, 0x27, 8, 0, 0, 0]),
            (3, 10_000, 8)
        );
        match err_packet(b"\xff\x7a\x04#42S02Table 'x' doesn't exist") {
            Fail::Server {
                code,
                state,
                message,
            } => {
                assert_eq!((code, &state), (1146, b"42S02"));
                assert_eq!(message, "Table 'x' doesn't exist");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn native_password() {
        // from the MySQL docs test vectors: empty password, empty answer
        assert_eq!(
            scramble("mysql_native_password", b"", &[1; 20], false),
            Some(vec![])
        );
        let a = scramble("mysql_native_password", b"secret", &[7; 20], false).unwrap();
        assert_eq!(a.len(), 20);
        let b = scramble("caching_sha2_password", b"secret", &[7; 20], false).unwrap();
        assert_eq!(b.len(), 32);
        assert!(scramble("sha256_password", b"secret", &[7; 20], false).is_none());
    }

    #[test]
    fn binary_rows() {
        let cols = [
            Column {
                name: b"a".to_vec(),
                kind: Kind::Int,
                ty: 8,
                unsigned: false,
            },
            Column {
                name: b"b".to_vec(),
                kind: Kind::Text,
                ty: 253,
                unsigned: false,
            },
            Column {
                name: b"c".to_vec(),
                kind: Kind::Date,
                ty: 12,
                unsigned: false,
            },
        ];
        // b is NULL: bit 1 + 2 = 3
        let mut row = vec![0, 0b1000];
        row.extend_from_slice(&(-5i64).to_le_bytes());
        row.extend_from_slice(&[4, 0xE8, 0x07, 2, 29]);
        let mut out = Vec::new();
        binary_row(&mut out, &row, &cols).unwrap();
        assert_eq!(out[0], crate::T_INT);
        assert_eq!(i64::from_le_bytes(out[1..9].try_into().unwrap()), -5);
        assert_eq!(out[9], T_NULL);
        assert_eq!(out[10], crate::T_DATE);
    }
}
