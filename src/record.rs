//! Binary format of a record (little-endian):
//!
//! | Field           | Type        | Purpose                                |
//! |-----------------|-------------|----------------------------------------|
//! | `length`        | `u32`       | Size of everything that follows        |
//! | `crc`           | `u32`       | CRC32C of everything that follows      |
//! | `offset`        | `u64`       | Absolute offset in the partition       |
//! | `timestamp`     | `i64`       | Milliseconds since the epoch           |
//! | `attributes`    | `u8`        | Reserved (compression, future flags)   |
//! | `key_len`, key  | `i32` + …   | -1 = no key                            |
//! | `value_len`, …  | `i32` + …   | -1 = null value (tombstone)            |
//! | `headers_count` | `u16`       | Then `u16` + key, `u32` + value        |

use std::io::{self, Read};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{Error, Result};

/// Maximum size of an encoded record, including the length prefix.
pub const MAX_RECORD_SIZE: usize = 16 * 1024 * 1024;

const LEN_SIZE: usize = 4;
const CRC_SIZE: usize = 4;
/// offset + timestamp + attributes + key_len + value_len + headers_count
const MIN_BODY_SIZE: usize = 8 + 8 + 1 + 4 + 4 + 2;

/// A message header: a text key and a binary value, as in Kafka.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub key: String,
    pub value: Vec<u8>,
}

/// A message to produce. The offset is assigned by the log.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Message {
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<Header>,
    /// Milliseconds since the epoch. `None` = the local clock at append time.
    pub timestamp: Option<i64>,
}

impl Message {
    /// A message with a key and a value.
    pub fn new(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: Some(key.into()),
            value: Some(value.into()),
            ..Self::default()
        }
    }

    /// A message without a key.
    pub fn with_value(value: impl Into<Vec<u8>>) -> Self {
        Self {
            value: Some(value.into()),
            ..Self::default()
        }
    }

    pub fn header(mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.headers.push(Header {
            key: key.into(),
            value: value.into(),
        });
        self
    }

    pub fn timestamp(mut self, millis: i64) -> Self {
        self.timestamp = Some(millis);
        self
    }
}

/// A record read from the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub offset: u64,
    pub timestamp: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<Header>,
}

pub(crate) fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Encoded size of a message, including the length prefix.
pub(crate) fn encoded_size(msg: &Message) -> usize {
    let opt = |b: &Option<Vec<u8>>| b.as_ref().map_or(0, Vec::len);
    let headers: usize = msg
        .headers
        .iter()
        .map(|h| 2 + h.key.len() + 4 + h.value.len())
        .sum();
    LEN_SIZE + CRC_SIZE + MIN_BODY_SIZE + opt(&msg.key) + opt(&msg.value) + headers
}

/// Checks that a message can be encoded, and returns its encoded size.
pub(crate) fn validate(msg: &Message) -> Result<usize> {
    let size = encoded_size(msg);
    if size > MAX_RECORD_SIZE {
        return Err(Error::RecordTooLarge {
            size,
            max: MAX_RECORD_SIZE,
        });
    }
    if msg.headers.len() > u16::MAX as usize {
        return Err(Error::InvalidRecord(format!(
            "{} headers (maximum {})",
            msg.headers.len(),
            u16::MAX
        )));
    }
    if let Some(h) = msg.headers.iter().find(|h| h.key.len() > u16::MAX as usize) {
        return Err(Error::InvalidRecord(format!(
            "header key of {} bytes (maximum {})",
            h.key.len(),
            u16::MAX
        )));
    }
    Ok(size)
}

/// Encodes `msg` at the end of `buf`. The message must have been validated.
pub(crate) fn encode(msg: &Message, offset: u64, timestamp: i64, buf: &mut Vec<u8>) {
    let start = buf.len();
    buf.extend_from_slice(&[0; LEN_SIZE + CRC_SIZE]);
    let body_start = buf.len();

    buf.extend_from_slice(&offset.to_le_bytes());
    buf.extend_from_slice(&timestamp.to_le_bytes());
    buf.push(0); // attributes
    put_bytes_i32(buf, msg.key.as_deref());
    put_bytes_i32(buf, msg.value.as_deref());
    buf.extend_from_slice(&(msg.headers.len() as u16).to_le_bytes());
    for h in &msg.headers {
        buf.extend_from_slice(&(h.key.len() as u16).to_le_bytes());
        buf.extend_from_slice(h.key.as_bytes());
        buf.extend_from_slice(&(h.value.len() as u32).to_le_bytes());
        buf.extend_from_slice(&h.value);
    }

    let crc = crc32c::crc32c(&buf[body_start..]);
    let len = (buf.len() - start - LEN_SIZE) as u32;
    buf[start..start + LEN_SIZE].copy_from_slice(&len.to_le_bytes());
    buf[start + LEN_SIZE..body_start].copy_from_slice(&crc.to_le_bytes());
}

fn put_bytes_i32(buf: &mut Vec<u8>, bytes: Option<&[u8]>) {
    match bytes {
        None => buf.extend_from_slice(&(-1i32).to_le_bytes()),
        Some(b) => {
            buf.extend_from_slice(&(b.len() as i32).to_le_bytes());
            buf.extend_from_slice(b);
        }
    }
}

/// Result of reading a record from a byte stream.
#[derive(Debug)]
pub(crate) enum ReadOutcome {
    /// A valid record, and its encoded size.
    Record(Record, usize),
    /// Clean end of the stream, on a record boundary.
    End,
    /// The stream stops in the middle of a record (interrupted write).
    Incomplete,
    /// Invalid bytes: absurd length, wrong CRC, inconsistent structure.
    Corrupt(String),
}

/// Reads the next record from `r`.
pub(crate) fn read_record<R: Read>(r: &mut R) -> io::Result<ReadOutcome> {
    let mut len_buf = [0u8; LEN_SIZE];
    match read_full(r, &mut len_buf)? {
        0 => return Ok(ReadOutcome::End),
        n if n < LEN_SIZE => return Ok(ReadOutcome::Incomplete),
        _ => {}
    }
    let len = u32::from_le_bytes(len_buf) as usize;
    if len < CRC_SIZE + MIN_BODY_SIZE || len + LEN_SIZE > MAX_RECORD_SIZE {
        return Ok(ReadOutcome::Corrupt(format!("invalid length: {len}")));
    }

    let mut frame = vec![0u8; len];
    if read_full(r, &mut frame)? < len {
        return Ok(ReadOutcome::Incomplete);
    }
    let stored_crc = u32::from_le_bytes(frame[..CRC_SIZE].try_into().unwrap());
    let body = &frame[CRC_SIZE..];
    if crc32c::crc32c(body) != stored_crc {
        return Ok(ReadOutcome::Corrupt("invalid CRC".into()));
    }
    Ok(match decode_body(body) {
        Some(record) => ReadOutcome::Record(record, len + LEN_SIZE),
        None => ReadOutcome::Corrupt("invalid structure".into()),
    })
}

/// Reads until `buf` is full or the end of the stream is reached. Returns the number of bytes read.
fn read_full<R: Read>(r: &mut R, buf: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match r.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(filled)
}

fn decode_body(body: &[u8]) -> Option<Record> {
    let mut c = Cursor { buf: body };
    let offset = u64::from_le_bytes(c.take_array()?);
    let timestamp = i64::from_le_bytes(c.take_array()?);
    let _attributes = c.take(1)?;
    let key = c.take_bytes_i32()?;
    let value = c.take_bytes_i32()?;
    let count = u16::from_le_bytes(c.take_array()?);
    let mut headers = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let klen = u16::from_le_bytes(c.take_array()?) as usize;
        let key = String::from_utf8(c.take(klen)?.to_vec()).ok()?;
        let vlen = u32::from_le_bytes(c.take_array()?) as usize;
        let value = c.take(vlen)?.to_vec();
        headers.push(Header { key, value });
    }
    if !c.buf.is_empty() {
        return None;
    }
    Some(Record {
        offset,
        timestamp,
        key,
        value,
        headers,
    })
}

struct Cursor<'a> {
    buf: &'a [u8],
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        if n > self.buf.len() {
            return None;
        }
        let (head, tail) = self.buf.split_at(n);
        self.buf = tail;
        Some(head)
    }

    fn take_array<const N: usize>(&mut self) -> Option<[u8; N]> {
        self.take(N).map(|b| b.try_into().unwrap())
    }

    fn take_bytes_i32(&mut self) -> Option<Option<Vec<u8>>> {
        match i32::from_le_bytes(self.take_array()?) {
            -1 => Some(None),
            n if n < 0 => None,
            n => Some(Some(self.take(n as usize)?.to_vec())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(msg: &Message) -> Record {
        validate(msg).unwrap();
        let mut buf = Vec::new();
        encode(msg, 42, 1_700_000_000_000, &mut buf);
        assert_eq!(buf.len(), encoded_size(msg));
        match read_record(&mut buf.as_slice()).unwrap() {
            ReadOutcome::Record(rec, size) => {
                assert_eq!(size, buf.len());
                rec
            }
            other => panic!("expected a record, got {other:?}"),
        }
    }

    #[test]
    fn encode_decode_full_message() {
        let msg = Message::new("key", "value")
            .header("trace-id", "abc")
            .header("empty", "");
        let rec = roundtrip(&msg);
        assert_eq!(rec.offset, 42);
        assert_eq!(rec.timestamp, 1_700_000_000_000);
        assert_eq!(rec.key.as_deref(), Some(&b"key"[..]));
        assert_eq!(rec.value.as_deref(), Some(&b"value"[..]));
        assert_eq!(rec.headers, msg.headers);
    }

    #[test]
    fn null_key_and_tombstone_are_distinct_from_empty() {
        let rec = roundtrip(&Message::default());
        assert_eq!((rec.key, rec.value), (None, None));
        let rec = roundtrip(&Message::new("", ""));
        assert_eq!((rec.key, rec.value), (Some(vec![]), Some(vec![])));
    }

    #[test]
    fn empty_stream_is_a_clean_end() {
        assert!(matches!(
            read_record(&mut &[][..]).unwrap(),
            ReadOutcome::End
        ));
    }

    #[test]
    fn every_truncation_is_detected_as_incomplete() {
        let mut buf = Vec::new();
        encode(&Message::new("k", "v").header("h", "x"), 0, 0, &mut buf);
        for cut in 1..buf.len() {
            let outcome = read_record(&mut &buf[..cut]).unwrap();
            assert!(
                matches!(outcome, ReadOutcome::Incomplete),
                "cut at {cut}: {outcome:?}"
            );
        }
    }

    #[test]
    fn every_flipped_byte_is_detected() {
        let mut buf = Vec::new();
        encode(&Message::new("k", "value"), 7, 0, &mut buf);
        for i in 0..buf.len() {
            let mut bad = buf.clone();
            bad[i] ^= 0xFF;
            let outcome = read_record(&mut bad.as_slice()).unwrap();
            assert!(
                !matches!(outcome, ReadOutcome::Record(..)),
                "flipped byte {i} not detected"
            );
        }
    }

    #[test]
    fn oversized_message_is_rejected() {
        let msg = Message::with_value(vec![0u8; MAX_RECORD_SIZE]);
        assert!(matches!(validate(&msg), Err(Error::RecordTooLarge { .. })));
    }
}
