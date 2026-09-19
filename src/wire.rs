//! The Kafka protocol's primitive types.
//!
//! Kafka's wire format is a hand-rolled binary encoding: big-endian fixed-width
//! integers, length-prefixed strings and arrays, and — since the "flexible
//! versions" change (KIP-482) — unsigned varint lengths plus a tagged-field
//! section appended to every struct. Every request/response in this crate is
//! built out of exactly these pieces.

use anyhow::{bail, Result};
use bytes::{Buf, BufMut, BytesMut};

/// Writes Kafka protocol primitives into a growable buffer.
#[derive(Debug, Default)]
pub struct Encoder {
    buf: BytesMut,
}

impl Encoder {
    pub fn new() -> Self {
        Self {
            buf: BytesMut::new(),
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.buf.to_vec()
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn put_raw(&mut self, bytes: &[u8]) {
        self.buf.put_slice(bytes);
    }

    pub fn put_i8(&mut self, v: i8) {
        self.buf.put_i8(v);
    }

    pub fn put_bool(&mut self, v: bool) {
        self.buf.put_u8(if v { 1 } else { 0 });
    }

    pub fn put_i16(&mut self, v: i16) {
        self.buf.put_i16(v);
    }

    pub fn put_i32(&mut self, v: i32) {
        self.buf.put_i32(v);
    }

    pub fn put_i64(&mut self, v: i64) {
        self.buf.put_i64(v);
    }

    /// A `uuid` field: 16 raw bytes, no length prefix.
    pub fn put_uuid(&mut self, uuid: &[u8; 16]) {
        self.buf.put_slice(uuid);
    }

    /// The all-zero UUID, which Kafka uses as "no topic id / look this up by
    /// name instead" in Metadata v10+ requests.
    pub fn put_zero_uuid(&mut self) {
        self.buf.put_slice(&[0u8; 16]);
    }

    /// Protobuf-style base-128 varint, low group first. Used for every length
    /// prefix in a flexible-version message.
    pub fn put_unsigned_varint(&mut self, mut v: u32) {
        loop {
            if v < 0x80 {
                self.buf.put_u8(v as u8);
                return;
            }
            self.buf.put_u8((v as u8 & 0x7f) | 0x80);
            v >>= 7;
        }
    }

    /// Non-compact STRING: `int16` byte length, then UTF-8.
    pub fn put_string(&mut self, s: &str) {
        self.buf.put_i16(s.len() as i16);
        self.buf.put_slice(s.as_bytes());
    }

    /// Non-compact NULLABLE_STRING: length `-1` means null.
    pub fn put_nullable_string(&mut self, s: Option<&str>) {
        match s {
            Some(s) => self.put_string(s),
            None => self.buf.put_i16(-1),
        }
    }

    /// COMPACT_STRING: varint of `len + 1`, then UTF-8. (`0` is reserved for
    /// null, which is why every compact length is stored off-by-one.)
    pub fn put_compact_string(&mut self, s: &str) {
        self.put_unsigned_varint(s.len() as u32 + 1);
        self.buf.put_slice(s.as_bytes());
    }

    pub fn put_compact_nullable_string(&mut self, s: Option<&str>) {
        match s {
            Some(s) => self.put_compact_string(s),
            None => self.put_unsigned_varint(0),
        }
    }

    /// COMPACT_ARRAY length: varint of `count + 1`.
    pub fn put_compact_array_len(&mut self, n: usize) {
        self.put_unsigned_varint(n as u32 + 1);
    }

    /// A null COMPACT_ARRAY, distinct from an empty one.
    pub fn put_null_compact_array(&mut self) {
        self.put_unsigned_varint(0);
    }

    /// Non-compact ARRAY length: `int32` count, `-1` for null.
    pub fn put_array_len(&mut self, n: usize) {
        self.buf.put_i32(n as i32);
    }

    /// The tagged-field section every flexible-version struct ends with. This
    /// crate never *sends* a tagged field, so it is always a single zero byte,
    /// but the byte is mandatory — omitting it desynchronises the whole frame.
    pub fn put_empty_tagged_fields(&mut self) {
        self.put_unsigned_varint(0);
    }
}

/// Reads Kafka protocol primitives out of a response frame.
#[derive(Debug)]
pub struct Decoder<'a> {
    buf: &'a [u8],
}

impl<'a> Decoder<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    pub fn remaining(&self) -> usize {
        self.buf.len()
    }

    fn need(&self, n: usize) -> Result<()> {
        if self.buf.len() < n {
            bail!(
                "truncated response: needed {n} more byte(s), {} left",
                self.buf.len()
            );
        }
        Ok(())
    }

    pub fn i8(&mut self) -> Result<i8> {
        self.need(1)?;
        Ok(self.buf.get_i8())
    }

    pub fn bool(&mut self) -> Result<bool> {
        self.need(1)?;
        Ok(self.buf.get_u8() != 0)
    }

    pub fn i16(&mut self) -> Result<i16> {
        self.need(2)?;
        Ok(self.buf.get_i16())
    }

    pub fn i32(&mut self) -> Result<i32> {
        self.need(4)?;
        Ok(self.buf.get_i32())
    }

    pub fn i64(&mut self) -> Result<i64> {
        self.need(8)?;
        Ok(self.buf.get_i64())
    }

    pub fn uuid(&mut self) -> Result<[u8; 16]> {
        self.need(16)?;
        let mut out = [0u8; 16];
        self.buf.copy_to_slice(&mut out);
        Ok(out)
    }

    pub fn raw(&mut self, n: usize) -> Result<&'a [u8]> {
        self.need(n)?;
        let (head, tail) = self.buf.split_at(n);
        self.buf = tail;
        Ok(head)
    }

    pub fn unsigned_varint(&mut self) -> Result<u32> {
        let mut value: u32 = 0;
        let mut shift = 0;
        loop {
            if shift > 28 {
                bail!("malformed unsigned varint: more than 5 bytes");
            }
            let byte = {
                self.need(1)?;
                self.buf.get_u8()
            };
            value |= u32::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
            shift += 7;
        }
    }

    pub fn string(&mut self) -> Result<String> {
        match self.nullable_string()? {
            Some(s) => Ok(s),
            None => bail!("unexpected null in a non-nullable STRING field"),
        }
    }

    pub fn nullable_string(&mut self) -> Result<Option<String>> {
        let len = self.i16()?;
        if len < 0 {
            return Ok(None);
        }
        let bytes = self.raw(len as usize)?;
        Ok(Some(String::from_utf8(bytes.to_vec())?))
    }

    pub fn compact_string(&mut self) -> Result<String> {
        match self.compact_nullable_string()? {
            Some(s) => Ok(s),
            None => bail!("unexpected null in a non-nullable COMPACT_STRING field"),
        }
    }

    pub fn compact_nullable_string(&mut self) -> Result<Option<String>> {
        let raw = self.unsigned_varint()?;
        if raw == 0 {
            return Ok(None);
        }
        let bytes = self.raw((raw - 1) as usize)?;
        Ok(Some(String::from_utf8(bytes.to_vec())?))
    }

    /// Non-compact ARRAY length; `None` for a null array.
    pub fn array_len(&mut self) -> Result<Option<usize>> {
        let n = self.i32()?;
        if n < 0 {
            return Ok(None);
        }
        Ok(Some(n as usize))
    }

    /// COMPACT_ARRAY length; `None` for a null array.
    pub fn compact_array_len(&mut self) -> Result<Option<usize>> {
        let raw = self.unsigned_varint()?;
        if raw == 0 {
            return Ok(None);
        }
        Ok(Some((raw - 1) as usize))
    }

    /// Reads and discards a tagged-field section: a varint count, then that
    /// many `(varint tag, varint size, size bytes)` triples. Skipping unknown
    /// tags rather than failing on them is the entire point of the mechanism —
    /// a newer broker may attach fields this client has never heard of.
    pub fn skip_tagged_fields(&mut self) -> Result<()> {
        let count = self.unsigned_varint()?;
        for _ in 0..count {
            let _tag = self.unsigned_varint()?;
            let size = self.unsigned_varint()? as usize;
            self.raw(size)?;
        }
        Ok(())
    }

    /// Decodes a COMPACT_ARRAY of structs with `f`, treating null as empty.
    pub fn compact_array<T, F>(&mut self, mut f: F) -> Result<Vec<T>>
    where
        F: FnMut(&mut Decoder<'a>) -> Result<T>,
    {
        let n = self.compact_array_len()?.unwrap_or(0);
        let mut out = Vec::with_capacity(n.min(1024));
        for _ in 0..n {
            out.push(f(self)?);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsigned_varint_matches_known_encodings() {
        // The canonical base-128 boundaries. Getting 128 wrong (a one-byte
        // 0x80) silently corrupts every compact length prefix past 127.
        for (value, expected) in [
            (0u32, vec![0x00]),
            (1, vec![0x01]),
            (127, vec![0x7f]),
            (128, vec![0x80, 0x01]),
            (300, vec![0xac, 0x02]),
            (16383, vec![0xff, 0x7f]),
            (16384, vec![0x80, 0x80, 0x01]),
        ] {
            let mut e = Encoder::new();
            e.put_unsigned_varint(value);
            assert_eq!(e.as_slice(), expected.as_slice(), "encoding {value}");

            let bytes = e.into_vec();
            let mut d = Decoder::new(&bytes);
            assert_eq!(d.unsigned_varint().unwrap(), value);
            assert_eq!(d.remaining(), 0);
        }
    }

    #[test]
    fn compact_string_is_length_plus_one() {
        let mut e = Encoder::new();
        e.put_compact_string("demo");
        assert_eq!(e.as_slice(), &[0x05, b'd', b'e', b'm', b'o']);

        let mut e = Encoder::new();
        e.put_compact_string("");
        assert_eq!(e.as_slice(), &[0x01]);
    }

    #[test]
    fn null_and_empty_compact_values_are_distinct() {
        let mut e = Encoder::new();
        e.put_compact_nullable_string(None);
        e.put_compact_nullable_string(Some(""));
        assert_eq!(e.as_slice(), &[0x00, 0x01]);

        let bytes = e.into_vec();
        let mut d = Decoder::new(&bytes);
        assert_eq!(d.compact_nullable_string().unwrap(), None);
        assert_eq!(d.compact_nullable_string().unwrap(), Some(String::new()));

        let mut e = Encoder::new();
        e.put_null_compact_array();
        e.put_compact_array_len(0);
        assert_eq!(e.as_slice(), &[0x00, 0x01]);
        let bytes = e.into_vec();
        let mut d = Decoder::new(&bytes);
        assert_eq!(d.compact_array_len().unwrap(), None);
        assert_eq!(d.compact_array_len().unwrap(), Some(0));
    }

    #[test]
    fn non_compact_null_string_is_minus_one() {
        let mut e = Encoder::new();
        e.put_nullable_string(None);
        e.put_nullable_string(Some("kafkalag"));
        assert_eq!(
            e.as_slice(),
            &[0xff, 0xff, 0x00, 0x08, b'k', b'a', b'f', b'k', b'a', b'l', b'a', b'g']
        );

        let bytes = e.into_vec();
        let mut d = Decoder::new(&bytes);
        assert_eq!(d.nullable_string().unwrap(), None);
        assert_eq!(d.nullable_string().unwrap().as_deref(), Some("kafkalag"));
    }

    #[test]
    fn skips_tagged_fields_it_does_not_understand() {
        // Two tagged fields: tag 0 with 3 bytes, tag 7 with 1 byte, then a
        // trailing sentinel int32 that must still line up afterwards.
        let frame: &[u8] = &[
            0x02, // two tagged fields
            0x00, 0x03, 0xaa, 0xbb, 0xcc, // tag 0, size 3
            0x07, 0x01, 0xdd, // tag 7, size 1
            0x00, 0x00, 0x00, 0x2a, // sentinel: 42
        ];
        let mut d = Decoder::new(frame);
        d.skip_tagged_fields().unwrap();
        assert_eq!(d.i32().unwrap(), 42);
        assert_eq!(d.remaining(), 0);
    }

    #[test]
    fn truncated_input_errors_instead_of_panicking() {
        let mut d = Decoder::new(&[0x00, 0x01]);
        assert!(d.i32().is_err());

        let mut d = Decoder::new(&[0x00, 0x05, b'h', b'i']);
        assert!(d.string().is_err());
    }

    #[test]
    fn integers_are_big_endian() {
        let mut e = Encoder::new();
        e.put_i16(-1);
        e.put_i32(1);
        e.put_i64(-1);
        assert_eq!(
            e.as_slice(),
            &[
                0xff, 0xff, // -1i16
                0x00, 0x00, 0x00, 0x01, // 1i32
                0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // -1i64
            ]
        );
    }
}
