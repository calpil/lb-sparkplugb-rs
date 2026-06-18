//! Minimal protobuf (proto2) wire primitives — exactly what Sparkplug B needs.
//!
//! Sparkplug uses only `uint32`/`uint64`/`bool` (varint), `float` (fixed32),
//! `double` (fixed64), and length-delimited `string`/`bytes`/messages. There is
//! no `sint`/zigzag, which keeps this small.

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::{Result, SparkplugError};

/// Varint wire type (`uint32`/`uint64`/`bool`/enum).
pub(crate) const WIRE_VARINT: u8 = 0;
/// 64-bit fixed wire type (`double`).
pub(crate) const WIRE_I64: u8 = 1;
/// Length-delimited wire type (`string`/`bytes`/messages).
pub(crate) const WIRE_LEN: u8 = 2;
/// 32-bit fixed wire type (`float`).
pub(crate) const WIRE_I32: u8 = 5;

/// A growable protobuf writer.
#[derive(Debug, Default)]
pub(crate) struct Writer {
    buf: BytesMut,
}

impl Writer {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn into_bytes(self) -> Bytes {
        self.buf.freeze()
    }

    fn write_varint(&mut self, mut value: u64) {
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            self.buf.put_u8(byte);
            if value == 0 {
                break;
            }
        }
    }

    fn tag(&mut self, field: u32, wire: u8) {
        self.write_varint((u64::from(field) << 3) | u64::from(wire));
    }

    pub(crate) fn uint32_field(&mut self, field: u32, value: u32) {
        self.tag(field, WIRE_VARINT);
        self.write_varint(u64::from(value));
    }

    pub(crate) fn uint64_field(&mut self, field: u32, value: u64) {
        self.tag(field, WIRE_VARINT);
        self.write_varint(value);
    }

    pub(crate) fn bool_field(&mut self, field: u32, value: bool) {
        self.tag(field, WIRE_VARINT);
        self.write_varint(u64::from(value));
    }

    pub(crate) fn float_field(&mut self, field: u32, value: f32) {
        self.tag(field, WIRE_I32);
        self.buf.put_f32_le(value);
    }

    pub(crate) fn double_field(&mut self, field: u32, value: f64) {
        self.tag(field, WIRE_I64);
        self.buf.put_f64_le(value);
    }

    pub(crate) fn string_field(&mut self, field: u32, value: &str) {
        self.bytes_field(field, value.as_bytes());
    }

    pub(crate) fn bytes_field(&mut self, field: u32, value: &[u8]) {
        self.tag(field, WIRE_LEN);
        self.write_varint(value.len() as u64);
        self.buf.put_slice(value);
    }

    pub(crate) fn message_field(&mut self, field: u32, body: &[u8]) {
        self.bytes_field(field, body);
    }
}

/// A protobuf reader over a borrowed buffer.
///
/// Length-delimited reads return slices borrowing the original buffer (`'a`),
/// enabling zero-copy nested decoding.
pub(crate) struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.pos >= self.buf.len()
    }

    fn read_byte(&mut self) -> Result<u8> {
        let byte = *self.buf.get(self.pos).ok_or(SparkplugError::Truncated)?;
        self.pos += 1;
        Ok(byte)
    }

    pub(crate) fn read_varint(&mut self) -> Result<u64> {
        let mut result = 0u64;
        let mut shift = 0u32;
        loop {
            if shift >= 64 {
                return Err(SparkplugError::VarintOverflow);
            }
            let byte = self.read_byte()?;
            result |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
        }
    }

    pub(crate) fn read_tag(&mut self) -> Result<(u32, u8)> {
        let key = self.read_varint()?;
        let field = u32::try_from(key >> 3).map_err(|_| SparkplugError::InvalidWireType(0))?;
        let wire = (key & 0x7) as u8;
        Ok((field, wire))
    }

    fn read_fixed32(&mut self) -> Result<u32> {
        let end = self.pos.checked_add(4).ok_or(SparkplugError::Truncated)?;
        let slice = self
            .buf
            .get(self.pos..end)
            .ok_or(SparkplugError::Truncated)?;
        let arr: [u8; 4] = slice.try_into().expect("slice is exactly 4 bytes");
        self.pos = end;
        Ok(u32::from_le_bytes(arr))
    }

    fn read_fixed64(&mut self) -> Result<u64> {
        let end = self.pos.checked_add(8).ok_or(SparkplugError::Truncated)?;
        let slice = self
            .buf
            .get(self.pos..end)
            .ok_or(SparkplugError::Truncated)?;
        let arr: [u8; 8] = slice.try_into().expect("slice is exactly 8 bytes");
        self.pos = end;
        Ok(u64::from_le_bytes(arr))
    }

    pub(crate) fn read_f32(&mut self) -> Result<f32> {
        Ok(f32::from_bits(self.read_fixed32()?))
    }

    pub(crate) fn read_f64(&mut self) -> Result<f64> {
        Ok(f64::from_bits(self.read_fixed64()?))
    }

    pub(crate) fn read_len_slice(&mut self) -> Result<&'a [u8]> {
        let len = usize::try_from(self.read_varint()?).map_err(|_| SparkplugError::Truncated)?;
        let end = self.pos.checked_add(len).ok_or(SparkplugError::Truncated)?;
        let slice = self
            .buf
            .get(self.pos..end)
            .ok_or(SparkplugError::Truncated)?;
        self.pos = end;
        Ok(slice)
    }

    pub(crate) fn read_string(&mut self) -> Result<String> {
        let slice = self.read_len_slice()?;
        std::str::from_utf8(slice)
            .map(str::to_owned)
            .map_err(|_| SparkplugError::InvalidUtf8)
    }

    pub(crate) fn read_bytes(&mut self) -> Result<Bytes> {
        Ok(Bytes::copy_from_slice(self.read_len_slice()?))
    }

    /// Skip over a field whose tag was already read, given its wire type.
    pub(crate) fn skip(&mut self, wire: u8) -> Result<()> {
        match wire {
            WIRE_VARINT => {
                self.read_varint()?;
            }
            WIRE_I64 => {
                self.read_fixed64()?;
            }
            WIRE_LEN => {
                self.read_len_slice()?;
            }
            WIRE_I32 => {
                self.read_fixed32()?;
            }
            other => return Err(SparkplugError::InvalidWireType(other)),
        }
        Ok(())
    }
}
