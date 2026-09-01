#![allow(dead_code)]

use std::fmt;

use bytes::{BufMut, Bytes};

use crate::commands::Compression;

// Encode

pub trait Encode {
    fn encode(&self, buf: &mut impl BufMut);
}

impl Encode for &str {
    fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u16(self.len() as u16);
        buf.put_slice(self.as_bytes());
    }
}

impl Encode for Bytes {
    fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u32(self.len() as u32);
        buf.put_slice(self);
    }
}

impl<T: Encode> Encode for &[T] {
    fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u32(self.len().try_into().unwrap());
        for item in self.iter() {
            item.encode(buf);
        }
    }
}

impl<A: Encode, B: Encode> Encode for (A, B) {
    fn encode(&self, buf: &mut impl BufMut) {
        self.0.encode(buf);
        self.1.encode(buf);
    }
}

// Decode

pub struct Reader<'a> {
    frame: &'a Bytes,
    rest: &'a [u8],
}

#[derive(Debug)]
pub enum DecodeError {
    /// Not enough bytes in the frame to parse a size the frame requests.
    Truncated,
    /// Non-sensical/invalid input.
    Malformed,
    /// Non-UTF8 string input.
    NotUtf8,
    /// Received a nullary value where the protocol requires non-null.
    UnexpectedNull,
    /// Extra unexpected trailing bytes in frame.
    TrailingBytes,
    /// Any other kind of error.
    Custom(Box<dyn std::error::Error>),
    /// The selected compression is not available.
    UnsupportedCompression(Compression),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => f.write_str("not enough bytes in frame"),
            Self::Malformed => f.write_str("frame contained malformed input"),
            Self::NotUtf8 => f.write_str("frame contained a non-UTF8 string"),
            Self::UnexpectedNull => f.write_str("frame contained an unexpected null size"),
            Self::TrailingBytes => f.write_str("unexpected trailing bytes in frame"),
            Self::UnsupportedCompression(c) => {
                f.write_fmt(format_args!("unsupported compression format {c:?}"))
            }
            Self::Custom(_err) => todo!(),
        }
    }
}

impl DecodeError {
    pub fn is_recoverable(&self) -> bool {
        matches!(self, Self::UnsupportedCompression(_))
    }
}

pub trait Decode: Sized {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError>;
}

impl<T: Decode> Decode for Vec<T> {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let n = reader.u32()?;
        let mut items = Vec::new();
        for _ in 0..n {
            items.push(T::decode(reader)?);
        }
        Ok(items)
    }
}

impl Decode for String {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        reader.str().map(ToOwned::to_owned)
    }
}

impl<A: Decode, B: Decode> Decode for (A, B) {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok((reader.decode()?, reader.decode()?))
    }
}

impl Decode for Bytes {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let len = reader.len32()?.ok_or(DecodeError::UnexpectedNull)?;
        reader.bytes_of(len)
    }
}

impl Decode for i64 {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(i64::from_be_bytes(reader.take(8)?.try_into().unwrap()))
    }
}

impl Decode for u64 {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(u64::from_be_bytes(reader.take(8)?.try_into().unwrap()))
    }
}

impl<'a> Reader<'a> {
    pub fn new(frame: &'a Bytes) -> Self {
        Self {
            frame,
            rest: frame.as_ref(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.rest.is_empty()
    }

    fn take(&mut self, n_bytes: usize) -> Result<&'a [u8], DecodeError> {
        if self.rest.len() < n_bytes {
            return Err(DecodeError::Truncated);
        }
        let (head, tail) = self.rest.split_at(n_bytes);
        self.rest = tail;
        Ok(head)
    }

    pub fn skip(&mut self, n_bytes: usize) -> Result<(), DecodeError> {
        self.take(n_bytes).map(|_| ())
    }

    pub fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    pub fn u16(&mut self) -> Result<u16, DecodeError> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }

    pub fn i16(&mut self) -> Result<i16, DecodeError> {
        Ok(i16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }

    pub fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn i32(&mut self) -> Result<i32, DecodeError> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    /// An int16 length: -1 is null, any other negative is malformed.
    fn len16(&mut self) -> Result<Option<usize>, DecodeError> {
        match self.i16()? {
            -1 => Ok(None),
            n if n < 0 => Err(DecodeError::Malformed),
            n => Ok(Some(n as usize)),
        }
    }

    /// An int32 length: -1 is null, any other negative is malformed.
    fn len32(&mut self) -> Result<Option<usize>, DecodeError> {
        match self.i32()? {
            -1 => Ok(None),
            n if n < 0 => Err(DecodeError::Malformed),
            n => Ok(Some(n as usize)),
        }
    }

    fn str_of(&mut self, len: usize) -> Result<&'a str, DecodeError> {
        std::str::from_utf8(self.take(len)?).map_err(|_| DecodeError::NotUtf8)
    }

    pub fn bytes_of(&mut self, len: usize) -> Result<Bytes, DecodeError> {
        Ok(self.frame.slice_ref(self.take(len)?))
    }

    pub fn remaining_bytes(&mut self) -> Bytes {
        self.frame.slice_ref(self.rest)
    }

    /// "string": int16 length then UTF-8 content.
    pub fn str(&mut self) -> Result<&'a str, DecodeError> {
        let len = self.len16()?.ok_or(DecodeError::UnexpectedNull)?;
        self.str_of(len)
    }

    /// "string" that may be null. Only the publish v2 filter value is.
    pub fn optional_str(&mut self) -> Result<Option<&'a str>, DecodeError> {
        match self.len16()? {
            None => Ok(None),
            Some(len) => self.str_of(len).map(Some),
        }
    }

    pub fn decode<T: Decode>(&mut self) -> Result<T, DecodeError> {
        T::decode(self)
    }
}
