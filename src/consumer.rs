#![allow(dead_code)]

use bytes::Bytes;

use crate::{
    Offset,
    codec::DecodeError,
    commands::{Chunk, Compression},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    offset: Offset,
    body: Bytes,
}

#[derive(Debug, Clone)]
pub struct Messages<'a> {
    chunk: &'a Chunk,
    rest: &'a [u8],
    next_offset: u64,
    batch: Option<BatchCursor>,
}

impl<'a> Iterator for Messages<'a> {
    type Item = Result<Message, DecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Drain the sub-batch we are inside, if any.
            if let Some(batch) = &mut self.batch {
                match batch.next_entry(&mut self.next_offset) {
                    Some(item) => return Some(item),
                    None => {
                        self.batch = None;
                        continue;
                    }
                }
            }

            if self.rest.is_empty() {
                return None;
            }

            match self.read_outer_entry() {
                Ok(Some(message)) => return Some(Ok(message)), // simple entry
                Ok(None) => continue,                          // sub-batch opened
                Err(e) if e.is_recoverable() => return Some(Err(e)),
                Err(e) => {
                    self.rest = &[];
                    return Some(Err(e));
                }
            }
        }
    }
}

impl<'a> Messages<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.rest.len() < n {
            return Err(DecodeError::Truncated);
        }
        let (head, tail) = self.rest.split_at(n);
        self.rest = tail;
        Ok(head)
    }

    fn read_outer_entry(&mut self) -> Result<Option<Message>, DecodeError> {
        let first = *self.rest.first().ok_or(DecodeError::Truncated)?;

        if first & 0x80 == 0 {
            // 0:1, len:31, body
            let len =
                (u32::from_be_bytes(self.take(4)?.try_into().unwrap()) & 0x7fff_ffff) as usize;
            let body = self.chunk.data.slice_ref(self.take(len)?);
            let offset = Offset::new(self.next_offset);
            self.next_offset += 1;
            return Ok(Some(Message { offset, body }));
        }

        // 1:1, compression:3, reserved:4, records:16, uncompressed_len:32, len:32
        let header = self.take(11)?;
        let compression = Compression::from((header[0] >> 4) & 0x07);
        let records = u16::from_be_bytes(header[1..3].try_into().unwrap());
        let uncompressed_len = u32::from_be_bytes(header[3..7].try_into().unwrap()) as usize;
        let len = u32::from_be_bytes(header[7..11].try_into().unwrap()) as usize;
        let raw = self.take(len)?;

        let buf = if compression == Compression::NONE {
            self.chunk.data.slice_ref(raw)
        } else {
            match decompress(compression, raw, uncompressed_len) {
                Ok(buf) => buf,
                Err(e) => {
                    // `raw` is already consumed, so the outer walk stays aligned.
                    // Advance past the records we cannot read.
                    self.next_offset += records as u64;
                    return Err(e);
                }
            }
        };

        self.batch = Some(BatchCursor {
            buf,
            pos: 0,
            remaining: records,
        });
        Ok(None)
    }
}

fn decompress(
    compression: Compression,
    _raw: &[u8],
    _uncompressed_len: usize,
) -> Result<Bytes, DecodeError> {
    Err(DecodeError::UnsupportedCompression(compression))
}

#[derive(Debug, Clone)]
struct BatchCursor {
    buf: Bytes,
    pos: usize,
    remaining: u16,
}

impl BatchCursor {
    fn next_entry(&mut self, next_offset: &mut u64) -> Option<Result<Message, DecodeError>> {
        if self.remaining == 0 {
            return None;
        }
        match self.read(next_offset) {
            Ok(message) => {
                self.remaining -= 1;
                Some(Ok(message))
            }
            Err(e) => {
                self.remaining = 0;
                Some(Err(e))
            }
        }
    }

    fn read(&mut self, next_offset: &mut u64) -> Result<Message, DecodeError> {
        let rest = self.buf.get(self.pos..).ok_or(DecodeError::Truncated)?;
        if rest.len() < 4 {
            return Err(DecodeError::Truncated);
        }
        let header = u32::from_be_bytes(rest[..4].try_into().unwrap());
        if header & 0x8000_0000 != 0 {
            // Sub-batches do not nest.
            return Err(DecodeError::Malformed);
        }

        let len = (header & 0x7fff_ffff) as usize;
        let start = self.pos + 4;
        let end = start.checked_add(len).ok_or(DecodeError::Malformed)?;
        if end > self.buf.len() {
            return Err(DecodeError::Truncated);
        }

        let body = self.buf.slice(start..end);
        self.pos = end;

        let offset = Offset::new(*next_offset);
        *next_offset += 1;
        Ok(Message { offset, body })
    }
}

impl Chunk {
    pub fn messages(&self) -> Messages<'_> {
        Messages {
            chunk: self,
            rest: &self.data,
            next_offset: self.first_offset.get(),
            batch: None,
        }
    }
}
