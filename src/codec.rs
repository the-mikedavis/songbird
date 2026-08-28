#![allow(dead_code)]

use bytes::BufMut;

pub trait Encode {
    fn encode(&self, buf: &mut impl BufMut);
}

impl Encode for &str {
    fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u16(self.len() as u16);
        buf.put_slice(self.as_bytes());
    }
}

impl<T: Encode> Encode for &[T] {
    fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u32(self.len().try_into().unwrap());
    }
}

impl<A: Encode, B: Encode> Encode for (A, B) {
    fn encode(&self, buf: &mut impl BufMut) {
        self.0.encode(buf);
        self.1.encode(buf);
    }
}
