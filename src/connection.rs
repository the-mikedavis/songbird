#![allow(dead_code)]

use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
};

use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::{Mutex, RwLock};
use tokio::sync::{mpsc, oneshot};

use crate::{
    PublishOutcome,
    codec::{Decode, DecodeError, Encode, Reader},
    commands::{
        self, Command, CommandKey, Notification, Reference, Request, ResponseCode, Status as _,
    },
};

#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    Io(std::io::Error),
    Decode {
        command: CommandKey,
        source: DecodeError,
    },
    Refused {
        command: CommandKey,
        code: ResponseCode,
    },
    FrameTooLarge {
        size: usize,
        frame_max: u32,
    },
    ClosedByPeer {
        code: ResponseCode,
        reason: String,
    },
    Closed,
    // #[error("no SASL mechanism in common; the server offers {offered:?}")]
    // NoCommonMechanism { offered: Vec<String> },
    PublisherIdsExhausted,
    SubscriptionIdsExhausted,
    // #[error(transparent)]
    // Invalid(#[from] ValidationError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => f.write_fmt(format_args!("IO error: {err}")),
            Self::Decode { command, source } => f.write_fmt(format_args!(
                "could not decode a {command:?} frame: {source}"
            )),
            Self::Refused { command, code } => {
                f.write_fmt(format_args!("{command:?} was refused: {code:?}"))
            }
            Self::FrameTooLarge { size, frame_max } => f.write_fmt(format_args!(
                "frame of {size} bytes exceeds the negotiated maximum of {frame_max}"
            )),
            Self::ClosedByPeer { code, reason } => {
                f.write_fmt(format_args!("closed by the peer: {code:?} ({reason})"))
            }
            Self::Closed => f.write_str("the connection is closed"),
            Self::PublisherIdsExhausted => {
                f.write_str("all 256 publisher ids on this connection are occupied")
            }
            Self::SubscriptionIdsExhausted => {
                f.write_str("all 256 subscription ids on this connection are occupied")
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct CorrelationId(u32);

struct CorrelationGuard<'a> {
    shared: &'a Shared,
    id: CorrelationId,
}

impl Drop for CorrelationGuard<'_> {
    fn drop(&mut self) {
        self.shared.correlations.lock().remove(&self.id);
    }
}

#[derive(Debug, Clone)]
pub struct Connection(Arc<Shared>);

#[derive(Debug)]
struct TableEntry<T> {
    stream: String,
    value: T,
}

#[derive(Debug)]
struct Table<T> {
    // NOTE: always 256 slots.
    slots: Box<[Option<TableEntry<T>>]>,
    next: u8,
    len: u16,
}

impl<T> Table<T> {
    fn new() -> Self {
        Self {
            slots: (0..=u8::MAX).map(|_| None).collect(),
            next: 0,
            len: 0,
        }
    }

    fn get(&self, id: u8) -> Option<&T> {
        self.slots[id as usize].as_ref().map(|e| &e.value)
    }

    pub fn insert(&mut self, stream: String, value: T) -> Option<u8> {
        let id = self.free_id()?;
        self.slots[id as usize] = Some(TableEntry { stream, value });
        self.len += 1;
        Some(id)
    }

    pub fn remove(&mut self, id: u8) -> Option<T> {
        let taken = self.slots[id as usize].take();
        if taken.is_some() {
            self.len -= 1;
        }
        taken.map(|e| e.value)
    }

    /// Everything registered against a stream, removed. For MetadataUpdate.
    pub fn drain_stream(&mut self, stream: &str) -> Vec<(u8, T)> {
        let ids: Vec<u8> = self
            .slots
            .iter()
            .enumerate()
            .filter(|(_, s)| s.as_ref().is_some_and(|e| e.stream == stream))
            .map(|(i, _)| i as u8)
            .collect();
        ids.into_iter()
            .filter_map(|id| self.remove(id).map(|v| (id, v)))
            .collect()
    }

    fn free_id(&mut self) -> Option<u8> {
        for _ in 0..=u8::MAX {
            let id = self.next;
            self.next = self.next.wrapping_add(1);
            if self.slots[id as usize].is_none() {
                return Some(id);
            }
        }
        None
    }
}

#[derive(Debug)]
struct SlotGuard<'a, T> {
    table: &'a RwLock<Table<T>>,
    id: u8,
    committed: bool,
}

impl<T> SlotGuard<'_, T> {
    fn commit(mut self) {
        self.committed = true;
    }
}

impl<T> Drop for SlotGuard<'_, T> {
    fn drop(&mut self) {
        if !self.committed {
            self.table.write().remove(self.id);
        }
    }
}

#[derive(Debug)]
struct PublisherSlot {
    outcomes: mpsc::UnboundedSender<PublishOutcome>,
}

#[derive(Debug)]
pub struct Publisher {
    connection: Connection,
    id: u8,
    stream: String,
    outcomes: mpsc::UnboundedReceiver<PublishOutcome>,
    next_publishing_id: AtomicU64,
}

#[derive(Debug)]
struct Shared {
    outbound: mpsc::Sender<Bytes>,

    correlations: Mutex<HashMap<CorrelationId, oneshot::Sender<Bytes>>>,
    next_correlation_id: AtomicU32,

    frame_max: u32,
    // heartbeat: u32 / Duration
    close_reason: Mutex<Option<(ResponseCode, String)>>,

    publishers: RwLock<Table<PublisherSlot>>,
}

impl Connection {
    fn next_correlation_id(&self) -> CorrelationId {
        // Never return 0, it's used as a sentinal by the server.
        loop {
            let id = self.0.next_correlation_id.fetch_add(1, Ordering::Relaxed);
            if id != 0 {
                return CorrelationId(id);
            }
        }
    }

    fn closed_error(&self) -> Error {
        match &*self.0.close_reason.lock() {
            Some((code, reason)) => Error::ClosedByPeer {
                code: *code,
                reason: reason.clone(),
            },
            None => Error::Closed,
        }
    }

    fn encode<C>(&self, correlation: Option<CorrelationId>, command: &C) -> Result<Bytes, Error>
    where
        C: Command + Encode,
    {
        let mut buf = BytesMut::with_capacity(64);
        C::KEY.encode(&mut buf);
        buf.put_u16(C::VERSION);
        if let Some(CorrelationId(id)) = correlation {
            buf.put_u32(id);
        }
        command.encode(&mut buf);
        if self.0.frame_max != 0 && buf.len() > self.0.frame_max as usize {
            return Err(Error::FrameTooLarge {
                size: buf.len(),
                frame_max: self.0.frame_max,
            });
        }
        Ok(buf.freeze())
    }

    async fn call<R>(&self, request: R) -> Result<R::Response, Error>
    where
        R: Request + Encode,
        R::Response: Decode + Send + 'static,
    {
        let id = self.next_correlation_id();
        let frame = self.encode(Some(id), &request)?;
        let (tx, rx) = oneshot::channel();
        self.0.correlations.lock().insert(id, tx);
        let _guard = CorrelationGuard {
            shared: &self.0,
            id,
        };
        self.0
            .outbound
            .send(frame)
            .await
            .map_err(|_| self.closed_error())?;
        let body = rx.await.map_err(|_| self.closed_error())?;

        let mut reader = Reader::new(&body);
        let value = R::Response::decode(&mut reader).map_err(|source| Error::Decode {
            command: R::KEY,
            source,
        })?;
        if !reader.is_empty() {
            return Err(Error::Decode {
                command: R::KEY,
                source: DecodeError::TrailingBytes,
            });
        }
        Ok(value)
    }

    pub async fn notify<N>(&self, notification: N) -> Result<(), Error>
    where
        N: Notification + Encode,
    {
        let frame = self.encode(None, &notification)?;
        self.0
            .outbound
            .send(frame)
            .await
            .map_err(|_| self.closed_error())
    }

    pub async fn query_publisher_sequence(
        &self,
        reference: &Reference,
        stream: &str,
    ) -> Result<u64, Error> {
        let response = self
            .call(commands::QueryPublisherSequence { reference, stream })
            .await?;
        check(commands::QueryPublisherSequence::KEY, response.code)?;
        Ok(response.sequence)
    }

    pub async fn declare_publisher(
        &self,
        stream: &str,
        reference: Option<&Reference>,
    ) -> Result<Publisher, Error> {
        let (tx, outcomes) = mpsc::unbounded_channel();
        let id = self
            .0
            .publishers
            .write()
            .insert(stream.to_owned(), PublisherSlot { outcomes: tx })
            .ok_or(Error::PublisherIdsExhausted)?;
        let slot = SlotGuard {
            table: &self.0.publishers,
            id,
            committed: false,
        };
        let response = self
            .call(commands::DeclarePublisher {
                id: commands::PublisherId(id),
                reference,
                stream,
            })
            .await?;
        check(commands::DeclarePublisher::KEY, response.code())?;
        let next_publishing_id = match reference {
            Some(reference) => self.query_publisher_sequence(reference, stream).await? + 1,
            None => 0,
        };
        slot.commit();
        Ok(Publisher {
            connection: self.clone(),
            id,
            stream: stream.to_owned(),
            outcomes,
            next_publishing_id: AtomicU64::new(next_publishing_id),
        })
    }
}

fn check(command: CommandKey, code: ResponseCode) -> Result<(), Error> {
    if code.is_ok() {
        Ok(())
    } else {
        Err(Error::Refused { command, code })
    }
}
