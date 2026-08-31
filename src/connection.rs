#![allow(dead_code)]

use std::{
    collections::HashMap,
    fmt,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
};

use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::{Mutex, RwLock};
use tokio::sync::{Notify, mpsc, oneshot};

use crate::{
    PublishOutcome, PublisherId, PublishingId, Reference,
    codec::{Decode, DecodeError, Encode, Reader},
    commands::{
        self, Chunk, Command, CommandKey, Compression, Notification, Offset, Request, ResponseCode,
        Status,
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
struct Table<I, T> {
    // NOTE: always 256 slots.
    slots: Box<[Option<TableEntry<T>>]>,
    next: u8,
    len: u16,
    _id: PhantomData<I>,
}

impl<I: From<u8> + Into<u8> + Copy, T> Table<I, T> {
    fn new() -> Self {
        Self {
            slots: (0..=u8::MAX).map(|_| None).collect(),
            next: 0,
            len: 0,
            _id: PhantomData,
        }
    }

    fn get(&self, id: u8) -> Option<&T> {
        self.slots[id as usize].as_ref().map(|e| &e.value)
    }

    pub fn insert(&mut self, stream: String, value: T) -> Option<I> {
        let id = self.free_slot()?;
        self.slots[id as usize] = Some(TableEntry { stream, value });
        self.len += 1;
        Some(id.into())
    }

    pub fn remove(&mut self, id: I) -> Option<T> {
        let taken = self.slots[id.into() as usize].take();
        if taken.is_some() {
            self.len -= 1;
        }
        taken.map(|e| e.value)
    }

    /// Everything registered against a stream, removed. For MetadataUpdate.
    pub fn drain_stream(&mut self, stream: &str) -> Vec<(I, T)> {
        let ids: Vec<I> = self
            .slots
            .iter()
            .enumerate()
            .filter(|(_, s)| s.as_ref().is_some_and(|e| e.stream == stream))
            .map(|(i, _)| (i as u8).into())
            .collect();
        ids.into_iter()
            .filter_map(|id| self.remove(id).map(|v| (id, v)))
            .collect()
    }

    fn free_slot(&mut self) -> Option<u8> {
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
struct SlotGuard<'a, I: From<u8> + Into<u8> + Copy, T> {
    table: &'a RwLock<Table<I, T>>,
    id: I,
    committed: bool,
}

impl<I: From<u8> + Into<u8> + Copy, T> SlotGuard<'_, I, T> {
    fn commit(mut self) {
        self.committed = true;
    }
}

impl<I: From<u8> + Into<u8> + Copy, T> Drop for SlotGuard<'_, I, T> {
    fn drop(&mut self) {
        if !self.committed {
            self.table.write().remove(self.id);
        }
    }
}

#[derive(Debug)]
struct PublisherSlot {
    outcomes: mpsc::UnboundedSender<PublishOutcome>,
    tracker: Arc<PublishTracker>,
}

#[derive(Default, Debug)]
struct PublishTracker {
    outstanding: AtomicU64,
    drained: Notify,
}

impl PublishTracker {
    fn sent(&self, n: u64) {
        self.outstanding.fetch_add(n, Ordering::AcqRel);
    }

    fn resolved(&self, n: u64) {
        if self.outstanding.fetch_sub(n, Ordering::AcqRel) == n {
            self.drained.notify_waiters();
        }
    }

    fn abandon(&self) {
        self.outstanding.store(0, Ordering::Release);
        self.drained.notify_waiters();
    }
}

#[derive(Debug)]
pub struct Publisher {
    connection: Connection,
    id: PublisherId,
    stream: String,
    reference: Option<Reference>,
    tracker: Arc<PublishTracker>,
    next_publishing_id: tokio::sync::Mutex<PublishingId>,
    closed: AtomicBool,
}

pub struct Confirms {
    outcomes: mpsc::UnboundedReceiver<PublishOutcome>,
}

impl Publisher {
    pub fn id(&self) -> PublisherId {
        self.id
    }

    pub async fn send(&self, body: Bytes) -> Result<PublishingId, Error> {
        let mut next = self.next_publishing_id.lock().await;
        let id = *next;
        self.tracker.sent(1);
        self.connection
            .notify(commands::Publish {
                publisher: self.id,
                messages: &[commands::PublishedMessage { id, body }],
            })
            .await
            .inspect_err(|_| self.tracker.resolved(1))?;
        *next = id + 1;
        Ok(id)
    }

    pub async fn send_batch(&self, bodies: Vec<Bytes>) -> Result<Vec<PublishingId>, Error> {
        let n = bodies.len() as u64;
        let mut next = self.next_publishing_id.lock().await;
        let first = *next;
        let messages: Vec<_> = bodies
            .into_iter()
            .enumerate()
            .map(|(i, body)| commands::PublishedMessage {
                id: first + i as u64,
                body,
            })
            .collect();
        // Count before sending.
        self.tracker.sent(n);
        self.connection
            .notify(commands::Publish {
                publisher: self.id,
                messages: &messages,
            })
            .await
            .inspect_err(|_| self.tracker.resolved(n))?;
        *next = first + n;
        Ok((first + first..n).collect())
    }

    async fn drain_outstanding(&self) {
        loop {
            // Create the future before checking, so a resolution between
            // the check and the await isn't missed.
            let notified = self.tracker.drained.notified();
            if self.tracker.outstanding.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }

    pub async fn close(&self) -> Result<(), Error> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(()); // already closed
        }
        self.drain_outstanding().await;
        self.connection.delete_publisher(self.id).await?;
        Ok(())
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let connection = self.connection.clone();
            let id = self.id;
            handle.spawn(async move {
                let _ = connection.delete_publisher(id).await;
            });
        }
    }
}

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

// impl<'a> Iterator for Messages<'a> {
//     type Item = Result<Message, DecodeError>;
// }

#[derive(Debug)]
struct Shared {
    outbound: mpsc::Sender<Bytes>,

    correlations: Mutex<HashMap<CorrelationId, oneshot::Sender<Bytes>>>,
    next_correlation_id: AtomicU32,

    frame_max: u32,
    // heartbeat: u32 / Duration
    close_reason: Mutex<Option<(ResponseCode, String)>>,

    publishers: RwLock<Table<PublisherId, PublisherSlot>>,
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

    // Same as call_raw but checks the response code.
    async fn call<R>(&self, request: R) -> Result<R::Response, Error>
    where
        R: Request + Encode,
        R::Response: Decode + Status + Send + 'static,
    {
        self.call_raw(request)
            .await?
            .into_result()
            .map_err(|code| Error::Refused {
                command: R::KEY,
                code,
            })
    }

    async fn call_raw<R>(&self, request: R) -> Result<R::Response, Error>
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
    ) -> Result<PublishingId, Error> {
        self.call(commands::QueryPublisherSequence { reference, stream })
            .await
            .map(|resp| resp.sequence)
    }

    pub async fn declare_publisher(
        &self,
        stream: &str,
        reference: Option<Reference>,
    ) -> Result<(Publisher, Confirms), Error> {
        let next_publishing_id = match &reference {
            Some(reference) => self.query_publisher_sequence(reference, stream).await? + 1,
            None => 1,
        };

        let (tx, rx) = mpsc::unbounded_channel();
        let tracker = Arc::new(PublishTracker::default());
        let id = self
            .0
            .publishers
            .write()
            .insert(
                stream.to_owned(),
                PublisherSlot {
                    outcomes: tx,
                    tracker: tracker.clone(),
                },
            )
            .ok_or(Error::PublisherIdsExhausted)?;
        let slot = SlotGuard {
            table: &self.0.publishers,
            id,
            committed: false,
        };

        self.call(commands::DeclarePublisher {
            id,
            reference: reference.as_deref().unwrap_or(""),
            stream,
        })
        .await?;

        slot.commit();

        Ok((
            Publisher {
                connection: self.clone(),
                id,
                stream: stream.to_owned(),
                reference,
                tracker,
                next_publishing_id: tokio::sync::Mutex::new(next_publishing_id),
                closed: AtomicBool::new(false),
            },
            Confirms { outcomes: rx },
        ))
    }

    async fn delete_publisher(&self, id: PublisherId) -> Result<bool, Error> {
        let result = self.call_raw(commands::DeletePublisher { id }).await;
        self.0.publishers.write().remove(id);
        match result?.code() {
            c if c.is_ok() => Ok(true),
            ResponseCode::PUBLISHER_DOES_NOT_EXIST => Ok(false),
            code => Err(Error::Refused {
                command: CommandKey::DELETE_PUBLISHER,
                code,
            }),
        }
    }

    pub async fn exchange_command_versions(&self) -> Result<Vec<commands::CommandVersion>, Error> {
        self.call(commands::ExchangeCommandVersions {
            // Extra versions supported in this client. Commands with only
            // one version are not necessary to exchange.
            commands: &[
                commands::CommandVersion {
                    key: CommandKey::PUBLISH,
                    min_version: 1,
                    max_version: commands::PublishV2::VERSION,
                },
                commands::CommandVersion {
                    key: CommandKey::DELIVER,
                    min_version: 1,
                    max_version: commands::DeliverV2::VERSION,
                },
            ],
        })
        .await
        .map(|resp| resp.commands)
    }
}

fn check(command: CommandKey, code: ResponseCode) -> Result<(), Error> {
    if code.is_ok() {
        Ok(())
    } else {
        Err(Error::Refused { command, code })
    }
}
