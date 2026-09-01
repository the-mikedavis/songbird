#![allow(dead_code)]

use std::{
    collections::HashMap,
    fmt,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use bytes::{BufMut, Bytes, BytesMut};
use futures_util::{SinkExt as _, StreamExt as _};
use parking_lot::{Mutex, RwLock};
use tokio::{
    net::{
        TcpStream, ToSocketAddrs,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::{mpsc, oneshot},
    time::{Instant, Interval, MissedTickBehavior, timeout},
};
use tokio_util::{
    codec::{Framed, FramedRead, FramedWrite, LengthDelimitedCodec},
    sync::CancellationToken,
};

use crate::{
    Confirms, PublishOutcome, Publisher, PublisherId, PublishingId, Reference, SubscribeOptions,
    SubscriptionEvent, SubscriptionId,
    codec::{Decode, DecodeError, Encode, Reader},
    commands::{self, Command, CommandKey, Mechanism, Notification, Request, ResponseCode, Status},
    publisher::PublishTracker,
    subscription::Subscription,
};

const INITIAL_FRAME_MAX: u32 = 8192;
const MAX_SASL_ROUNDS: usize = 8;

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
    PublisherIdsExhausted,
    SubscriptionIdsExhausted,
    // #[error(transparent)]
    // Invalid(#[from] ValidationError),
    UnexpectedChallenge {
        mechanism: Mechanism,
    },
    HandshakeTimeout {
        step: &'static str,
    },
    UnexpectedFrame {
        step: &'static str,
        expected: CommandKey,
        got: CommandKey,
    },
    NoCommonMechanism {
        offered: Vec<Mechanism>,
    },
    TooManySaslRounds,
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
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
            Self::UnexpectedChallenge { mechanism } => f.write_fmt(format_args!(
                "unexpected SASL challenge for mechanism {mechanism:?}"
            )),
            Self::HandshakeTimeout { step } => f.write_fmt(format_args!(
                "timeout awaiting server handshake in {step} step"
            )),
            Self::UnexpectedFrame {
                step,
                expected,
                got,
            } => f.write_fmt(format_args!(
                "expected command {expected:?} in step {step}, got {got:?}"
            )),
            Self::NoCommonMechanism { offered } => f.write_fmt(format_args!(
                "no SASL offered mechanism in common, offered: {offered:?}"
            )),
            Self::TooManySaslRounds => f.write_str("too many SASL challenge rounds"),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(***)")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Credentials {
    /// SASL PLAIN.
    Plain { username: String, password: Secret },
    /// SASL EXTERNAL: identity comes from the TLS client certificate.
    External,
    /// SASL ANONYMOUS.
    Anonymous,
}

impl Credentials {
    pub fn mechanism(&self) -> Mechanism {
        match self {
            Self::Plain { .. } => Mechanism::PLAIN,
            Self::External => Mechanism::EXTERNAL,
            Self::Anonymous => Mechanism::ANONYMOUS,
        }
    }

    /// The opaque data for the first SaslAuthenticate.
    pub fn initial_response(&self) -> Option<Bytes> {
        match self {
            Self::Plain { username, password } => {
                let mut buf = BytesMut::new();
                buf.put_u8(0);
                buf.put_slice(username.as_bytes());
                buf.put_u8(0);
                buf.put_slice(password.expose().as_bytes());
                Some(buf.freeze())
            }
            Self::External | Self::Anonymous => None,
        }
    }

    /// Answer a SASL_CHALLENGE. No built-in mechanism issues one.
    pub fn respond(&self, _challenge: &[u8]) -> Result<Bytes, Error> {
        Err(Error::UnexpectedChallenge {
            mechanism: self.mechanism(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionConfig {
    pub credentials: Credentials,
    pub virtual_host: String,
    pub client_properties: Vec<(String, String)>,
    pub frame_max: u32,
    /// None to accept server's value, Some(0) to disable heartbeats.
    pub heartbeat: Option<u32>,
    /// Default 16 MiB.
    pub max_inbound_frame: u32,
    /// Default 64.
    pub outbound_capacity: usize,
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

// This is watered-down Connection just for the handshake parts.
struct Handshake<'a> {
    io: &'a mut Framed<TcpStream, LengthDelimitedCodec>,
    next_id: u32,
    limit: u32,
    step_timeout: Duration,
}

impl<'a> Handshake<'a> {
    fn new(io: &'a mut Framed<TcpStream, LengthDelimitedCodec>, limit: u32) -> Self {
        Self {
            io,
            limit,
            next_id: 1,
            // Maybe this should be configurable?
            step_timeout: Duration::from_secs(10),
        }
    }

    async fn send(&mut self, frame: Bytes) -> Result<(), Error> {
        self.io.send(frame).await.map_err(Error::Io)
    }

    async fn recv(
        &mut self,
        step: &'static str,
        expected: CommandKey,
        correlation: Option<CorrelationId>,
    ) -> Result<Bytes, Error> {
        let malformed = |source| Error::Decode {
            command: expected,
            source,
        };

        loop {
            let frame = timeout(self.step_timeout, self.io.next())
                .await
                .map_err(|_| Error::HandshakeTimeout { step })?
                .ok_or(Error::Closed)??;
            let frame = frame.freeze();

            let mut reader = Reader::new(&frame);
            let key = CommandKey::decode(&mut reader).map_err(malformed)?;
            let _version = reader.u16().map_err(malformed)?;

            if key == CommandKey::HEARTBEAT {
                continue;
            }

            if key.command_id() == CommandKey::CLOSE && !key.is_response() {
                let _correlation = reader.u32().map_err(malformed)?;
                let code = ResponseCode::from(reader.u16().map_err(malformed)?);
                let reason = reader.str().map_err(malformed)?.to_owned();
                return Err(Error::ClosedByPeer { code, reason });
            }

            if key != expected {
                return Err(Error::UnexpectedFrame {
                    step,
                    expected,
                    got: key,
                });
            }

            if let Some(want) = correlation {
                let got = CorrelationId(reader.u32().map_err(malformed)?);
                if got != want {
                    return Err(Error::UnexpectedFrame {
                        step,
                        expected,
                        got: key,
                    });
                }
            }

            return Ok(reader.remaining_bytes());
        }
    }

    async fn call<R>(&mut self, step: &'static str, request: R) -> Result<R::Response, Error>
    where
        R: Request + Encode,
        R::Response: Decode,
    {
        let id = CorrelationId(self.next_id);
        self.next_id += 1;

        self.send(encode_frame(&request, Some(id), self.limit)?)
            .await?;
        let body = self.recv(step, R::KEY.to_response(), Some(id)).await?;
        decode_body::<R::Response>(R::KEY, &body)
    }

    async fn peer_properties(
        &mut self,
        client_properties: &[(String, String)],
    ) -> Result<HashMap<String, String>, Error> {
        let properties: Vec<(&str, &str)> = client_properties
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let response = self
            .call(
                "peer properties exchange",
                commands::PeerProperties {
                    properties: &properties,
                },
            )
            .await?;
        check(CommandKey::PEER_PROPERTIES, response.code)?;
        Ok(response.properties.into_iter().collect())
    }

    async fn sasl_handshake(&mut self) -> Result<Vec<Mechanism>, Error> {
        let response = self.call("SASL handshake", commands::SaslHandshake).await?;
        check(CommandKey::SASL_HANDSHAKE, response.code)?;
        Ok(response.mechanisms)
    }

    async fn authenticate(
        &mut self,
        credentials: &Credentials,
        offered: &[Mechanism],
    ) -> Result<(), Error> {
        let mechanism = credentials.mechanism();
        if !offered.iter().any(|m| m == &mechanism) {
            return Err(Error::NoCommonMechanism {
                offered: offered.to_vec(),
            });
        }

        let mut data = credentials.initial_response();

        for _ in 0..MAX_SASL_ROUNDS {
            let response = self
                .call(
                    "SASL authentication",
                    commands::SaslAuthenticate {
                        mechanism: &mechanism,
                        sasl_data: data.as_deref(),
                    },
                )
                .await?;

            match response.code {
                code if code.is_ok() => return Ok(()),
                ResponseCode::SASL_CHALLENGE => {
                    let challenge = response.challenge.unwrap_or_default();
                    data = Some(credentials.respond(&challenge)?);
                }
                code => {
                    return Err(Error::Refused {
                        command: CommandKey::SASL_AUTHENTICATE,
                        code,
                    });
                }
            }
        }

        Err(Error::TooManySaslRounds)
    }

    async fn tune(
        &mut self,
        want_frame_max: u32,
        want_heartbeat: Option<u32>,
    ) -> Result<(u32, u32), Error> {
        // Tune is server-initiated during handshake.
        let body = self.recv("tune", CommandKey::TUNE, None).await?;
        let proposal: commands::Tune = decode_body(CommandKey::TUNE, &body)?;
        let frame_max = negotiate_frame_max(want_frame_max, proposal.frame_max);
        let heartbeat = want_heartbeat.unwrap_or(proposal.heartbeat);
        self.send(encode_response(
            &commands::Tune {
                frame_max,
                heartbeat,
            },
            None,
            self.limit,
        )?)
        .await?;
        Ok((frame_max, heartbeat))
    }

    async fn open(&mut self, virtual_host: &str) -> Result<Vec<(String, String)>, Error> {
        let response = self.call("open", commands::Open { virtual_host }).await?;
        check(CommandKey::OPEN, response.code)?;
        Ok(response.connection_properties)
    }

    async fn exchange_command_versions(&mut self) -> Result<Vec<commands::CommandVersion>, Error> {
        let response = self
            .call(
                "exchange command versions",
                commands::ExchangeCommandVersions {
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
                },
            )
            .await?;
        check(CommandKey::EXCHANGE_COMMAND_VERSIONS, response.code)?;
        Ok(response.commands)
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

    fn get(&self, id: I) -> Option<&T> {
        self.slots[id.into() as usize].as_ref().map(|e| &e.value)
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

    pub fn drain(&mut self) -> Vec<(I, T)> {
        self.len = 0;
        self.next = 0;
        self.slots
            .iter_mut()
            .enumerate()
            .filter_map(|(i, slot)| slot.take().map(|e| (I::from(i as u8), e.value)))
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

#[derive(Debug)]
struct SubscriptionSlot {
    events: mpsc::Sender<SubscriptionEvent>,
}

#[derive(Debug)]
struct Shared {
    outbound: mpsc::Sender<Bytes>,

    correlations: Mutex<HashMap<CorrelationId, oneshot::Sender<Bytes>>>,
    next_correlation_id: AtomicU32,
    command_versions: Vec<commands::CommandVersion>,
    subscriptions: RwLock<Table<SubscriptionId, SubscriptionSlot>>,
    frame_max: u32,
    heartbeat: Duration,
    server_properties: HashMap<String, String>,
    connection_properties: Vec<(String, String)>,

    publishers: RwLock<Table<PublisherId, PublisherSlot>>,
    close_reason: Mutex<Option<(ResponseCode, String)>>,
}

impl Connection {
    pub async fn connect(
        addr: impl ToSocketAddrs,
        config: ConnectionConfig,
    ) -> Result<Self, Error> {
        let socket = TcpStream::connect(addr).await?;
        let mut io = Framed::new(socket, length_codec(config.max_inbound_frame));

        let mut hs = Handshake::new(&mut io, INITIAL_FRAME_MAX);
        let server_properties = hs.peer_properties(&config.client_properties).await?;
        let mechanisms = hs.sasl_handshake().await?;
        hs.authenticate(&config.credentials, &mechanisms).await?;
        let (frame_max, heartbeat) = hs.tune(config.frame_max, config.heartbeat).await?;
        let connection_properties = hs.open(&config.virtual_host).await?;
        hs.limit = frame_max; // the ceiling lifts only once Open has succeeded
        let command_versions = hs.exchange_command_versions().await?;
        let next_correlation_id = hs.next_id;

        // Split the socket rather than the Framed: owned halves for spawning,
        // no BiLock, and an independent frame limit per direction.
        let parts = io.into_parts();
        debug_assert!(parts.write_buf.is_empty());
        let (read_half, write_half) = parts.io.into_split();

        let mut reader = FramedRead::new(read_half, length_codec(config.max_inbound_frame));
        if !parts.read_buf.is_empty() {
            // Carry over anything the server pipelined behind the Open response.
            reader.read_buffer_mut().extend_from_slice(&parts.read_buf);
        }

        let writer = FramedWrite::new(write_half, length_codec(frame_max));

        let (outbound_tx, outbound_rx) = mpsc::channel(config.outbound_capacity);
        let (replies_tx, replies_rx) = mpsc::unbounded_channel();

        let shared = Arc::new(Shared {
            outbound: outbound_tx,
            correlations: Mutex::new(HashMap::new()),
            next_correlation_id: AtomicU32::new(next_correlation_id),
            publishers: RwLock::new(Table::new()),
            subscriptions: RwLock::new(Table::new()),
            frame_max,
            heartbeat: Duration::from_secs(heartbeat as u64),
            server_properties,
            connection_properties,
            command_versions,
            close_reason: Mutex::new(None),
        });

        let token = CancellationToken::new();
        tokio::spawn(writer_task(
            writer,
            outbound_rx,
            replies_rx,
            Duration::from_secs(heartbeat as u64),
            token.clone(),
        ));
        tokio::spawn(reader_task(reader, Arc::clone(&shared), replies_tx, token));

        Ok(Connection(shared))
    }

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
        let response = self.call_raw(request).await?;
        check(R::KEY, response.code())?;
        Ok(response)
    }

    async fn call_raw<R>(&self, request: R) -> Result<R::Response, Error>
    where
        R: Request + Encode,
        R::Response: Decode + Send + 'static,
    {
        let id = self.next_correlation_id();
        let frame = encode_frame(&request, Some(id), self.0.frame_max)?;
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
        decode_body(R::KEY, &body)
    }

    pub async fn notify<N>(&self, notification: N) -> Result<(), Error>
    where
        N: Notification + Encode,
    {
        let frame = encode_frame(&notification, None, self.0.frame_max)?;
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
            Publisher::new(
                self.clone(),
                id,
                stream.to_owned(),
                reference,
                tracker,
                next_publishing_id,
            ),
            Confirms::new(rx),
        ))
    }

    pub(crate) async fn delete_publisher(&self, id: PublisherId) -> Result<bool, Error> {
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

    pub async fn subscribe(
        &self,
        stream: &str,
        options: SubscribeOptions,
    ) -> Result<Subscription, Error> {
        let properties = options.to_properties();
        let borrowed: Vec<(&str, &str)> = properties
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        // At least the credit window, so the server's flow control bounds
        // how far the reader can get ahead of the application.
        let (tx, rx) = mpsc::channel(options.credit as usize * 2);

        let id = self
            .0
            .subscriptions
            .write()
            .insert(stream.to_owned(), SubscriptionSlot { events: tx })
            .ok_or(Error::SubscriptionIdsExhausted)?;
        let slot = SlotGuard {
            table: &self.0.subscriptions,
            id,
            committed: false,
        };

        let response = self
            .call(commands::Subscribe {
                subscription_id: id,
                stream,
                offset: options.offset,
                credit: options.credit,
                properties: &borrowed,
            })
            .await?;
        check(CommandKey::SUBSCRIBE, response.code())?;

        slot.commit();
        Ok(Subscription::new(
            self.clone(),
            id,
            stream.to_owned(),
            rx,
            options.credit,
        ))
    }

    pub(crate) async fn unsubscribe(&self, id: SubscriptionId) -> Result<bool, Error> {
        let result = self
            .call(commands::Unsubscribe {
                subscription_id: id,
            })
            .await;
        self.0.subscriptions.write().remove(id);
        match result?.code() {
            c if c.is_ok() => Ok(true),
            ResponseCode::SUBSCRIPTION_ID_DOES_NOT_EXIST => Ok(false),
            code => Err(Error::Refused {
                command: CommandKey::UNSUBSCRIBE,
                code,
            }),
        }
    }
}

fn encode_frame<C: Command + Encode>(
    cmd: &C,
    corr: Option<CorrelationId>,
    limit: u32,
) -> Result<Bytes, Error> {
    encode_with(C::KEY, C::VERSION, corr, cmd, limit)
}

fn encode_response<C: Command + Encode>(
    cmd: &C,
    corr: Option<CorrelationId>,
    limit: u32,
) -> Result<Bytes, Error> {
    encode_with(C::KEY.to_response(), C::VERSION, corr, cmd, limit)
}

fn encode_with<C: Encode>(
    key: CommandKey,
    version: u16,
    correlation: Option<CorrelationId>,
    body: &C,
    frame_max: u32,
) -> Result<Bytes, Error> {
    let mut buf = BytesMut::with_capacity(64);
    buf.put_u16(key.get());
    buf.put_u16(version);
    if let Some(CorrelationId(id)) = correlation {
        buf.put_u32(id);
    }
    body.encode(&mut buf);

    if frame_max != 0 && buf.len() > frame_max as usize {
        return Err(Error::FrameTooLarge {
            size: buf.len(),
            frame_max,
        });
    }
    Ok(buf.freeze())
}

fn decode_body<T: Decode>(command: CommandKey, body: &Bytes) -> Result<T, Error> {
    let mut r = Reader::new(body);
    let value = T::decode(&mut r).map_err(|source| Error::Decode { command, source })?;
    if !r.is_empty() {
        return Err(Error::Decode {
            command,
            source: DecodeError::TrailingBytes,
        });
    }
    Ok(value)
}

fn length_codec(max_frame: u32) -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .length_field_type::<u32>()
        .max_frame_length(max_frame as usize)
        .new_codec()
}

fn check(command: CommandKey, code: ResponseCode) -> Result<(), Error> {
    if code.is_ok() {
        Ok(())
    } else {
        Err(Error::Refused { command, code })
    }
}

fn negotiate_frame_max(client: u32, server: u32) -> u32 {
    match (client, server) {
        (0, s) => s,
        (c, 0) => c,
        (c, s) => c.min(s),
    }
}

// Writer task

const MAX_BATCH: usize = 32;

async fn writer_task(
    mut writer: FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
    mut outbound: mpsc::Receiver<Bytes>,
    mut replies: mpsc::UnboundedReceiver<Bytes>,
    heartbeat: Duration,
    token: CancellationToken,
) {
    let heartbeat_frame = {
        let mut buf = BytesMut::with_capacity(4);
        buf.put_u16(CommandKey::HEARTBEAT.get());
        buf.put_u16(1);
        buf.freeze()
    };

    let mut ticker = (!heartbeat.is_zero()).then(|| {
        let mut interval = tokio::time::interval(heartbeat / 2);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        interval
    });
    let mut wrote_since_tick = false;

    loop {
        let frame = tokio::select! {
            biased;

            _ = token.cancelled() => break,

            // Replies to Tune, ConsumerUpdate and Close jump the queue.
            Some(frame) = replies.recv() => frame,

            _ = next_tick(&mut ticker) => {
                // Any frame we sent already proved liveness.
                if std::mem::take(&mut wrote_since_tick) {
                    continue;
                }
                heartbeat_frame.clone()
            }

            frame = outbound.recv() => match frame {
                Some(frame) => frame,
                None => break,          // every Connection handle was dropped
            },
        };

        if let Err(_err) = write_batch(&mut writer, frame, &mut outbound, &mut replies).await {
            // Log err
            break;
        }
        wrote_since_tick = true;
    }

    let _ = writer.close().await; // flushes, then shuts the write half
    token.cancel();
}

/// Blocks forever when heartbeats are disabled, so the select arm never fires.
async fn next_tick(ticker: &mut Option<Interval>) {
    match ticker {
        Some(interval) => {
            interval.tick().await;
        }
        None => std::future::pending::<()>().await,
    }
}

async fn write_batch(
    writer: &mut FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
    first: Bytes,
    outbound: &mut mpsc::Receiver<Bytes>,
    replies: &mut mpsc::UnboundedReceiver<Bytes>,
) -> Result<(), std::io::Error> {
    writer.feed(first).await?;

    let mut n = 1;
    while n < MAX_BATCH {
        let next = replies.try_recv().ok().or_else(|| outbound.try_recv().ok());
        match next {
            Some(frame) => {
                writer.feed(frame).await?;
                n += 1
            }
            None => break,
        }
    }

    writer.flush().await
}

// Reader task

enum Flow {
    Continue,
    Stop(Option<(ResponseCode, String)>),
}

async fn reader_task(
    mut reader: FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
    shared: Arc<Shared>,
    replies: mpsc::UnboundedSender<Bytes>,
    token: CancellationToken,
) {
    // The server declares a peer dead after 2-3 intervals; mirror that.
    let idle_limit = (!shared.heartbeat.is_zero()).then(|| shared.heartbeat * 5 / 2);
    let mut idle_ticker =
        (!shared.heartbeat.is_zero()).then(|| tokio::time::interval(shared.heartbeat));
    let mut last_frame = Instant::now();
    let mut peer_close = None;

    loop {
        let frame = tokio::select! {
            biased;

            _ = token.cancelled() => break,

            _ = next_tick(&mut idle_ticker) => {
                if idle_limit.is_some_and(|limit| last_frame.elapsed() > limit) {
                    // log: peer silent past the heartbeat window
                    break;
                }
                continue;
            }

            frame = reader.next() => match frame {
                Some(Ok(frame)) => frame.freeze(),
                Some(Err(_err)) => {
                    // log err
                    break
                }
                None => break,
            },
        };

        last_frame = Instant::now();

        match dispatch(&shared, &replies, frame).await {
            Flow::Continue => {}
            Flow::Stop(reason) => {
                peer_close = reason;
                break;
            }
        }
    }

    teardown(&shared, peer_close);
    token.cancel();
}

async fn dispatch(
    shared: &Arc<Shared>,
    replies: &mpsc::UnboundedSender<Bytes>,
    frame: Bytes,
) -> Flow {
    let mut reader = Reader::new(&frame);
    let (Ok(key), Ok(version)) = (CommandKey::decode(&mut reader), reader.u16()) else {
        return Flow::Continue;
    };

    if key == CommandKey::HEARTBEAT {
        // liveness is recorded by the outer frame loop
        return Flow::Continue;
    }

    // A Close *request* from the peer. The response form (0x8016) is our own
    // Close being answered, and goes through the correlation map below.
    if key == CommandKey::CLOSE {
        return handle_peer_close(shared, replies, &mut reader);
    }

    // CreditResponse has no correlation id (rabbit_stream_core.erl:1046-1051):
    // just a code and a subscription id.
    if key == CommandKey::CREDIT.to_response() {
        credit_refused(shared, &mut reader).await;
        return Flow::Continue;
    }

    if key.is_response() {
        let Ok(id) = reader.u32() else {
            return Flow::Continue;
        };
        if let Some(tx) = shared.correlations.lock().remove(&CorrelationId(id)) {
            let _ = tx.send(reader.remaining_bytes());
        }
        return Flow::Continue;
    }

    match key {
        CommandKey::DELIVER => deliver(shared, version, &mut reader).await,
        CommandKey::PUBLISH_CONFIRM => publish_confirm(shared, &mut reader),
        CommandKey::PUBLISH_ERROR => publish_error(shared, &mut reader),
        // CommandKey::METADATA_UPDATE => metadata_update(shared, &mut reader),
        // CommandKey::CONSUMER_UPDATE => consumer_update(shared, replies, &mut reader),
        // Other commands...?
        _ => (),
    }
    Flow::Continue
}

fn teardown(shared: &Arc<Shared>, peer_close: Option<(ResponseCode, String)>) {
    if let Some(reason) = peer_close {
        *shared.close_reason.lock() = Some(reason);
    }
    // Dropping every sender resolves each pending call to closed_error().
    shared.correlations.lock().clear();
    // Without this, drain_outstanding waits forever on ids that can never resolve.
    for (_, slot) in shared.publishers.write().drain() {
        slot.tracker.abandon();
    }
    for (_, slot) in shared.subscriptions.write().drain() {
        let _ = slot.events.try_send(SubscriptionEvent::Unavailable(
            ResponseCode::STREAM_NOT_AVAILABLE,
        ));
    }
}

// Server->client command handlers

async fn deliver(shared: &Arc<Shared>, version: u16, reader: &mut Reader<'_>) {
    let (id, chunk) = match version {
        1 => match commands::Deliver::decode(reader) {
            Ok(d) => (d.subscription_id, d.chunk),
            Err(_) => return,
        },
        2 => match commands::DeliverV2::decode(reader) {
            Ok(d) => (d.subscription_id, d.chunk),
            Err(_) => return,
        },
        _unknown_version => return,
    };

    // Clone the sender out from under the lock; the send below awaits.
    let sender = shared
        .subscriptions
        .read()
        .get(id)
        .map(|slot| slot.events.clone());

    if let Some(tx) = sender
        && tx.send(SubscriptionEvent::Chunk(chunk)).await.is_err()
    {
        // error logging
    }
}

async fn credit_refused(shared: &Arc<Shared>, reader: &mut Reader<'_>) {
    let Ok(cmd) = commands::CreditResponse::decode(reader) else {
        return;
    };
    let sender = shared
        .subscriptions
        .read()
        .get(cmd.subscription_id)
        .map(|slot| slot.events.clone());
    if let Some(tx) = sender
        && tx
            .send(SubscriptionEvent::CreditRefused(cmd.code))
            .await
            .is_err()
    {
        // error logging
    }
}

fn publish_confirm(shared: &Arc<Shared>, reader: &mut Reader<'_>) {
    let Ok(cmd) = commands::PublishConfirm::decode(reader) else {
        return;
    };
    let sender = shared
        .publishers
        .read()
        .get(cmd.publisher_id)
        .map(|slot| slot.outcomes.clone());
    if let Some(tx) = sender
        && tx
            .send(PublishOutcome::Confirmed(cmd.publishing_ids))
            .is_err()
    {
        // error logging
    }
}

fn publish_error(shared: &Arc<Shared>, reader: &mut Reader<'_>) {
    let Ok(cmd) = commands::PublishError::decode(reader) else {
        return;
    };
    let sender = shared
        .publishers
        .read()
        .get(cmd.publisher_id)
        .map(|slot| slot.outcomes.clone());
    if let Some(tx) = sender
        && tx.send(PublishOutcome::Failed(cmd.errors)).is_err()
    {
        // error logging
    }
}

fn read_close(r: &mut Reader<'_>) -> Result<(CorrelationId, ResponseCode, String), DecodeError> {
    let correlation = CorrelationId(r.u32()?);
    let code = ResponseCode::from(r.u16()?);
    let reason = r.str()?.to_owned();
    Ok((correlation, code, reason))
}

fn handle_peer_close(
    shared: &Arc<Shared>,
    replies: &mpsc::UnboundedSender<Bytes>,
    reader: &mut Reader<'_>,
) -> Flow {
    let (correlation, code, reason) = match read_close(reader) {
        Ok(parts) => parts,
        Err(_e) => {
            // log malformed close
            return Flow::Stop(None);
        }
    };

    if let Ok(frame) = encode_response(
        &commands::Close {
            code: ResponseCode::OK,
            // TODO: is this optional?
            reason: "".into(),
        },
        Some(correlation),
        shared.frame_max,
    ) {
        let _ = replies.send(frame);
    }

    Flow::Stop(Some((code, reason)))
}
