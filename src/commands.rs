#![allow(dead_code)]

use std::{
    borrow::Cow,
    fmt,
    num::{NonZeroU16, NonZeroU32},
};

use bytes::{BufMut, Bytes};

use crate::codec::{Decode, DecodeError, Encode, Reader};

macro_rules! wire_code {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident($repr:ty);
        $(
            $(#[$kmeta:meta])*
            ($konst:ident, $value:expr, $phrase:expr);
        )+
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        $vis struct $name($repr);

        impl $name {
            $(
                $(#[$kmeta])*
                pub const $konst: Self = Self($value);
            )+

            /// The underlying wire value.
            pub const fn get(self) -> $repr {
                self.0
            }

            /// The spec name for this value, or `None` if the peer sent an unknown one.
            pub fn name(self) -> Option<&'static str> {
                match self {
                    $( Self::$konst => Some($phrase), )+
                    _ => None,
                }
            }
        }

        impl From<$repr> for $name {
            fn from(value: $repr) -> Self {
                Self(value)
            }
        }

        impl From<$name> for $repr {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl core::fmt::Debug for $name {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                match self.name() {
                    Some(name) => f.write_str(name),
                    None => write!(f, concat!(stringify!($name), "({:#x})"), self.0),
                }
            }
        }
    };
}

wire_code! {
    pub struct ResponseCode(u16);

    (OK,                                   0x01, "OK");
    (STREAM_DOES_NOT_EXIST,                0x02, "Stream does not exist");
    (SUBSCRIPTION_ID_EXISTS,               0x03, "Subscription ID already exists");
    (SUBSCRIPTION_ID_DOES_NOT_EXIST,       0x04, "Subscription ID does not exist");
    (STREAM_ALREADY_EXISTS,                0x05, "Stream already exists");
    (STREAM_NOT_AVAILABLE,                 0x06, "Stream not available");
    (SASL_MECHANISM_NOT_SUPPORTED,         0x07, "SASL mechanism not supported");
    (AUTHENTICATION_FAILURE,               0x08, "Authentication failure");
    (SASL_ERROR,                           0x09, "SASL error");
    (SASL_CHALLENGE,                       0x0a, "SASL challenge");
    (SASL_AUTHENTICATION_FAILURE_LOOPBACK, 0x0b, "SASL authentication failure loopback");
    (VIRTUAL_HOST_ACCESS_FAILURE,          0x0c, "Virtual host access failure");
    (UNKNOWN_FRAME,                        0x0d, "Unknown frame");
    (FRAME_TOO_LARGE,                      0x0e, "Frame too large");
    (INTERNAL_ERROR,                       0x0f, "Internal error");
    (ACCESS_REFUSED,                       0x10, "Access refused");
    (PRECONDITION_FAILED,                  0x11, "Precondition failed");
    (PUBLISHER_DOES_NOT_EXIST,             0x12, "Publisher does not exist");
    (NO_OFFSET,                            0x13, "No offset");
    (SASL_CANNOT_CHANGE_MECHANISM,         0x14, "SASL cannot change mechanism");
    (SASL_CANNOT_CHANGE_USERNAME,          0x15, "SASL cannot change username");
}

impl ResponseCode {
    pub const fn is_ok(self) -> bool {
        self.0 == Self::OK.0
    }

    pub const fn into_result(self) -> Result<(), Self> {
        if self.is_ok() { Ok(()) } else { Err(self) }
    }
}

impl Encode for ResponseCode {
    fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u16(self.0);
    }
}

impl Decode for ResponseCode {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        reader.u16().map(Self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodeResponse(ResponseCode);

impl Decode for CodeResponse {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self(reader.decode()?))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Offset(pub u64);

impl Encode for Offset {
    fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u64(self.0);
    }
}

impl Decode for Offset {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        reader.decode().map(Self)
    }
}

pub type ChunkId = Offset;

pub trait Command {
    const KEY: u16;
    const VERSION: u16 = 1;
}

pub trait Status {
    fn code(&self) -> ResponseCode;

    fn into_result(self) -> Result<Self, ResponseCode>
    where
        Self: Sized,
    {
        if self.code().is_ok() {
            Ok(self)
        } else {
            Err(self.code())
        }
    }
}

impl Status for CodeResponse {
    fn code(&self) -> ResponseCode {
        self.0
    }
}

pub trait Request: Command {
    type Response;
}

pub trait Notification: Command {}

#[derive(Debug, Clone, Copy)]
pub struct ReferenceTooLongError(usize);

impl fmt::Display for ReferenceTooLongError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!(
            "reference too long, expected 256 bytes or less, got {}",
            self.0
        ))
    }
}

impl std::error::Error for ReferenceTooLongError {}

/// A string of max 256 bytes.
pub struct Reference(String);

impl Reference {
    pub fn new(value: impl Into<String>) -> Result<Self, ReferenceTooLongError> {
        let inner = value.into();
        if inner.len() > 256 {
            Err(ReferenceTooLongError(inner.len()))
        } else {
            Ok(Self(inner))
        }
    }
}

impl Encode for Reference {
    fn encode(&self, buf: &mut impl BufMut) {
        self.0.as_str().encode(buf)
    }
}

impl Decode for Reference {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Self::new(reader.str()?).map_err(|err| DecodeError::Custom(err.into()))
    }
}

// DeclarePublisher

pub struct DeclarePublisher<'a> {
    pub id: PublisherId,
    pub reference: Option<&'a Reference>,
    pub stream: &'a str,
}

impl Command for DeclarePublisher<'_> {
    const KEY: u16 = 0x0001;
}

impl Request for DeclarePublisher<'_> {
    type Response = CodeResponse;
}

impl Encode for DeclarePublisher<'_> {
    fn encode(&self, buf: &mut impl BufMut) {
        self.id.encode(buf);
        match self.reference {
            Some(r) => r.encode(buf),
            // Required according to spec, but can be empty to signal none.
            None => "".encode(buf),
        }
        self.stream.encode(buf);
    }
}

// Publish

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublisherId(pub u8);

impl Encode for PublisherId {
    fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u8(self.0);
    }
}

impl Decode for PublisherId {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        reader.u8().map(Self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishingId(pub u64);

impl Encode for PublishingId {
    fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u64(self.0);
    }
}

impl Decode for PublishingId {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        reader.decode().map(Self)
    }
}

pub struct PublishedMessage {
    pub id: PublishingId,
    pub message: Vec<u8>,
}

pub struct Publish {
    pub publisher_id: PublisherId,
    pub published_messages: Vec<PublishedMessage>,
}

impl Command for Publish {
    const KEY: u16 = 0x0002;
}

impl Notification for Publish {}

pub struct PublishedMessageV2 {
    pub id: PublishingId,
    // NOTE: null is actually accepted, but the protocol doc suggests using
    // version 1 if there is no filter value. Ideally we'd choose between
    // version 1 and 2 when checking a higher level message's type.
    pub filter_value: String,
    pub message: Vec<u8>,
}

pub struct PublishV2 {
    pub publisher_id: PublisherId,
    pub published_messages: Vec<PublishedMessageV2>,
}

impl Command for PublishV2 {
    const KEY: u16 = 0x002;
}

impl Notification for PublishV2 {}

// PublishConfirm

pub struct PublishConfirm {
    pub publisher_id: PublisherId,
    pub publishing_ids: Vec<PublishingId>,
}

impl Command for PublishConfirm {
    const KEY: u16 = 0x0003;
}

impl Notification for PublishConfirm {}

impl Decode for PublishConfirm {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            publisher_id: reader.decode()?,
            publishing_ids: reader.decode()?,
        })
    }
}

// PublishError

pub struct PublishingError {
    pub publishing_id: PublishingId,
    pub code: ResponseCode,
}

impl Decode for PublishingError {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            publishing_id: PublishingId::decode(reader)?,
            code: ResponseCode::decode(reader)?,
        })
    }
}

pub struct PublishError {
    pub publisher_id: PublisherId,
    pub errors: Vec<PublishingError>,
}

impl Command for PublishError {
    const KEY: u16 = 0x0004;
}

impl Notification for PublishError {}

impl Decode for PublishError {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            publisher_id: reader.decode()?,
            errors: reader.decode()?,
        })
    }
}

// QueryPublisherSequence

pub struct QueryPublisherSequence<'a> {
    pub reference: &'a Reference,
    pub stream: &'a str,
}

impl Command for QueryPublisherSequence<'_> {
    const KEY: u16 = 0x0005;
}

pub struct QueryPublisherResponse {
    pub code: ResponseCode,
    pub sequence: u64,
}

impl Status for QueryPublisherResponse {
    fn code(&self) -> ResponseCode {
        self.code
    }
}

impl Request for QueryPublisherSequence<'_> {
    type Response = QueryPublisherResponse;
}

impl Encode for QueryPublisherSequence<'_> {
    fn encode(&self, buf: &mut impl BufMut) {
        self.reference.encode(buf);
        self.stream.encode(buf);
    }
}

impl Decode for QueryPublisherResponse {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            code: reader.decode()?,
            sequence: reader.decode()?,
        })
    }
}

// DeletePublisher

pub struct DeletePublisher {
    pub publisher_id: PublisherId,
}

impl Command for DeletePublisher {
    const KEY: u16 = 0x0006;
}

impl Request for DeletePublisher {
    type Response = CodeResponse;
}

impl Encode for DeletePublisher {
    fn encode(&self, buf: &mut impl BufMut) {
        self.publisher_id.encode(buf);
    }
}

// Subscribe

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubscriptionId(pub u8);

impl Encode for SubscriptionId {
    fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u8(self.0);
    }
}

impl Decode for SubscriptionId {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        reader.u8().map(Self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetSpecification {
    First,
    Last,
    Next,
    Offset(u64),
    Timestamp(i64),
}

impl Encode for OffsetSpecification {
    fn encode(&self, buf: &mut impl BufMut) {
        match self {
            Self::First => buf.put_u16(1),
            Self::Last => buf.put_u16(2),
            Self::Next => buf.put_u16(3),
            Self::Offset(offset) => {
                buf.put_u16(4);
                buf.put_u64(*offset);
            }
            Self::Timestamp(ts) => {
                buf.put_u16(5);
                buf.put_i64(*ts)
            }
        }
    }
}

pub struct Subscribe<'a> {
    pub subscription_id: SubscriptionId,
    pub stream: &'a str,
    pub offset_specification: OffsetSpecification,
    pub credit: u16,
    // See supported properties in doc. Might make sense to have a stronger
    // type here.
    pub properties: &'a [(&'a str, &'a str)],
}

impl Command for Subscribe<'_> {
    const KEY: u16 = 0x0007;
}

impl Request for Subscribe<'_> {
    type Response = CodeResponse;
}

impl Encode for Subscribe<'_> {
    fn encode(&self, buf: &mut impl BufMut) {
        self.subscription_id.encode(buf);
        self.stream.encode(buf);
        self.offset_specification.encode(buf);
        buf.put_u16(self.credit);
        if !self.properties.is_empty() {
            buf.put_u32(self.properties.len() as u32);
            for (k, v) in self.properties {
                k.encode(buf);
                v.encode(buf);
            }
        }
    }
}

// Deliver

pub struct ChunkType(i8);

impl ChunkType {
    pub const USER: Self = Self(0);
    pub const TRACKING_DELTA: Self = Self(1);
    pub const TRACKING_SNAPSHOT: Self = Self(2);
}

pub struct Chunk {
    pub magic_version: i8,
    pub chunk_type: ChunkType,
    // Should be consumed into the vec for messages - it's the length.
    // pub num_entries: u16,
    pub num_records: u32,
    pub timestamp: i64,
    pub epoch: u64,
    pub chunk_first_offset: ChunkId,
    pub crc: i32,
    // This would need refinement in practice.
    pub messages: Vec<u8>,
}

pub struct Deliver {
    pub subscription_id: SubscriptionId,
    pub chunk: Chunk,
}

impl Command for Deliver {
    const KEY: u16 = 0x0008;
}

impl Notification for Deliver {}

pub struct DeliverV2 {
    pub subscription_id: SubscriptionId,
    pub committed_chunk_id: ChunkId,
    pub chunk: Chunk,
}

impl Command for DeliverV2 {
    const KEY: u16 = 0x0008;
    const VERSION: u16 = 2;
}

impl Notification for DeliverV2 {}

// Credit

pub struct Credit {
    pub subscription_id: SubscriptionId,
    /// The number of chunks that can be sent
    pub credit: u16,
}

impl Command for Credit {
    const KEY: u16 = 0x0009;
}

/// NB: the server sent a response only in case of problem, e.g. crediting an unknown subscription.
pub struct CreditResponse {
    pub code: ResponseCode,
    pub subscription_id: SubscriptionId,
}

impl Status for CreditResponse {
    fn code(&self) -> ResponseCode {
        self.code
    }
}

impl Request for Credit {
    type Response = CreditResponse;
}

impl Encode for Credit {
    fn encode(&self, buf: &mut impl BufMut) {
        self.subscription_id.encode(buf);
        buf.put_u16(self.credit);
    }
}

impl Decode for CreditResponse {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            code: reader.decode()?,
            subscription_id: reader.decode()?,
        })
    }
}

// StoreOffset

pub struct StoreOffset<'a> {
    pub reference: &'a Reference,
    pub stream: &'a str,
    pub offset: Offset,
}

impl Command for StoreOffset<'_> {
    const KEY: u16 = 0x000a;
}

impl Notification for StoreOffset<'_> {}

impl Encode for StoreOffset<'_> {
    fn encode(&self, buf: &mut impl BufMut) {
        self.reference.encode(buf);
        self.stream.encode(buf);
        self.offset.encode(buf);
    }
}

// QueryOffset

pub struct QueryOffset<'a> {
    pub reference: Reference,
    pub stream: &'a str,
}

impl Command for QueryOffset<'_> {
    const KEY: u16 = 0x000b;
}

pub struct QueryOffsetResponse {
    pub code: ResponseCode,
    pub offset: Offset,
}

impl Status for QueryOffsetResponse {
    fn code(&self) -> ResponseCode {
        self.code
    }
}

impl Request for QueryOffset<'_> {
    type Response = QueryOffsetResponse;
}

impl Encode for QueryOffset<'_> {
    fn encode(&self, buf: &mut impl BufMut) {
        self.reference.encode(buf);
        self.stream.encode(buf);
    }
}

impl Decode for QueryOffsetResponse {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let code = reader.decode()?;
        // Server always responds with the encoding for an offset spec, but
        // only returns offsets.
        let offset_spec_type = reader.u16()?;
        if offset_spec_type != 4 {
            return Err(DecodeError::Malformed);
        }
        Ok(Self {
            code,
            offset: reader.decode()?,
        })
    }
}

// Unsubscribe

pub struct Unsubscribe {
    pub subscription_id: SubscriptionId,
}

impl Command for Unsubscribe {
    const KEY: u16 = 0x000c;
}

impl Request for Unsubscribe {
    type Response = CodeResponse;
}

impl Encode for Unsubscribe {
    fn encode(&self, buf: &mut impl BufMut) {
        self.subscription_id.encode(buf);
    }
}

// Create

pub struct Create<'a> {
    pub stream: &'a str,
    pub arguments: &'a [(&'a str, &'a str)],
}

impl Command for Create<'_> {
    const KEY: u16 = 0x000d;
}

impl Request for Create<'_> {
    type Response = CodeResponse;
}

impl Encode for Create<'_> {
    fn encode(&self, buf: &mut impl BufMut) {
        self.stream.encode(buf);
        self.arguments.encode(buf);
    }
}

// Delete

pub struct Delete<'a> {
    pub stream: &'a str,
}

impl Command for Delete<'_> {
    const KEY: u16 = 0x000e;
}

impl Request for Delete<'_> {
    type Response = CodeResponse;
}

impl Encode for Delete<'_> {
    fn encode(&self, buf: &mut impl BufMut) {
        self.stream.encode(buf);
    }
}

// Metadata

pub struct MetadataQuery<'a> {
    pub streams: &'a [&'a str],
}

impl Command for MetadataQuery<'_> {
    const KEY: u16 = 0x000f;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NonMaxU16(NonZeroU16);

impl NonMaxU16 {
    pub fn new(n: u16) -> Option<Self> {
        NonZeroU16::new(n ^ u16::MAX).map(Self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrokerRef(NonMaxU16);

impl Decode for BrokerRef {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        NonMaxU16::new(reader.u16()?)
            .map(BrokerRef)
            .ok_or(DecodeError::Malformed)
    }
}

impl Decode for Option<BrokerRef> {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(NonMaxU16::new(reader.u16()?).map(BrokerRef))
    }
}

pub struct Broker {
    pub reference: BrokerRef,
    pub host: String,
    pub port: u32,
}

impl Decode for Broker {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            reference: reader.decode()?,
            host: reader.decode()?,
            port: reader.u32()?,
        })
    }
}

pub struct StreamMetadata {
    pub name: String,
    pub code: ResponseCode,
    pub leader: Option<BrokerRef>,
    pub replicas: Vec<BrokerRef>,
}

impl Decode for StreamMetadata {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            name: reader.decode()?,
            code: reader.decode()?,
            leader: reader.decode()?,
            replicas: reader.decode()?,
        })
    }
}

pub struct MetadataResponse {
    pub brokers: Vec<Broker>,
    pub streams: Vec<StreamMetadata>,
}

impl Request for MetadataQuery<'_> {
    type Response = MetadataResponse;
}

impl Encode for MetadataQuery<'_> {
    fn encode(&self, buf: &mut impl BufMut) {
        self.streams.encode(buf);
    }
}

impl Decode for MetadataResponse {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            brokers: reader.decode()?,
            streams: reader.decode()?,
        })
    }
}

// MetadataUpdate

pub struct MetadataUpdate {
    pub code: ResponseCode,
    pub stream: String,
}

impl Command for MetadataUpdate {
    const KEY: u16 = 0x0010;
}

impl Notification for MetadataUpdate {}

impl Decode for MetadataUpdate {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            code: reader.decode()?,
            stream: reader.decode()?,
        })
    }
}

// PeerProperties

pub struct PeerProperties<'a> {
    pub properties: &'a [(&'a str, &'a str)],
}

impl Command for PeerProperties<'_> {
    const KEY: u16 = 0x0011;
}

pub struct PeerPropertiesResponse {
    pub code: ResponseCode,
    pub properties: Vec<(String, String)>,
}

impl Status for PeerPropertiesResponse {
    fn code(&self) -> ResponseCode {
        self.code
    }
}

impl Request for PeerProperties<'_> {
    type Response = PeerPropertiesResponse;
}

impl Encode for PeerProperties<'_> {
    fn encode(&self, buf: &mut impl BufMut) {
        self.properties.encode(buf);
    }
}

impl Decode for PeerPropertiesResponse {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            code: reader.decode()?,
            properties: reader.decode()?,
        })
    }
}

// SaslHandshake

pub struct SaslHandshake;

impl Command for SaslHandshake {
    const KEY: u16 = 0x0012;
}

pub struct SaslHandshakeResponse {
    pub code: ResponseCode,
    pub mechanisms: Vec<String>,
}

impl Status for SaslHandshakeResponse {
    fn code(&self) -> ResponseCode {
        self.code
    }
}

impl Request for SaslHandshake {
    type Response = SaslHandshakeResponse;
}

impl Encode for SaslHandshake {
    // Nothing to do.
    fn encode(&self, _buf: &mut impl BufMut) {}
}

impl Decode for SaslHandshakeResponse {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            code: reader.decode()?,
            mechanisms: reader.decode()?,
        })
    }
}

// SaslAuthenticate

pub struct Mechanism<'a>(pub &'a str);

impl Mechanism<'_> {
    pub const PLAIN: Self = Self("PLAIN");
    pub const EXTERNAL: Self = Self("EXTERNAL");
    pub const ANONYMOUS: Self = Self("ANONYMOUS");
}

pub struct SaslAuthenticate<'a> {
    pub mechanism: Mechanism<'a>,
    pub opaque_data: Option<&'a [u8]>,
}

impl Command for SaslAuthenticate<'_> {
    const KEY: u16 = 0x0013;
}

pub struct SaslAuthenticateResponse {
    pub code: ResponseCode,
    pub challenge: Option<Bytes>,
}

impl Status for SaslAuthenticateResponse {
    fn code(&self) -> ResponseCode {
        self.code
    }
}

impl Request for SaslAuthenticate<'_> {
    type Response = SaslAuthenticateResponse;
}

impl Encode for SaslAuthenticate<'_> {
    fn encode(&self, buf: &mut impl BufMut) {
        self.mechanism.0.encode(buf);
        match self.opaque_data.as_ref() {
            Some(data) => {
                buf.put_u32(data.len() as u32);
                buf.put_slice(data);
            }
            None => buf.put_i32(-1),
        }
    }
}

impl Decode for SaslAuthenticateResponse {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let code = reader.decode()?;
        let challenge = if reader.is_empty() {
            None
        } else {
            Some(reader.decode()?)
        };
        Ok(Self { code, challenge })
    }
}

// Tune

pub struct Tune {
    /// In bytes, 0 (None) means no limit
    pub frame_max: Option<NonZeroU32>,
    /// In seconds, 0 (None) means no heartbeat
    pub heartbeat: Option<NonZeroU32>,
}

impl Command for Tune {
    const KEY: u16 = 0x0014;
}

impl Request for Tune {
    type Response = Tune;
}

impl Encode for Tune {
    fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u32(self.frame_max.map(NonZeroU32::get).unwrap_or(0));
        buf.put_u32(self.heartbeat.map(NonZeroU32::get).unwrap_or(0));
    }
}

impl Decode for Tune {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            frame_max: NonZeroU32::new(reader.u32()?),
            heartbeat: NonZeroU32::new(reader.u32()?),
        })
    }
}

// Open

pub struct Open<'a> {
    pub virtual_host: &'a str,
}

impl Command for Open<'_> {
    const KEY: u16 = 0x0015;
}

pub struct OpenResponse {
    pub code: ResponseCode,
    pub connection_properties: Vec<(String, String)>,
}

impl Status for OpenResponse {
    fn code(&self) -> ResponseCode {
        self.code
    }
}

impl Request for Open<'_> {
    type Response = OpenResponse;
}

impl Encode for Open<'_> {
    fn encode(&self, buf: &mut impl BufMut) {
        self.virtual_host.encode(buf)
    }
}

impl Decode for OpenResponse {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let code = reader.decode()?;
        // NOTE: this is an important check. The server will not include the properties map if
        // the result is not OK.
        let connection_properties = if reader.is_empty() {
            Vec::new()
        } else {
            reader.decode()?
        };
        Ok(Self {
            code,
            connection_properties,
        })
    }
}

// Close

pub struct Close<'a> {
    pub code: ResponseCode,
    // Owned when sent from the server.
    pub reason: Cow<'a, str>,
}

impl Command for Close<'_> {
    const KEY: u16 = 0x0016;
}

impl Request for Close<'_> {
    type Response = CodeResponse;
}

impl Encode for Close<'_> {
    fn encode(&self, buf: &mut impl BufMut) {
        self.code.encode(buf);
        self.reason.as_ref().encode(buf);
    }
}

impl Decode for Close<'static> {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            code: reader.decode()?,
            reason: Cow::Owned(reader.decode()?),
        })
    }
}

// Heartbeat

pub struct Heartbeat;

impl Command for Heartbeat {
    const KEY: u16 = 0x0017;
}

impl Notification for Heartbeat {}

impl Encode for Heartbeat {
    // Nothing to do.
    fn encode(&self, _buf: &mut impl BufMut) {}
}

impl Decode for Heartbeat {
    fn decode(_reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self)
    }
}

// Route

pub struct Route<'a> {
    pub routing_key: &'a str,
    pub super_stream: &'a str,
}

impl Command for Route<'_> {
    const KEY: u16 = 0x0018;
}

pub struct RouteResponse {
    pub code: ResponseCode,
    pub streams: Vec<String>,
}

impl Status for RouteResponse {
    fn code(&self) -> ResponseCode {
        self.code
    }
}

impl Request for Route<'_> {
    type Response = RouteResponse;
}

impl Encode for Route<'_> {
    fn encode(&self, buf: &mut impl BufMut) {
        self.routing_key.encode(buf);
        self.super_stream.encode(buf);
    }
}

impl Decode for RouteResponse {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            code: reader.decode()?,
            streams: reader.decode()?,
        })
    }
}

// Partitions

pub struct Partitions<'a> {
    pub super_stream: &'a str,
}

impl Command for Partitions<'_> {
    const KEY: u16 = 0x0019;
}

pub struct PartitionsResponse {
    pub code: ResponseCode,
    pub streams: Vec<String>,
}

impl Status for PartitionsResponse {
    fn code(&self) -> ResponseCode {
        self.code
    }
}

impl Request for Partitions<'_> {
    type Response = PartitionsResponse;
}

impl Encode for Partitions<'_> {
    fn encode(&self, buf: &mut impl BufMut) {
        self.super_stream.encode(buf);
    }
}

impl Decode for PartitionsResponse {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            code: reader.decode()?,
            streams: reader.decode()?,
        })
    }
}

// ConsumerUpdate

pub struct ConsumerUpdate {
    pub subscription_id: SubscriptionId,
    // repr(u8), but is only 0 or 1
    pub active: bool,
}

impl Command for ConsumerUpdate {
    const KEY: u16 = 0x001a;
}

pub struct ConsumerUpdateResponse {
    pub code: ResponseCode,
    pub offset_specification: Option<OffsetSpecification>,
}

// ExchangeCommandVersions

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommandVersions {
    pub key: u16,
    pub min_version: u16,
    pub max_version: u16,
}

impl Encode for CommandVersions {
    fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u16(self.key);
        buf.put_u16(self.min_version);
        buf.put_u16(self.max_version);
    }
}

impl Decode for CommandVersions {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            key: reader.u16()?,
            min_version: reader.u16()?,
            max_version: reader.u16()?,
        })
    }
}

pub struct CommandVersionsExchange<'a> {
    pub commands: &'a [CommandVersions],
}

impl Command for CommandVersionsExchange<'_> {
    const KEY: u16 = 0x001b;
}

pub struct CommandVersionsExchangeResponse {
    pub code: ResponseCode,
    pub commands: Vec<CommandVersions>,
}

impl Status for CommandVersionsExchangeResponse {
    fn code(&self) -> ResponseCode {
        self.code
    }
}

impl Request for CommandVersionsExchange<'_> {
    type Response = CommandVersionsExchangeResponse;
}

impl Encode for CommandVersionsExchange<'_> {
    fn encode(&self, buf: &mut impl BufMut) {
        self.commands.encode(buf);
    }
}

impl Decode for CommandVersionsExchangeResponse {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            code: reader.decode()?,
            commands: reader.decode()?,
        })
    }
}

// StreamStats

pub struct StreamStats<'a> {
    pub stream: &'a str,
}

impl Command for StreamStats<'_> {
    const KEY: u16 = 0x001c;
}

pub struct StreamStatsResponse {
    pub code: ResponseCode,
    pub stats: Vec<(String, i64)>,
}

impl Status for StreamStatsResponse {
    fn code(&self) -> ResponseCode {
        self.code
    }
}

impl Request for StreamStats<'_> {
    type Response = StreamStatsResponse;
}

impl Encode for StreamStats<'_> {
    fn encode(&self, buf: &mut impl BufMut) {
        self.stream.encode(buf);
    }
}

impl Decode for StreamStatsResponse {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            code: reader.decode()?,
            stats: reader.decode()?,
        })
    }
}

// CreateSuperStream

pub struct CreateSuperStream<'a> {
    pub name: &'a str,
    pub partitions: &'a [&'a str],
    pub binding_keys: &'a [&'a str],
    pub arguments: &'a [(&'a str, &'a str)],
}

impl Command for CreateSuperStream<'_> {
    const KEY: u16 = 0x001d;
}

impl Request for CreateSuperStream<'_> {
    type Response = CodeResponse;
}

impl Encode for CreateSuperStream<'_> {
    fn encode(&self, buf: &mut impl BufMut) {
        self.name.encode(buf);
        self.partitions.encode(buf);
        self.binding_keys.encode(buf);
        self.arguments.encode(buf);
    }
}

// DeleteSuperStream

pub struct DeleteSuperStream<'a> {
    pub name: &'a str,
}

impl Command for DeleteSuperStream<'_> {
    const KEY: u16 = 0x001e;
}

impl Request for DeleteSuperStream<'_> {
    type Response = CodeResponse;
}

impl Encode for DeleteSuperStream<'_> {
    fn encode(&self, buf: &mut impl BufMut) {
        self.name.encode(buf);
    }
}

// ResolveOffsetSpec

pub struct ResolveOffsetSpec<'a> {
    pub stream: &'a str,
    pub offset_specification: OffsetSpecification,
    pub properties: &'a [(&'a str, &'a str)],
}

impl Command for ResolveOffsetSpec<'_> {
    const KEY: u16 = 0x001f;
}

pub struct ResolveOffsetSpecResponse {
    pub code: ResponseCode,
    // Returned as an offset spec but is always an offset
    pub offset: Offset,
}

impl Status for ResolveOffsetSpecResponse {
    fn code(&self) -> ResponseCode {
        self.code
    }
}

impl Request for ResolveOffsetSpec<'_> {
    type Response = ResolveOffsetSpecResponse;
}

impl Encode for ResolveOffsetSpec<'_> {
    fn encode(&self, buf: &mut impl BufMut) {
        self.stream.encode(buf);
        self.offset_specification.encode(buf);
        self.properties.encode(buf);
    }
}

impl Decode for ResolveOffsetSpecResponse {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            code: reader.decode()?,
            offset: reader.decode()?,
        })
    }
}
