#![allow(dead_code)]

use std::{borrow::Cow, fmt, num::NonZeroU16};

use bytes::{BufMut, Bytes};

use crate::{
    ChunkId, Offset, OffsetSpec, PublisherId, PublishingId, Reference, SubscriptionId,
    codec::{Decode, DecodeError, Encode, Reader},
};

macro_rules! wire_code {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident($repr:ty);
        $( ($konst:ident, $value:expr); )+
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        $vis struct $name($repr);

        impl $name {
            $( pub const $konst: Self = Self($value); )+

            pub const fn get(self) -> $repr { self.0 }

            pub fn raw_name(self) -> Option<&'static str> {
                match self {
                    $( Self::$konst => Some(stringify!($konst)), )+
                    _ => None,
                }
            }
        }

        impl From<$repr> for $name { fn from(v: $repr) -> Self { Self(v) } }
        impl From<$name> for $repr { fn from(v: $name) -> Self { v.0 } }
    };
}

wire_code! {
    pub struct ResponseCode(u16);

    (OK,                                   0x01);
    (STREAM_DOES_NOT_EXIST,                0x02);
    (SUBSCRIPTION_ID_EXISTS,               0x03);
    (SUBSCRIPTION_ID_DOES_NOT_EXIST,       0x04);
    (STREAM_ALREADY_EXISTS,                0x05);
    (STREAM_NOT_AVAILABLE,                 0x06);
    (SASL_MECHANISM_NOT_SUPPORTED,         0x07);
    (AUTHENTICATION_FAILURE,               0x08);
    (SASL_ERROR,                           0x09);
    (SASL_CHALLENGE,                       0x0a);
    (SASL_AUTHENTICATION_FAILURE_LOOPBACK, 0x0b);
    (VIRTUAL_HOST_ACCESS_FAILURE,          0x0c);
    (UNKNOWN_FRAME,                        0x0d);
    (FRAME_TOO_LARGE,                      0x0e);
    (INTERNAL_ERROR,                       0x0f);
    (ACCESS_REFUSED,                       0x10);
    (PRECONDITION_FAILED,                  0x11);
    (PUBLISHER_DOES_NOT_EXIST,             0x12);
    (NO_OFFSET,                            0x13);
    (SASL_CANNOT_CHANGE_MECHANISM,         0x14);
    (SASL_CANNOT_CHANGE_USERNAME,          0x15);
}

impl ResponseCode {
    pub const fn is_ok(self) -> bool {
        self.0 == Self::OK.0
    }
}

impl fmt::Debug for ResponseCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.raw_name() {
            Some(name) => f.write_str(name),
            None => write!(f, "ResponseCode({:#x})", self.0),
        }
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

wire_code! {
    pub struct CommandKey(u16);
    (DECLARE_PUBLISHER,         0x01);
    (PUBLISH,                   0x02);
    (PUBLISH_CONFIRM,           0x03);
    (PUBLISH_ERROR,             0x04);
    (QUERY_PUBLISHER_SEQUENCE,  0x05);
    (DELETE_PUBLISHER,          0x06);
    (SUBSCRIBE,                 0x07);
    (DELIVER,                   0x08);
    (CREDIT,                    0x09);
    (STORE_OFFSET,              0x0a);
    (QUERY_OFFSET,              0x0b);
    (UNSUBSCRIBE,               0x0c);
    (CREATE,                    0x0d);
    (DELETE,                    0x0e);
    (METADATA,                  0x0f);
    (METADATA_UPDATE,           0x10);
    (PEER_PROPERTIES,           0x11);
    (SASL_HANDSHAKE,            0x12);
    (SASL_AUTHENTICATE,         0x13);
    (TUNE,                      0x14);
    (OPEN,                      0x15);
    (CLOSE,                     0x16);
    (HEARTBEAT,                 0x17);
    (ROUTE,                     0x18);
    (PARTITIONS,                0x19);
    (CONSUMER_UPDATE,           0x1a);
    (EXCHANGE_COMMAND_VERSIONS, 0x1b);
    (STREAM_STATS,              0x1c);
    (CREATE_SUPER_STREAM,       0x1d);
    (DELETE_SUPER_STREAM,       0x1e);
    (RESOLVE_OFFSET_SPEC,       0x1f);
}

impl CommandKey {
    const RESPONSE_BIT: u16 = 0x8000;

    pub const fn command_id(self) -> Self {
        Self(self.0 & !Self::RESPONSE_BIT)
    }
    pub const fn is_response(self) -> bool {
        self.0 & Self::RESPONSE_BIT != 0
    }
    pub const fn to_response(self) -> Self {
        Self(self.0 | Self::RESPONSE_BIT)
    }

    pub fn name(self) -> Option<&'static str> {
        self.command_id().raw_name()
    }
}

impl fmt::Debug for CommandKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.name() {
            Some(name) if self.is_response() => write!(f, "{name}Response"),
            Some(name) => f.write_str(name),
            None => write!(f, "CommandKey({:#06x})", self.0),
        }
    }
}

impl Encode for CommandKey {
    fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u16(self.0);
    }
}

impl Decode for CommandKey {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self(reader.u16()?))
    }
}

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

pub trait Command {
    const KEY: CommandKey;
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

// DeclarePublisher

pub struct DeclarePublisher<'a> {
    pub id: PublisherId,
    pub reference: &'a str,
    pub stream: &'a str,
}

impl Command for DeclarePublisher<'_> {
    const KEY: CommandKey = CommandKey::DECLARE_PUBLISHER;
}

impl Request for DeclarePublisher<'_> {
    type Response = CodeResponse;
}

impl Encode for DeclarePublisher<'_> {
    fn encode(&self, buf: &mut impl BufMut) {
        self.id.encode(buf);
        self.reference.encode(buf);
        self.stream.encode(buf);
    }
}

// Publish

#[derive(Debug)]
pub struct PublishedMessage {
    pub id: PublishingId,
    pub body: Bytes,
}

impl Encode for PublishedMessage {
    fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u64(self.id);
        self.body.encode(buf);
    }
}

#[derive(Debug)]
pub struct Publish<'a> {
    pub publisher: PublisherId,
    pub messages: &'a [PublishedMessage],
}

impl Command for Publish<'_> {
    const KEY: CommandKey = CommandKey::PUBLISH;
}

impl Notification for Publish<'_> {}

impl Encode for Publish<'_> {
    fn encode(&self, buf: &mut impl BufMut) {
        self.publisher.encode(buf);
        self.messages.encode(buf);
    }
}

pub struct FilteredMessage {
    pub id: PublishingId,
    pub filter_value: Option<String>,
    pub message: Bytes,
}

impl Encode for FilteredMessage {
    fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u64(self.id);
        match &self.filter_value {
            Some(filter) => filter.as_str().encode(buf),
            None => buf.put_i16(-1),
        }
        self.message.encode(buf);
    }
}

pub struct PublishV2<'a> {
    pub publisher: PublisherId,
    pub messages: &'a [FilteredMessage],
}

impl Command for PublishV2<'_> {
    const KEY: CommandKey = CommandKey::PUBLISH;
}

impl Notification for PublishV2<'_> {}

impl Encode for PublishV2<'_> {
    fn encode(&self, buf: &mut impl BufMut) {
        self.publisher.encode(buf);
        self.messages.encode(buf);
    }
}

// PublishConfirm

pub struct PublishConfirm {
    pub publisher_id: PublisherId,
    pub publishing_ids: Vec<PublishingId>,
}

impl Command for PublishConfirm {
    const KEY: CommandKey = CommandKey::PUBLISH_CONFIRM;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishingError {
    pub publishing_id: PublishingId,
    pub code: ResponseCode,
}

impl Decode for PublishingError {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            publishing_id: reader.decode()?,
            code: reader.decode()?,
        })
    }
}

pub struct PublishError {
    pub publisher_id: PublisherId,
    pub errors: Vec<PublishingError>,
}

impl Command for PublishError {
    const KEY: CommandKey = CommandKey::PUBLISH_ERROR;
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
    const KEY: CommandKey = CommandKey::QUERY_PUBLISHER_SEQUENCE;
}

pub struct QueryPublisherResponse {
    pub code: ResponseCode,
    pub sequence: PublishingId,
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
    pub id: PublisherId,
}

impl Command for DeletePublisher {
    const KEY: CommandKey = CommandKey::DELETE_PUBLISHER;
}

impl Request for DeletePublisher {
    type Response = CodeResponse;
}

impl Encode for DeletePublisher {
    fn encode(&self, buf: &mut impl BufMut) {
        self.id.encode(buf);
    }
}

// Subscribe

impl Encode for OffsetSpec {
    fn encode(&self, buf: &mut impl BufMut) {
        match self {
            Self::First => buf.put_u16(1),
            Self::Last => buf.put_u16(2),
            Self::Next => buf.put_u16(3),
            Self::Offset(offset) => {
                buf.put_u16(4);
                offset.encode(buf);
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
    pub offset: OffsetSpec,
    pub credit: u16,
    // See supported properties in doc. Might make sense to have a stronger
    // type here.
    pub properties: &'a [(&'a str, &'a str)],
}

impl Command for Subscribe<'_> {
    const KEY: CommandKey = CommandKey::SUBSCRIBE;
}

impl Request for Subscribe<'_> {
    type Response = CodeResponse;
}

impl Encode for Subscribe<'_> {
    fn encode(&self, buf: &mut impl BufMut) {
        self.subscription_id.encode(buf);
        self.stream.encode(buf);
        self.offset.encode(buf);
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkType(u8);

impl ChunkType {
    pub const USER: Self = Self(0);
    pub const TRACKING_DELTA: Self = Self(1);
    pub const TRACKING_SNAPSHOT: Self = Self(2);

    pub const fn is_user(&self) -> bool {
        self.0 == Self::USER.0
    }
}

impl Decode for ChunkType {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self(reader.u8()?))
    }
}

#[derive(Debug, Clone)]
pub struct Chunk {
    /// First offset of the most recently committed chunk.
    /// Always absent when the chunk comes from deliver v1, always present with v2.
    pub committed_chunk_id: Option<ChunkId>,
    pub chunk_type: ChunkType,
    pub num_entries: u16,
    pub num_records: u32,
    pub timestamp: i64,
    pub epoch: u64,
    pub first_offset: Offset,
    pub crc: u32,
    pub(crate) data: Bytes, // the entries, zero-copy
}

impl Decode for Chunk {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let magic_version = reader.u8()?;
        if magic_version >> 4 != 5 {
            return Err(DecodeError::Malformed);
        }
        let chunk_type = reader.decode()?;
        let num_entries = reader.u16()?;
        let num_records = reader.u32()?;
        let timestamp = reader.decode()?;
        let epoch = reader.decode()?;
        let first_offset = reader.decode()?;
        let crc = reader.u32()?;
        let data_length = reader.u32()? as usize;
        let trailer_length = reader.u32()?; // present in the header, not on the wire
        let _bloom_size = reader.u8()?; // ditto: skipped server-side
        reader.skip(3)?; // reserved

        let data = reader.bytes_of(data_length)?;
        // The trailer is present if the subscription uses the 'all' chunk selector.
        if !reader.is_empty() {
            reader.skip(trailer_length as usize)?;
        }

        Ok(Chunk {
            committed_chunk_id: None, // set by DeliverV2
            chunk_type,
            num_entries,
            num_records,
            timestamp,
            epoch,
            first_offset,
            crc,
            data,
        })
    }
}

wire_code! {
    pub struct Compression(u8);
    (NONE, 0);
    (GZIP, 1);
    (SNAPPY, 2);
    (LZ4, 3);
    (ZSTD, 4);
}

impl Compression {
    pub(crate) fn new(n: u8) -> Self {
        Self(n)
    }
}

impl fmt::Debug for Compression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.raw_name() {
            Some(name) => f.write_str(name),
            None => write!(f, "Compression({:#x})", self.0),
        }
    }
}

pub struct Deliver {
    pub subscription_id: SubscriptionId,
    pub chunk: Chunk,
}

impl Command for Deliver {
    const KEY: CommandKey = CommandKey::DELIVER;
}

impl Notification for Deliver {}

impl Decode for Deliver {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            subscription_id: reader.decode()?,
            chunk: reader.decode()?,
        })
    }
}

pub struct DeliverV2 {
    pub subscription_id: SubscriptionId,
    pub committed_chunk_id: ChunkId,
    pub chunk: Chunk,
}

impl Command for DeliverV2 {
    const KEY: CommandKey = CommandKey::DELIVER;
    const VERSION: u16 = 2;
}

impl Notification for DeliverV2 {}

impl Decode for DeliverV2 {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            subscription_id: reader.decode()?,
            committed_chunk_id: reader.decode()?,
            chunk: reader.decode()?,
        })
    }
}

// Credit

pub struct Credit {
    pub subscription_id: SubscriptionId,
    /// The number of chunks that can be sent
    pub credit: i16,
}

impl Command for Credit {
    const KEY: CommandKey = CommandKey::CREDIT;
}

impl Notification for Credit {}

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
        buf.put_i16(self.credit);
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
    const KEY: CommandKey = CommandKey::STORE_OFFSET;
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
    pub reference: &'a Reference,
    pub stream: &'a str,
}

impl Command for QueryOffset<'_> {
    const KEY: CommandKey = CommandKey::QUERY_OFFSET;
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
    const KEY: CommandKey = CommandKey::UNSUBSCRIBE;
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
    const KEY: CommandKey = CommandKey::CREATE;
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
    const KEY: CommandKey = CommandKey::DELETE;
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

pub struct Metadata<'a> {
    pub streams: &'a [&'a str],
}

impl Command for Metadata<'_> {
    const KEY: CommandKey = CommandKey::METADATA;
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

impl Request for Metadata<'_> {
    type Response = MetadataResponse;
}

impl Encode for Metadata<'_> {
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
    const KEY: CommandKey = CommandKey::METADATA_UPDATE;
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
    const KEY: CommandKey = CommandKey::PEER_PROPERTIES;
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
    const KEY: CommandKey = CommandKey::SASL_HANDSHAKE;
}

pub struct SaslHandshakeResponse {
    pub code: ResponseCode,
    pub mechanisms: Vec<Mechanism>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mechanism(Cow<'static, str>);

impl Mechanism {
    pub const PLAIN: Self = Self(Cow::Borrowed("PLAIN"));
    pub const EXTERNAL: Self = Self(Cow::Borrowed("EXTERNAL"));
    pub const ANONYMOUS: Self = Self(Cow::Borrowed("ANONYMOUS"));

    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

impl From<String> for Mechanism {
    fn from(value: String) -> Self {
        Self(Cow::Owned(value))
    }
}

impl Encode for Mechanism {
    fn encode(&self, buf: &mut impl BufMut) {
        self.0.as_ref().encode(buf);
    }
}

impl Decode for Mechanism {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self(Cow::Owned(reader.str()?.to_owned())))
    }
}

pub struct SaslAuthenticate<'a> {
    pub mechanism: &'a Mechanism,
    pub sasl_data: Option<&'a [u8]>,
}

impl Command for SaslAuthenticate<'_> {
    const KEY: CommandKey = CommandKey::SASL_AUTHENTICATE;
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
        self.mechanism.encode(buf);
        match self.sasl_data.as_ref() {
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
    pub frame_max: u32,
    /// In seconds, 0 (None) means no heartbeat
    pub heartbeat: u32,
}

impl Command for Tune {
    const KEY: CommandKey = CommandKey::TUNE;
}

impl Request for Tune {
    type Response = Tune;
}

impl Encode for Tune {
    fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u32(self.frame_max);
        buf.put_u32(self.heartbeat);
    }
}

impl Decode for Tune {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            frame_max: reader.u32()?,
            heartbeat: reader.u32()?,
        })
    }
}

// Open

pub struct Open<'a> {
    pub virtual_host: &'a str,
}

impl Command for Open<'_> {
    const KEY: CommandKey = CommandKey::OPEN;
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
    const KEY: CommandKey = CommandKey::CLOSE;
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
    const KEY: CommandKey = CommandKey::HEARTBEAT;
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
    const KEY: CommandKey = CommandKey::ROUTE;
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
    const KEY: CommandKey = CommandKey::PARTITIONS;
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
    const KEY: CommandKey = CommandKey::CONSUMER_UPDATE;
}

pub struct ConsumerUpdateResponse {
    pub code: ResponseCode,
    pub offset_specification: Option<OffsetSpec>,
}

// ExchangeCommandVersions

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommandVersion {
    pub key: CommandKey,
    pub min_version: u16,
    pub max_version: u16,
}

impl Encode for CommandVersion {
    fn encode(&self, buf: &mut impl BufMut) {
        self.key.encode(buf);
        buf.put_u16(self.min_version);
        buf.put_u16(self.max_version);
    }
}

impl Decode for CommandVersion {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            key: reader.decode()?,
            min_version: reader.u16()?,
            max_version: reader.u16()?,
        })
    }
}

pub struct ExchangeCommandVersions<'a> {
    pub commands: &'a [CommandVersion],
}

impl Command for ExchangeCommandVersions<'_> {
    const KEY: CommandKey = CommandKey::EXCHANGE_COMMAND_VERSIONS;
}

pub struct ExchangeCommandVersionsResponse {
    pub code: ResponseCode,
    pub commands: Vec<CommandVersion>,
}

impl Status for ExchangeCommandVersionsResponse {
    fn code(&self) -> ResponseCode {
        self.code
    }
}

impl Request for ExchangeCommandVersions<'_> {
    type Response = ExchangeCommandVersionsResponse;
}

impl Encode for ExchangeCommandVersions<'_> {
    fn encode(&self, buf: &mut impl BufMut) {
        self.commands.encode(buf);
    }
}

impl Decode for ExchangeCommandVersionsResponse {
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
    const KEY: CommandKey = CommandKey::STREAM_STATS;
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
    const KEY: CommandKey = CommandKey::CREATE_SUPER_STREAM;
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
    const KEY: CommandKey = CommandKey::DELETE_SUPER_STREAM;
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
    pub offset_specification: OffsetSpec,
    pub properties: &'a [(&'a str, &'a str)],
}

impl Command for ResolveOffsetSpec<'_> {
    const KEY: CommandKey = CommandKey::RESOLVE_OFFSET_SPEC;
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
