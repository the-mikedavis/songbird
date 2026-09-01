pub(crate) mod codec;
pub(crate) mod commands;
pub(crate) mod connection;
pub(crate) mod consumer;
pub(crate) mod publisher;
pub(crate) mod subscription;

use std::{borrow::Borrow, fmt, ops::Deref, str::FromStr};

pub use commands::{PublishingError, ResponseCode};
pub use connection::{Connection, Error};
pub use publisher::{Confirms, Publisher};
pub use subscription::{ChunkSelector, SubscribeOptions, Subscription, SubscriptionEvent};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Offset(u64);

impl Offset {
    pub fn new(n: u64) -> Self {
        Self(n)
    }

    pub fn get(&self) -> u64 {
        self.0
    }
}

pub type ChunkId = Offset;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetSpec {
    First,
    Last,
    Next,
    Offset(Offset),
    Timestamp(i64),
}

#[derive(Debug)]
#[non_exhaustive]
pub enum ValidationError {
    TooLong {
        field: &'static str,
        len: usize,
        max: usize,
    },
    Empty {
        field: &'static str,
    },
    ReservedPrefix,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLong { field, len, max } => {
                f.write_fmt(format_args!("{field} is {len} bytes, the maximum is {max}"))
            }
            Self::Empty { field } => f.write_fmt(format_args!("{field} must not be empty")),
            Self::ReservedPrefix => f.write_str("stream name cannot start with \"amq.\""),
        }
    }
}

impl std::error::Error for ValidationError {}

/// A string of max 255 bytes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Reference(String);

impl Reference {
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        if value.len() > 255 {
            return Err(ValidationError::TooLong {
                field: "reference",
                len: value.len(),
                max: 255,
            });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for Reference {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
impl Borrow<str> for Reference {
    fn borrow(&self) -> &str {
        &self.0
    }
}
impl fmt::Display for Reference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl TryFrom<String> for Reference {
    type Error = ValidationError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl FromStr for Reference {
    type Err = ValidationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl Deref for Reference {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

pub type PublishingId = u64;

// TODO: move to publisher module.
pub enum PublishOutcome {
    Confirmed(Vec<PublishingId>),
    Failed(Vec<PublishingError>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PublisherId(u8);

impl From<u8> for PublisherId {
    fn from(value: u8) -> Self {
        Self(value)
    }
}

impl From<PublisherId> for u8 {
    fn from(val: PublisherId) -> Self {
        val.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubscriptionId(u8);

impl From<u8> for SubscriptionId {
    fn from(value: u8) -> Self {
        Self(value)
    }
}

impl From<SubscriptionId> for u8 {
    fn from(val: SubscriptionId) -> Self {
        val.0
    }
}
