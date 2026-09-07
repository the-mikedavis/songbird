pub(crate) mod codec;
pub(crate) mod commands;
pub(crate) mod connection;
pub(crate) mod consumer;
pub(crate) mod publisher;
pub(crate) mod subscription;

use std::{borrow::Borrow, fmt, ops::Deref, str::FromStr};

pub use commands::{PublishingError, ResponseCode};
pub use connection::{Connection, Error};
pub use publisher::{Confirms, PublishOutcome, Publisher, PublishingId};
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
    ControlCharacters,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLong { field, len, max } => {
                f.write_fmt(format_args!("{field} is {len} bytes, the maximum is {max}"))
            }
            Self::Empty { field } => f.write_fmt(format_args!("{field} must not be empty")),
            Self::ReservedPrefix => f.write_str("stream name cannot start with \"amq.\""),
            Self::ControlCharacters => f.write_str("stream name cannot contain control characters"),
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

#[derive(Default)]
pub struct StreamOptions {
    pub max_length_bytes: Option<u64>,
    pub max_age: Option<String>, // duration string, e.g. "7D"
    pub max_segment_size_bytes: Option<u64>,
    pub initial_cluster_size: Option<u32>, // must be > 0
    pub leader_locator: Option<String>,
    pub filter_size_bytes: Option<u8>, // must be 16..=255
    pub extra: Vec<(String, String)>,
}

impl StreamOptions {
    pub(crate) fn to_arguments(&self) -> Vec<(String, String)> {
        let mut args = Vec::new();
        if let Some(v) = self.max_length_bytes {
            args.push(("max-length-bytes".into(), v.to_string()));
        }
        if let Some(v) = &self.max_age {
            args.push(("max-age".into(), v.clone()));
        }
        if let Some(v) = self.max_segment_size_bytes {
            args.push(("stream-max-segment-size-bytes".into(), v.to_string()));
        }
        if let Some(v) = self.initial_cluster_size {
            args.push(("initial-cluster-size".into(), v.to_string()));
        }
        if let Some(v) = &self.leader_locator {
            args.push(("queue-leader-locator".into(), v.clone()));
        }
        if let Some(v) = self.filter_size_bytes {
            args.push(("stream-filter-size-bytes".into(), v.to_string()));
        }
        args.extend(self.extra.iter().cloned());
        args
    }
}
