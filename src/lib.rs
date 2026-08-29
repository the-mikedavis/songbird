pub(crate) mod codec;
pub(crate) mod commands;
pub(crate) mod connection;

use std::{borrow::Borrow, fmt, ops::Deref, str::FromStr};

pub use commands::ResponseCode;
pub use connection::{Connection, Error, Publisher};

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

pub enum PublishOutcome {
    Confirmed(Vec<u64>),
    Failed(Vec<PublishFailure>),
}

pub struct PublishFailure {
    pub publishing_id: u64,
    pub code: ResponseCode,
}
