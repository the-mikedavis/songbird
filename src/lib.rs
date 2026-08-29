pub(crate) mod codec;
pub(crate) mod commands;
pub(crate) mod connection;

pub use commands::ResponseCode;
pub use connection::{Connection, Error, Publisher};

pub enum PublishOutcome {
    Confirmed(Vec<u64>),
    Failed(Vec<PublishFailure>),
}

pub struct PublishFailure {
    pub publishing_id: u64,
    pub code: ResponseCode,
}
