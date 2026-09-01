#![allow(dead_code)]

use std::fmt;

use tokio::sync::{mpsc, oneshot};

use crate::{
    Connection, Error, OffsetSpec, Reference, ResponseCode, SubscriptionId,
    commands::{self, Chunk, CommandKey},
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ChunkSelector {
    /// Only CHNK_USER chunks, no trailer.
    #[default]
    UserData,
    /// Every chunk type, including osiris tracking chunks, with trailers.
    All,
}

impl ChunkSelector {
    fn as_str(self) -> &'static str {
        match self {
            Self::UserData => "user_data",
            Self::All => "all",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeOptions {
    pub offset: OffsetSpec,
    pub credit: u16,
    /// Some(name) enables single active consumer for that group.
    pub single_active_consumer: Option<Reference>,
    pub super_stream: Option<String>,
    pub filters: Vec<String>,
    pub match_unfiltered: bool,
    pub chunk_selector: ChunkSelector,
    pub extra: Vec<(String, String)>,
}

impl Default for SubscribeOptions {
    fn default() -> Self {
        Self {
            offset: OffsetSpec::First,
            credit: 50,
            single_active_consumer: None,
            super_stream: None,
            filters: Vec::new(),
            match_unfiltered: false,
            chunk_selector: ChunkSelector::default(),
            extra: Vec::new(),
        }
    }
}

impl SubscribeOptions {
    pub(crate) fn to_properties(&self) -> Vec<(String, String)> {
        let mut properties = Vec::new();

        // The server rejects single-active-consumer without a name
        // (rabbit_stream_reader.erl:2185-2198), so they go together.
        if let Some(name) = &self.single_active_consumer {
            properties.push(("single-active-consumer".into(), "true".into()));
            properties.push(("name".into(), name.as_str().into()));
        }

        if let Some(super_stream) = &self.super_stream {
            properties.push(("super-stream".into(), super_stream.clone()));
        }

        for (i, filter) in self.filters.iter().enumerate() {
            properties.push((format!("filter.{i}"), filter.clone()));
        }

        // Only consulted when at least one filter is present.
        if !self.filters.is_empty() && self.match_unfiltered {
            properties.push(("match-unfiltered".into(), "true".into()));
        }

        if self.chunk_selector != ChunkSelector::UserData {
            properties.push(("chunk_selector".into(), self.chunk_selector.as_str().into()));
        }

        properties.extend(self.extra.iter().cloned());
        properties
    }
}

#[derive(Debug)]
pub enum SubscriptionEvent {
    Chunk(Chunk),
    /// CreditResponse: error-only, keyed by subscription, no correlation id.
    CreditRefused(ResponseCode),
    /// MetadataUpdate: the server has already torn this subscription down.
    Unavailable(ResponseCode),
    /// Single active consumer handover. The reply is the resume point.
    ConsumerUpdate {
        active: bool,
        reply: oneshot::Sender<Option<OffsetSpec>>,
    },
}

pub struct Subscription {
    connection: Connection,
    id: SubscriptionId,
    stream: String,
    events: mpsc::Receiver<SubscriptionEvent>,
    credit_target: u16,
    outstanding: u16,
    on_update: Option<Box<dyn Fn(bool) -> Option<OffsetSpec> + Send>>,
    closed: bool,
}

impl fmt::Debug for Subscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscription").finish()
    }
}

impl Subscription {
    pub(crate) fn new(
        connection: Connection,
        id: SubscriptionId,
        stream: String,
        events: mpsc::Receiver<SubscriptionEvent>,
        credit_target: u16,
    ) -> Self {
        Self {
            connection,
            id,
            stream,
            events,
            credit_target,
            outstanding: 0,
            // TODO: SAC update handler.
            on_update: None,
            closed: false,
        }
    }

    pub async fn close(&mut self) -> Result<(), Error> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.connection.unsubscribe(self.id).await.map(|_| ())
    }

    pub async fn next(&mut self) -> Option<Result<Chunk, Error>> {
        loop {
            match self.events.recv().await? {
                SubscriptionEvent::Chunk(chunk) => {
                    self.outstanding = self.outstanding.saturating_sub(1);
                    self.replenish().await;
                    return Some(Ok(chunk));
                }
                SubscriptionEvent::CreditRefused(code) => {
                    return Some(Err(Error::Refused {
                        command: CommandKey::CREDIT,
                        code,
                    }));
                }
                // This comes from MetadataUpdate. The server already closed
                // the subscription on its end.
                SubscriptionEvent::Unavailable(code) => {
                    self.closed = true;
                    return Some(Err(Error::Refused {
                        command: CommandKey::METADATA_UPDATE,
                        code,
                    }));
                }
                SubscriptionEvent::ConsumerUpdate { active, reply } => {
                    let offset = self.on_update.as_ref().and_then(|handler| handler(active));
                    let _ = reply.send(offset);
                    continue;
                }
            }
        }
    }

    async fn replenish(&mut self) {
        if self.outstanding * 2 <= self.credit_target {
            let top_up = self.credit_target - self.outstanding;
            if self
                .connection
                .notify(commands::Credit {
                    subscription_id: self.id,
                    credit: top_up as i16,
                })
                .await
                .is_ok()
            {
                self.outstanding += top_up;
            }
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let (connection, id) = (self.connection.clone(), self.id);
            handle.spawn(async move {
                let _ = connection.unsubscribe(id).await;
            });
        }
    }
}
