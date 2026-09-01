#![allow(dead_code)]

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use bytes::Bytes;
use tokio::sync::{Mutex, Notify, mpsc};

use crate::{Connection, Error, PublishOutcome, PublisherId, PublishingId, Reference, commands};

#[derive(Default, Debug)]
pub(crate) struct PublishTracker {
    outstanding: AtomicU64,
    drained: Notify,
}

impl PublishTracker {
    fn sent(&self, n: u64) {
        self.outstanding.fetch_add(n, Ordering::AcqRel);
    }

    pub(crate) fn resolved(&self, n: u64) {
        if self.outstanding.fetch_sub(n, Ordering::AcqRel) == n {
            self.drained.notify_one();
        }
    }

    pub(crate) fn abandon(&self) {
        self.outstanding.store(0, Ordering::Release);
        self.drained.notify_waiters();
    }
}

#[derive(Debug)]
pub struct Confirms {
    outcomes: mpsc::UnboundedReceiver<PublishOutcome>,
}

impl Confirms {
    pub(crate) fn new(outcomes: mpsc::UnboundedReceiver<PublishOutcome>) -> Self {
        Self { outcomes }
    }

    pub async fn recv(&mut self) -> Option<PublishOutcome> {
        self.outcomes.recv().await
    }

    pub fn try_recv(&mut self) -> Option<PublishOutcome> {
        self.outcomes.try_recv().ok()
    }
}

#[derive(Debug)]
pub struct Publisher {
    connection: Connection,
    id: PublisherId,
    stream: String,
    reference: Option<Reference>,
    tracker: Arc<PublishTracker>,
    next_publishing_id: Mutex<PublishingId>,
    closed: AtomicBool,
}

impl Publisher {
    pub(crate) fn new(
        connection: Connection,
        id: PublisherId,
        stream: String,
        reference: Option<Reference>,
        tracker: Arc<PublishTracker>,
        next_publishing_id: PublishingId,
    ) -> Self {
        Self {
            connection,
            id,
            stream,
            reference,
            tracker,
            next_publishing_id: Mutex::new(next_publishing_id),
            closed: AtomicBool::new(false),
        }
    }

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
        Ok((first..first + n).collect())
    }

    pub async fn drain_outstanding(&self) {
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
