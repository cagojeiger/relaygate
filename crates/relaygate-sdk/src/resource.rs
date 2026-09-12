use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{Error, ErrorCode, PeerObservation, config::ResourceLimits};

#[derive(Clone, Copy)]
pub(crate) enum ResourceLimitKind {
    ListenerPendingPipes,
    ListenerLivePipes,
    RelayLivePipes,
    PipeBufferedFrames,
    PipeBufferedBytes,
    RelayBufferedBytes,
}

impl ResourceLimitKind {
    const fn metric_name(self) -> &'static str {
        match self {
            Self::ListenerPendingPipes => "listener_pending_pipes",
            Self::ListenerLivePipes => "listener_live_pipes",
            Self::RelayLivePipes => "relay_live_pipes",
            Self::PipeBufferedFrames => "pipe_buffered_frames",
            Self::PipeBufferedBytes => "pipe_buffered_bytes",
            Self::RelayBufferedBytes => "relay_buffered_bytes",
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::ListenerPendingPipes => "Listener pending Pipe limit is exhausted",
            Self::ListenerLivePipes => "Listener live Pipe limit is exhausted",
            Self::RelayLivePipes => "Relay live Pipe limit is exhausted",
            Self::PipeBufferedFrames => "Pipe buffered frame limit is exhausted",
            Self::PipeBufferedBytes => "Pipe buffered byte limit is exhausted",
            Self::RelayBufferedBytes => "Relay buffered byte limit is exhausted",
        }
    }
}

pub(crate) fn resource_exhausted(kind: ResourceLimitKind, observation: PeerObservation) -> Error {
    metrics::counter!(
        "relaygate_sdk_resource_rejections_total",
        "resource" => kind.metric_name()
    )
    .increment(1);
    Error::new(ErrorCode::ResourceExhausted, observation, kind.message())
}

pub(crate) struct RelayResources {
    live_pipes: Arc<Semaphore>,
    buffered_bytes: Arc<ByteBudget>,
    observed_live_pipe_limit: usize,
    observed_buffered_byte_limit: usize,
}

impl RelayResources {
    pub(crate) fn new(limits: ResourceLimits) -> Self {
        metrics::gauge!(
            "relaygate_sdk_resource_used",
            "resource" => "live_pipes"
        )
        .increment(0.0);
        metrics::gauge!(
            "relaygate_sdk_resource_used",
            "resource" => "buffered_bytes"
        )
        .increment(0.0);
        metrics::gauge!(
            "relaygate_sdk_resource_limit",
            "resource" => "live_pipes"
        )
        .increment(limits.max_live_pipes_per_relay as f64);
        metrics::gauge!(
            "relaygate_sdk_resource_limit",
            "resource" => "buffered_bytes"
        )
        .increment(limits.max_buffered_bytes_per_relay as f64);
        Self {
            live_pipes: Arc::new(Semaphore::new(limits.max_live_pipes_per_relay)),
            buffered_bytes: Arc::new(ByteBudget::new(limits.max_buffered_bytes_per_relay, true)),
            observed_live_pipe_limit: limits.max_live_pipes_per_relay,
            observed_buffered_byte_limit: limits.max_buffered_bytes_per_relay,
        }
    }

    pub(crate) fn listener_slots(&self, maximum: usize) -> Arc<Semaphore> {
        Arc::new(Semaphore::new(maximum))
    }

    pub(crate) fn try_reserve_outgoing(&self) -> Result<LivePipeReservation, Error> {
        let relay = Arc::clone(&self.live_pipes)
            .try_acquire_owned()
            .map_err(|_| {
                resource_exhausted(
                    ResourceLimitKind::RelayLivePipes,
                    PeerObservation::NotObserved,
                )
            })?;
        Ok(LivePipeReservation::new(relay, None))
    }

    pub(crate) fn try_reserve_incoming(
        &self,
        listener: &Arc<Semaphore>,
    ) -> Result<LivePipeReservation, Error> {
        let relay = Arc::clone(&self.live_pipes)
            .try_acquire_owned()
            .map_err(|_| {
                resource_exhausted(ResourceLimitKind::RelayLivePipes, PeerObservation::Observed)
            })?;
        let listener = Arc::clone(listener).try_acquire_owned().map_err(|_| {
            resource_exhausted(
                ResourceLimitKind::ListenerLivePipes,
                PeerObservation::Observed,
            )
        })?;
        Ok(LivePipeReservation::new(relay, Some(listener)))
    }

    pub(crate) fn buffered_bytes(&self) -> Arc<ByteBudget> {
        Arc::clone(&self.buffered_bytes)
    }

    pub(crate) fn pipe_resources(
        &self,
        live: LivePipeReservation,
        max_buffered_bytes_per_pipe: usize,
    ) -> PipeResources {
        PipeResources {
            live: Mutex::new(Some(live)),
            pipe_buffered_bytes: Arc::new(ByteBudget::new(max_buffered_bytes_per_pipe, false)),
            relay_buffered_bytes: self.buffered_bytes(),
        }
    }
}

impl Drop for RelayResources {
    fn drop(&mut self) {
        metrics::gauge!(
            "relaygate_sdk_resource_limit",
            "resource" => "live_pipes"
        )
        .decrement(self.observed_live_pipe_limit as f64);
        metrics::gauge!(
            "relaygate_sdk_resource_limit",
            "resource" => "buffered_bytes"
        )
        .decrement(self.observed_buffered_byte_limit as f64);
    }
}

pub(crate) struct PipeResources {
    live: Mutex<Option<LivePipeReservation>>,
    pipe_buffered_bytes: Arc<ByteBudget>,
    relay_buffered_bytes: Arc<ByteBudget>,
}

impl PipeResources {
    #[cfg(test)]
    pub(crate) fn for_tests() -> Self {
        Self {
            live: Mutex::new(None),
            pipe_buffered_bytes: Arc::new(ByteBudget::new(usize::MAX, false)),
            relay_buffered_bytes: Arc::new(ByteBudget::new(usize::MAX, false)),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_test_limits(pipe: usize, relay: usize) -> Self {
        Self {
            live: Mutex::new(None),
            pipe_buffered_bytes: Arc::new(ByteBudget::new(pipe, false)),
            relay_buffered_bytes: Arc::new(ByteBudget::new(relay, false)),
        }
    }

    pub(crate) fn try_reserve_buffered(&self, amount: usize) -> Result<BufferedBytes, Error> {
        let pipe = self
            .pipe_buffered_bytes
            .try_reserve(amount, ResourceLimitKind::PipeBufferedBytes)?;
        let relay = self
            .relay_buffered_bytes
            .try_reserve(amount, ResourceLimitKind::RelayBufferedBytes)?;
        Ok(BufferedBytes {
            _pipe: pipe,
            _relay: relay,
        })
    }

    pub(crate) fn release_live(&self) {
        match self.live.lock() {
            Ok(mut live) => {
                live.take();
            }
            Err(error) => {
                error.into_inner().take();
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn buffered_bytes(&self) -> (usize, usize) {
        (
            self.pipe_buffered_bytes.used(),
            self.relay_buffered_bytes.used(),
        )
    }
}

pub(crate) struct BufferedBytes {
    _pipe: ByteReservation,
    _relay: ByteReservation,
}

pub(crate) struct LivePipeReservation {
    _relay: OwnedSemaphorePermit,
    _listener: Option<OwnedSemaphorePermit>,
}

impl LivePipeReservation {
    fn new(relay: OwnedSemaphorePermit, listener: Option<OwnedSemaphorePermit>) -> Self {
        metrics::gauge!(
            "relaygate_sdk_resource_used",
            "resource" => "live_pipes"
        )
        .increment(1.0);
        Self {
            _relay: relay,
            _listener: listener,
        }
    }
}

impl Drop for LivePipeReservation {
    fn drop(&mut self) {
        metrics::gauge!(
            "relaygate_sdk_resource_used",
            "resource" => "live_pipes"
        )
        .decrement(1.0);
    }
}

pub(crate) struct ByteBudget {
    maximum: usize,
    used: AtomicUsize,
    observe: bool,
}

impl ByteBudget {
    pub(crate) const fn new(maximum: usize, observe: bool) -> Self {
        Self {
            maximum,
            used: AtomicUsize::new(0),
            observe,
        }
    }

    pub(crate) fn try_reserve(
        self: &Arc<Self>,
        amount: usize,
        kind: ResourceLimitKind,
    ) -> Result<ByteReservation, Error> {
        self.try_charge(amount, kind)?;
        Ok(ByteReservation {
            budget: Arc::clone(self),
            amount,
        })
    }

    fn try_charge(&self, amount: usize, kind: ResourceLimitKind) -> Result<(), Error> {
        let reserved = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(amount)
                    .filter(|next| *next <= self.maximum)
            });
        if reserved.is_err() {
            return Err(resource_exhausted(kind, PeerObservation::Observed));
        }
        if self.observe {
            metrics::gauge!(
                "relaygate_sdk_resource_used",
                "resource" => "buffered_bytes"
            )
            .increment(amount as f64);
        }
        Ok(())
    }

    fn release(&self, amount: usize) {
        self.used.fetch_sub(amount, Ordering::AcqRel);
        if self.observe {
            metrics::gauge!(
                "relaygate_sdk_resource_used",
                "resource" => "buffered_bytes"
            )
            .decrement(amount as f64);
        }
    }

    #[cfg(test)]
    pub(crate) fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }
}

pub(crate) struct ByteReservation {
    budget: Arc<ByteBudget>,
    amount: usize,
}

impl Drop for ByteReservation {
    fn drop(&mut self) {
        self.budget.release(self.amount);
    }
}

#[cfg(test)]
mod tests;
