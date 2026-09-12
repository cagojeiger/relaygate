use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{Frame, FrameCodec, SessionId};
use relaygate_transport::BoxedIo;
use tokio::time::{Instant, sleep_until, timeout};
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

use crate::{Config, Error, ErrorCode, PeerObservation, Result};

mod outbound;

pub(crate) use outbound::{FrameCommit, SessionOutbound, session_outbound_channel};

pub(crate) type WireTransport = Framed<BoxedIo, FrameCodec>;

const SDK_FRAME_INITIAL_CAPACITY: usize = 2 * 1024;
const SDK_FRAME_WRITE_BACKPRESSURE_BOUNDARY: usize = 8 * 1024;

pub(crate) struct EstablishedSession {
    pub(crate) id: SessionId,
    pub(crate) transport: WireTransport,
}

pub(crate) async fn establish(config: &Config) -> Result<EstablishedSession> {
    crate::observability::observe("session_connect", establish_inner(config)).await
}

async fn establish_inner(config: &Config) -> Result<EstablishedSession> {
    let stream = config.transport.connect(config.connect_timeout).await?;
    let mut transport = Framed::with_capacity(
        stream,
        FrameCodec::new(config.max_frame_len),
        SDK_FRAME_INITIAL_CAPACITY,
    );
    transport.set_backpressure_boundary(SDK_FRAME_WRITE_BACKPRESSURE_BOUNDARY);
    timeout(config.connect_timeout, transport.send(Frame::Hello))
        .await
        .map_err(|_| Error::deadline(PeerObservation::NotObserved))?
        .map_err(|error| Error::unavailable(format!("session hello failed: {error}")))?;
    let frame = timeout(config.connect_timeout, transport.next())
        .await
        .map_err(|_| Error::deadline(PeerObservation::NotObserved))?
        .ok_or_else(|| Error::unavailable("Gateway closed before WELCOME"))?
        .map_err(|error| {
            Error::new(
                ErrorCode::ProtocolError,
                PeerObservation::NotObserved,
                format!("WELCOME decode failed: {error}"),
            )
        })?;
    let session_id = match frame {
        Frame::Welcome { session_id } => session_id,
        Frame::SessionRejected { code, message } => {
            return Err(Error::new(
                ErrorCode::from_wire(code),
                PeerObservation::Observed,
                message,
            ));
        }
        _ => {
            return Err(Error::new(
                ErrorCode::ProtocolError,
                PeerObservation::NotObserved,
                "first Gateway response was not WELCOME",
            ));
        }
    };
    tracing::debug!(
        component = "sdk",
        event = "sdk.session.ready",
        session_id = %session_id.as_uuid(),
        "SDK session is ready"
    );
    Ok(EstablishedSession {
        id: session_id,
        transport,
    })
}

pub(crate) async fn send_bounded(
    transport: &mut WireTransport,
    frame: Frame,
    duration: Duration,
    cancel: &CancellationToken,
) -> std::result::Result<(), ()> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(()),
        result = timeout(duration, transport.send(frame)) => {
            match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(_)) | Err(_) => Err(()),
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct SessionHeartbeat {
    idle_timeout: Duration,
    response_timeout: Duration,
    last_inbound: Instant,
    pending: Option<PendingHeartbeat>,
    next_nonce: u64,
}

#[derive(Debug)]
struct PendingHeartbeat {
    nonce: u64,
    deadline: Instant,
}

impl SessionHeartbeat {
    pub(crate) fn new(config: &Config, session_id: SessionId, salt: u8) -> Self {
        Self {
            idle_timeout: jittered_duration(config.heartbeat_idle_interval, session_id, salt),
            response_timeout: config.heartbeat_response_timeout,
            last_inbound: Instant::now(),
            pending: None,
            next_nonce: 1,
        }
    }

    pub(crate) fn observe_inbound(&mut self, frame: &Frame) {
        match (&self.pending, frame) {
            (Some(pending), Frame::Pong { nonce })
                if pending.nonce == *nonce && Instant::now() < pending.deadline => {}
            (Some(_), _) | (None, Frame::Pong { .. }) => return,
            (None, Frame::Hello)
            | (None, Frame::Welcome { .. })
            | (None, Frame::SessionRejected { .. })
            | (None, Frame::Publish { .. })
            | (None, Frame::Published { .. })
            | (None, Frame::PublishFailed { .. })
            | (None, Frame::Unpublish { .. })
            | (None, Frame::Unpublished { .. })
            | (None, Frame::Dial { .. })
            | (None, Frame::Offer { .. })
            | (None, Frame::OfferAccepted { .. })
            | (None, Frame::OfferRejected { .. })
            | (None, Frame::Opened { .. })
            | (None, Frame::DialFailed { .. })
            | (None, Frame::Data { .. })
            | (None, Frame::Fin { .. })
            | (None, Frame::Close { .. })
            | (None, Frame::Reset { .. })
            | (None, Frame::Ping { .. })
            | (None, Frame::Cancel { .. }) => {}
        }
        self.last_inbound = Instant::now();
        self.pending = None;
    }

    pub(crate) fn response_timed_out(&self) -> bool {
        self.pending
            .as_ref()
            .is_some_and(|pending| pending.deadline <= Instant::now())
    }

    pub(crate) fn next_deadline(&self) -> Instant {
        self.pending.as_ref().map_or_else(
            || {
                self.last_inbound
                    .checked_add(self.idle_timeout)
                    .unwrap_or(self.last_inbound)
            },
            |pending| pending.deadline,
        )
    }

    pub(crate) fn on_deadline(&mut self) -> Option<Frame> {
        let now = Instant::now();
        if self
            .pending
            .as_ref()
            .is_some_and(|pending| pending.deadline <= now)
        {
            return None;
        }
        let nonce = self.next_nonce;
        self.next_nonce = self.next_nonce.wrapping_add(1).max(1);
        let deadline = now.checked_add(self.response_timeout)?;
        self.pending = Some(PendingHeartbeat { nonce, deadline });
        Some(Frame::Ping { nonce })
    }

    pub(crate) fn mark_probe_committed(&mut self) {
        if let Some(pending) = self.pending.as_mut() {
            let now = Instant::now();
            pending.deadline = now.checked_add(self.response_timeout).unwrap_or(now);
        }
    }
}

pub(crate) async fn wait_for_heartbeat(deadline: Instant) {
    sleep_until(deadline).await;
}

fn jittered_duration(duration: Duration, session_id: SessionId, salt: u8) -> Duration {
    let mut hash = u64::from(salt);
    for byte in session_id.as_uuid().as_bytes() {
        hash = hash.wrapping_mul(16_777_619) ^ u64::from(*byte);
    }
    let offset_per_mille = (hash % 201) as i128 - 100;
    let factor_per_mille = 1_000_i128 + offset_per_mille;
    let nanos = duration.as_nanos().saturating_mul(factor_per_mille as u128) / 1_000;
    Duration::from_nanos(nanos.try_into().unwrap_or(u64::MAX)).max(Duration::from_millis(1))
}

pub(crate) struct ReconnectBackoff {
    initial: Duration,
    current: Duration,
    maximum: Duration,
    entropy: u64,
}

impl ReconnectBackoff {
    pub(crate) fn new(initial: Duration, maximum: Duration) -> Self {
        let seed = SessionId::new();
        let mut entropy = 14_695_981_039_346_656_037_u64;
        for byte in seed.as_uuid().as_bytes() {
            entropy = entropy.wrapping_mul(1_099_511_628_211) ^ u64::from(*byte);
        }
        Self {
            initial,
            current: initial,
            maximum,
            entropy,
        }
    }

    pub(crate) fn next_delay(&mut self) -> Duration {
        self.entropy ^= self.entropy << 13;
        self.entropy ^= self.entropy >> 7;
        self.entropy ^= self.entropy << 17;

        let base_nanos = self.current.as_nanos();
        let floor_nanos = base_nanos.saturating_mul(2) / 3;
        let jitter_nanos = (base_nanos - floor_nanos).saturating_mul(u128::from(self.entropy))
            / u128::from(u64::MAX);
        let delay = duration_from_nanos(floor_nanos.saturating_add(jitter_nanos));
        self.current = self.current.saturating_mul(2).min(self.maximum);
        delay
    }

    pub(crate) fn reset(&mut self) {
        self.current = self.initial;
    }

    #[cfg(test)]
    pub(crate) const fn current_delay(&self) -> Duration {
        self.current
    }
}

fn duration_from_nanos(nanos: u128) -> Duration {
    const NANOS_PER_SECOND: u128 = 1_000_000_000;
    let seconds = (nanos / NANOS_PER_SECOND).min(u128::from(u64::MAX));
    let subsecond_nanos = if seconds == u128::from(u64::MAX) {
        999_999_999
    } else {
        nanos % NANOS_PER_SECOND
    };
    Duration::new(seconds as u64, subsecond_nanos as u32).max(Duration::from_millis(1))
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error as StdError,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use bytes::Bytes;
    use futures_util::{SinkExt, StreamExt};
    use relaygate_protocol::{
        BindingId, DEFAULT_MAX_FRAME_LEN, Frame, FrameCodec, PipeId, SessionId,
    };
    use tokio::{
        net::TcpListener,
        sync::oneshot,
        time::{Instant, sleep, timeout},
    };
    use tokio_util::codec::Framed;

    use super::{ReconnectBackoff, SessionHeartbeat};
    use crate::{
        AccessToken, AccessTokenSource, Config, Destination, ErrorCode, ListenerStatus,
        PeerObservation, Relay,
    };

    type TestResult<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;

    fn heartbeat() -> SessionHeartbeat {
        let config = Config::new_insecure_for_tests("127.0.0.1:0")
            .with_heartbeat(Duration::from_secs(60), Duration::from_secs(20));
        SessionHeartbeat::new(&config, SessionId::new(), 0x43)
    }

    #[test]
    fn inbound_activity_without_pending_probe_resets_idle_deadline() {
        let mut heartbeat = heartbeat();
        heartbeat.last_inbound = Instant::now() - Duration::from_secs(30);
        let previous_deadline = heartbeat.next_deadline();

        heartbeat.observe_inbound(&Frame::Ping { nonce: 7 });

        assert!(heartbeat.next_deadline() > previous_deadline);
        assert!(heartbeat.pending.is_none());
    }

    #[test]
    fn pending_probe_requires_matching_pong() {
        let mut heartbeat = heartbeat();
        heartbeat.last_inbound = Instant::now() - Duration::from_secs(60);

        assert!(matches!(
            heartbeat.on_deadline(),
            Some(Frame::Ping { nonce: 1 })
        ));
        assert!(heartbeat.pending.is_some());

        heartbeat.observe_inbound(&Frame::Data {
            pipe_id: PipeId::new(SessionId::new(), 1),
            payload: Bytes::from_static(b"x"),
        });
        assert!(heartbeat.pending.is_some());

        heartbeat.observe_inbound(&Frame::Pong { nonce: 999 });
        assert!(heartbeat.pending.is_some());

        heartbeat.observe_inbound(&Frame::Pong { nonce: 1 });
        assert!(heartbeat.pending.is_none());
    }

    #[test]
    fn probe_commit_starts_full_response_window() {
        let mut heartbeat = heartbeat();
        heartbeat.last_inbound = Instant::now() - Duration::from_secs(60);
        assert!(matches!(
            heartbeat.on_deadline(),
            Some(Frame::Ping { nonce: 1 })
        ));
        if let Some(pending) = heartbeat.pending.as_mut() {
            pending.deadline = Instant::now() - Duration::from_millis(1);
        }

        let committed_at = Instant::now();
        heartbeat.mark_probe_committed();

        assert!(!heartbeat.response_timed_out());
        assert!(heartbeat.next_deadline() >= committed_at + heartbeat.response_timeout);
    }

    #[test]
    fn late_matching_pong_does_not_clear_pending_probe() {
        let mut heartbeat = heartbeat();
        heartbeat.last_inbound = Instant::now() - Duration::from_secs(60);
        assert!(matches!(
            heartbeat.on_deadline(),
            Some(Frame::Ping { nonce: 1 })
        ));
        if let Some(pending) = heartbeat.pending.as_mut() {
            pending.deadline = Instant::now() - Duration::from_millis(1);
        }

        heartbeat.observe_inbound(&Frame::Pong { nonce: 1 });

        assert!(heartbeat.pending.is_some());
        assert!(heartbeat.response_timed_out());
    }

    #[test]
    fn reconnect_backoff_uses_bounded_jitter_and_resets() {
        let initial = Duration::from_millis(100);
        let maximum = Duration::from_millis(400);
        let mut backoff = ReconnectBackoff::new(initial, maximum);

        let first = backoff.next_delay();
        let second = backoff.next_delay();
        let third = backoff.next_delay();
        let capped = backoff.next_delay();
        assert!((Duration::from_millis(66)..=initial).contains(&first));
        assert!((Duration::from_millis(133)..=Duration::from_millis(200)).contains(&second));
        assert!((Duration::from_millis(266)..=maximum).contains(&third));
        assert!((Duration::from_millis(266)..=maximum).contains(&capped));

        backoff.reset();
        let reset = backoff.next_delay();
        assert!((Duration::from_millis(66)..=initial).contains(&reset));
    }

    #[tokio::test]
    async fn initial_unexpected_non_welcome_frame_returns_protocol_error() -> TestResult {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut transport = Framed::new(stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
            let first = transport.next().await.ok_or("SDK closed before HELLO")??;
            if !matches!(first, Frame::Hello) {
                return Err::<(), Box<dyn StdError + Send + Sync>>(
                    "SDK first frame was not HELLO".into(),
                );
            }
            transport.send(Frame::Ping { nonce: 1 }).await?;
            Ok::<(), Box<dyn StdError + Send + Sync>>(())
        });

        let error = match Relay::connect(Config::new_insecure_for_tests(address.to_string())).await
        {
            Ok(relay) => {
                relay.close();
                return Err("unexpected non-WELCOME frame established RelaySession".into());
            }
            Err(error) => error,
        };

        assert_eq!(error.code(), ErrorCode::ProtocolError);
        assert_eq!(error.observation(), PeerObservation::NotObserved);
        assert!(
            error
                .to_string()
                .contains("first Gateway response was not WELCOME")
        );
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn returned_listener_republishes_after_unexpected_runtime_frame() -> TestResult {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let destination: Destination = "inference/stt.seoul".parse()?;
        let expected_destination = destination.clone();
        let (send_unexpected, receive_unexpected) = oneshot::channel();
        let (send_republished, receive_republished) = oneshot::channel();
        let (send_shutdown, receive_shutdown) = oneshot::channel();
        let server = tokio::spawn(async move {
            let first_session_id = SessionId::new();
            let (first_stream, _) = listener.accept().await?;
            let mut first = Framed::new(first_stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
            let first_hello = first
                .next()
                .await
                .ok_or("SDK closed before first HELLO")??;
            if !matches!(first_hello, Frame::Hello) {
                return Err::<(), Box<dyn StdError + Send + Sync>>(
                    "SDK first frame was not HELLO".into(),
                );
            }
            first
                .send(Frame::Welcome {
                    session_id: first_session_id,
                })
                .await?;
            let first_publish = first
                .next()
                .await
                .ok_or("SDK closed before first PUBLISH")??;
            let (first_request_id, first_destination) = match first_publish {
                Frame::Publish {
                    request_id,
                    destination,
                    ..
                } => (request_id, destination),
                _ => return Err("SDK did not PUBLISH on the first session".into()),
            };
            if first_destination != expected_destination {
                return Err("SDK published an unexpected Destination".into());
            }
            first
                .send(Frame::Published {
                    request_id: first_request_id,
                    binding_id: BindingId::new(),
                })
                .await?;

            receive_unexpected
                .await
                .map_err(|_| "runtime-frame trigger was dropped")?;
            first.send(Frame::Hello).await?;
            let first_end = timeout(Duration::from_secs(1), first.next()).await?;
            if matches!(first_end, Some(Ok(_))) {
                return Err("SDK kept the invalid runtime session open".into());
            }

            let second_session_id = SessionId::new();
            let (second_stream, _) = listener.accept().await?;
            let mut second = Framed::new(second_stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
            let second_hello = second
                .next()
                .await
                .ok_or("SDK closed before replacement HELLO")??;
            if !matches!(second_hello, Frame::Hello) {
                return Err("SDK replacement first frame was not HELLO".into());
            }
            second
                .send(Frame::Welcome {
                    session_id: second_session_id,
                })
                .await?;
            let second_publish = second
                .next()
                .await
                .ok_or("SDK closed before replacement PUBLISH")??;
            let (second_request_id, second_destination) = match second_publish {
                Frame::Publish {
                    request_id,
                    destination,
                    ..
                } => (request_id, destination),
                _ => return Err("SDK did not republish on the replacement session".into()),
            };
            second
                .send(Frame::Published {
                    request_id: second_request_id,
                    binding_id: BindingId::new(),
                })
                .await?;
            let _ =
                send_republished.send((first_session_id, second_session_id, second_destination));
            let _ = receive_shutdown.await;
            Ok::<(), Box<dyn StdError + Send + Sync>>(())
        });

        let config = Config::new_insecure_for_tests(address.to_string())
            .with_operation_timeout(Duration::from_secs(2))
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20));
        let relay = Relay::connect(config).await?;
        let publication = relay
            .listen(
                destination.clone(),
                AccessTokenSource::static_token(AccessToken::new("grant")?),
            )
            .await?;
        assert_eq!(publication.status(), ListenerStatus::Active);
        send_unexpected
            .send(())
            .map_err(|_| "fake Gateway stopped before runtime-frame trigger")?;

        let (first_session_id, second_session_id, republished_destination) =
            timeout(Duration::from_secs(2), receive_republished).await??;
        assert_ne!(first_session_id, second_session_id);
        assert_eq!(republished_destination, destination);
        timeout(Duration::from_secs(1), async {
            while publication.status() != ListenerStatus::Active {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;

        relay.close();
        let _ = send_shutdown.send(());
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn dial_supplies_token_only_after_replacement_session_is_ready() -> TestResult {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let destination: Destination = "inference/stt.seoul".parse()?;
        let expected_destination = destination.clone();
        let (close_first_tx, close_first_rx) = oneshot::channel();
        let (second_hello_tx, second_hello_rx) = oneshot::channel();
        let (welcome_second_tx, welcome_second_rx) = oneshot::channel();
        let (observed_token_tx, observed_token_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (first_stream, _) = listener.accept().await?;
            let mut first = Framed::new(first_stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
            if !matches!(
                first
                    .next()
                    .await
                    .ok_or("SDK closed before first HELLO")??,
                Frame::Hello
            ) {
                return Err::<(), Box<dyn StdError + Send + Sync>>(
                    "SDK first frame was not HELLO".into(),
                );
            }
            first
                .send(Frame::Welcome {
                    session_id: SessionId::new(),
                })
                .await?;
            close_first_rx
                .await
                .map_err(|_| "first-session close trigger was dropped")?;
            drop(first);

            let (second_stream, _) = listener.accept().await?;
            let mut second = Framed::new(second_stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
            if !matches!(
                second
                    .next()
                    .await
                    .ok_or("SDK closed before replacement HELLO")??,
                Frame::Hello
            ) {
                return Err("SDK replacement first frame was not HELLO".into());
            }
            let _ = second_hello_tx.send(());
            welcome_second_rx
                .await
                .map_err(|_| "replacement WELCOME trigger was dropped")?;
            second
                .send(Frame::Welcome {
                    session_id: SessionId::new(),
                })
                .await?;
            let dial = second
                .next()
                .await
                .ok_or("SDK closed before replacement DIAL")??;
            let (connection_id, access_token) = match dial {
                Frame::Dial {
                    connection_id,
                    destination,
                    access_token,
                } if destination == expected_destination => (connection_id, access_token),
                _ => return Err("SDK did not DIAL the expected Destination".into()),
            };
            let _ = observed_token_tx.send(access_token.expose_secret().to_owned());
            second
                .send(Frame::DialFailed {
                    connection_id,
                    code: relaygate_protocol::ErrorCode::Unavailable,
                    observation: relaygate_protocol::PeerObservation::Observed,
                    message: "test rejection".to_owned(),
                })
                .await?;
            Ok::<(), Box<dyn StdError + Send + Sync>>(())
        });

        let relay = Relay::connect(
            Config::new_insecure_for_tests(address.to_string())
                .with_operation_timeout(Duration::from_secs(2))
                .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20)),
        )
        .await?;
        close_first_tx
            .send(())
            .map_err(|_| "fake Gateway stopped before first-session close")?;
        timeout(Duration::from_secs(1), second_hello_rx).await??;

        let supplies = Arc::new(AtomicUsize::new(0));
        let observed_supplies = Arc::clone(&supplies);
        let source = AccessTokenSource::dynamic(move |_| {
            let observed_supplies = Arc::clone(&observed_supplies);
            async move {
                observed_supplies.fetch_add(1, Ordering::SeqCst);
                AccessToken::new("fresh-grant").map_err(|_| crate::AccessTokenSourceError)
            }
        });
        let dial_relay = relay.clone();
        let dial = tokio::spawn(async move { dial_relay.dial(destination, source).await });

        sleep(Duration::from_millis(50)).await;
        assert_eq!(supplies.load(Ordering::SeqCst), 0);
        welcome_second_tx
            .send(())
            .map_err(|_| "fake Gateway stopped before replacement WELCOME")?;
        assert_eq!(
            timeout(Duration::from_secs(1), observed_token_rx).await??,
            "fresh-grant"
        );
        assert_eq!(supplies.load(Ordering::SeqCst), 1);
        let error = match timeout(Duration::from_secs(1), dial).await?? {
            Ok(_) => return Err("fake Gateway unexpectedly accepted the test DIAL".into()),
            Err(error) => error,
        };
        assert_eq!(error.code(), ErrorCode::Unavailable);

        relay.close();
        server.await??;
        Ok(())
    }
}
