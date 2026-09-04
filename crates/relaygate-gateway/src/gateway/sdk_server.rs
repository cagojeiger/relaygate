use std::{sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{Frame, FrameCodec, SessionRole};
use tokio::{
    net::{TcpListener, TcpStream, ToSocketAddrs},
    task::{JoinError, JoinSet},
    time::{MissedTickBehavior, timeout},
};
use tokio_util::{codec::Framed, sync::CancellationToken};

use crate::GatewayError;

use super::{Gateway, Inner};

const MIN_OFFER_SWEEP_INTERVAL: Duration = Duration::from_millis(10);
const MAX_OFFER_SWEEP_INTERVAL: Duration = Duration::from_millis(100);
const DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(25);

impl Gateway {
    pub(super) async fn serve_sdk(
        &self,
        listener: TcpListener,
        shutdown: CancellationToken,
    ) -> Result<(), GatewayError> {
        self.serve_sdk_with_force(listener, shutdown, CancellationToken::new())
            .await
    }

    pub(super) async fn serve_sdk_with_force(
        &self,
        listener: TcpListener,
        shutdown: CancellationToken,
        force_shutdown: CancellationToken,
    ) -> Result<(), GatewayError> {
        let mut sessions = JoinSet::new();
        let mut offer_sweep = tokio::time::interval(offer_sweep_interval(self.inner.offer_timeout));
        offer_sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let session_lifecycle = CancellationToken::new();
        let mut first_error = None;
        let mut graceful_shutdown = false;
        loop {
            tokio::select! {
                biased;
                _ = force_shutdown.cancelled() => break,
                _ = shutdown.cancelled() => {
                    graceful_shutdown = true;
                    self.inner.begin_draining();
                    break;
                },
                _ = offer_sweep.tick() => self.inner.expire_offers().await,
                accepted = listener.accept() => {
                    let (stream, peer_addr) = match accepted {
                        Ok(accepted) => accepted,
                        Err(error) => {
                            tracing::error!(
                                component = "gateway",
                                event = "gateway.sdk_listener.accept_failed",
                                %error,
                                "Gateway SDK listener failed; stopping Gateway runtime"
                            );
                            first_error.get_or_insert(GatewayError::Io(error));
                            shutdown.cancel();
                            break;
                        }
                    };
                    let Ok(session_slot) = Arc::clone(&self.inner.session_slots).try_acquire_owned()
                    else {
                        tracing::warn!(
                            component = "gateway",
                            event = "gateway.session.rejected",
                            %peer_addr,
                            reason = "session_limit",
                            "rejecting SDK connection because the session limit is reached"
                        );
                        drop(stream);
                        continue;
                    };
                    if let Err(error) = stream.set_nodelay(true) {
                        tracing::warn!(
                            component = "gateway",
                            event = "gateway.socket.configure_failed",
                            %peer_addr,
                            %error,
                            "failed to enable TCP_NODELAY"
                        );
                    }
                    let inner = Arc::clone(&self.inner);
                    let session_shutdown = session_lifecycle.child_token();
                    sessions.spawn(async move {
                        let _session_slot = session_slot;
                        if let Err(error) = inner.run_session(stream, session_shutdown).await {
                            tracing::debug!(
                                component = "gateway",
                                event = "gateway.session.task_ended",
                                %peer_addr,
                                %error,
                                "SDK session ended"
                            );
                        }
                    });
                }
                completed = sessions.join_next(), if !sessions.is_empty() => {
                    if let Some(Err(error)) = completed {
                        tracing::error!(
                            component = "gateway",
                            event = "gateway.session.task_failed",
                            %error,
                            "SDK session task failed; stopping Gateway runtime"
                        );
                        first_error.get_or_insert(session_task_failure(error));
                        session_lifecycle.cancel();
                        break;
                    }
                }
            }
        }

        drop(listener);
        if graceful_shutdown {
            let snapshot = self.snapshot();
            tracing::info!(
                component = "gateway",
                event = "gateway.drain.started",
                pending_offers = snapshot.pending_offers,
                live_pipes = snapshot.live_pipes,
                remote_open_attempts = snapshot.remote_open_attempts,
                drain_timeout_ms = self.inner.drain_timeout.as_millis(),
                "Gateway stopped admission and started draining existing work"
            );
            if wait_until_drained(&self.inner, self.inner.drain_timeout).await {
                tracing::info!(
                    component = "gateway",
                    event = "gateway.drain.completed",
                    "Gateway drain completed"
                );
            } else {
                let snapshot = self.snapshot();
                tracing::warn!(
                    component = "gateway",
                    event = "gateway.drain.timed_out",
                    pending_offers = snapshot.pending_offers,
                    live_pipes = snapshot.live_pipes,
                    remote_open_attempts = snapshot.remote_open_attempts,
                    "Gateway drain deadline expired; forcing remaining work to close"
                );
            }
        }
        session_lifecycle.cancel();
        let mut shutdown_error = first_error;
        while let Some(result) = sessions.join_next().await {
            match result {
                Ok(()) => {}
                Err(error) => {
                    tracing::error!(
                        component = "gateway",
                        event = "gateway.session.shutdown_task_failed",
                        %error,
                        "SDK session task failed during shutdown"
                    );
                    shutdown_error.get_or_insert(session_shutdown_task_failure(error));
                }
            }
        }
        match shutdown_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

async fn wait_until_drained(inner: &Inner, drain_timeout: Duration) -> bool {
    if inner.is_drained() {
        return true;
    }
    timeout(drain_timeout, async {
        let mut poll = tokio::time::interval(DRAIN_POLL_INTERVAL);
        poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            poll.tick().await;
            if inner.is_drained() {
                return;
            }
        }
    })
    .await
    .is_ok()
}

impl Inner {
    async fn expire_offers(self: &Arc<Self>) {
        let actions = {
            let mut state = self.lock_state();
            let actions = state.expire_offers(std::time::Instant::now());
            self.commit_registration_actions(&actions);
            actions
        };
        self.execute_all(actions).await;
    }
}

fn offer_sweep_interval(offer_timeout: Duration) -> Duration {
    offer_timeout.clamp(MIN_OFFER_SWEEP_INTERVAL, MAX_OFFER_SWEEP_INTERVAL)
}

fn session_task_failure(error: JoinError) -> GatewayError {
    GatewayError::Runtime(format!("SDK session task failed: {error}"))
}

fn session_shutdown_task_failure(error: JoinError) -> GatewayError {
    GatewayError::Runtime(format!("SDK session task failed during shutdown: {error}"))
}

/// Checks SDK admission readiness through a TCP `HELLO`/`WELCOME` exchange.
///
/// This does not check RouteTable availability, Listener bindings, Pipe
/// establishment, or application payload processing.
pub async fn check(address: impl ToSocketAddrs, deadline: Duration) -> Result<(), GatewayError> {
    timeout(deadline, async {
        let stream = TcpStream::connect(address).await?;
        let mut framed = Framed::new(stream, FrameCodec::default());
        framed
            .send(Frame::Hello {
                role: SessionRole::Connector,
            })
            .await?;
        match framed.next().await {
            Some(Ok(Frame::Welcome { .. })) => Ok(()),
            Some(Ok(_)) | None => Err(GatewayError::UnexpectedAdmissionResponse),
            Some(Err(error)) => Err(GatewayError::Protocol(error)),
        }
    })
    .await
    .map_err(|_| GatewayError::AdmissionCheckTimeout)?
}

#[cfg(test)]
mod tests {
    use super::check;
    use futures_util::{SinkExt, StreamExt};
    use relaygate_protocol::{ClientKey, Frame, FrameCodec, SessionRole};
    use tokio::{
        net::{TcpListener, TcpStream},
        time::{Duration, sleep, timeout},
    };
    use tokio_util::{codec::Framed, sync::CancellationToken};

    use crate::{Gateway, GatewayConfig, GatewayError};

    #[tokio::test]
    async fn admitted_session_panic_cleans_siblings_and_stops_gateway()
    -> Result<(), Box<dyn std::error::Error>> {
        let gateway = Gateway::new(GatewayConfig::default())?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let shutdown = CancellationToken::new();
        let serving_gateway = gateway.clone();
        let serving_shutdown = shutdown.clone();
        let serving =
            tokio::spawn(
                async move { serving_gateway.serve_sdk(listener, serving_shutdown).await },
            );

        let mut sibling = open_sdk_session(address, SessionRole::Connector).await?;
        assert_eq!(gateway.snapshot().sessions, 1);
        gateway
            .inner
            .panic_next_session_after_admission
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let stream = TcpStream::connect(address).await?;
        let mut panicking = Framed::new(stream, FrameCodec::default());
        panicking
            .send(Frame::Hello {
                role: SessionRole::Connector,
            })
            .await?;

        let result = timeout(Duration::from_secs(2), serving).await??;
        assert!(matches!(result, Err(GatewayError::Runtime(_))));
        assert_eq!(gateway.snapshot().sessions, 0);
        let sibling_end = timeout(Duration::from_secs(1), sibling.next()).await?;
        assert!(sibling_end.is_none() || sibling_end.is_some_and(|frame| frame.is_err()));
        shutdown.cancel();
        Ok(())
    }

    #[tokio::test]
    async fn graceful_shutdown_stops_admission_and_waits_for_existing_pipe()
    -> Result<(), Box<dyn std::error::Error>> {
        let gateway = Gateway::new(
            GatewayConfig::new([("echo.alpha".to_owned(), "secret".to_owned())])
                .with_drain_timeout(Duration::from_secs(1)),
        )?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let shutdown = CancellationToken::new();
        let serving_gateway = gateway.clone();
        let serving_shutdown = shutdown.clone();
        let serving =
            tokio::spawn(
                async move { serving_gateway.serve_sdk(listener, serving_shutdown).await },
            );

        let (mut listener_sdk, mut connector_sdk, pipe_id) = open_pipe(address).await?;
        assert_eq!(gateway.snapshot().live_pipes, 1);
        shutdown.cancel();
        sleep(Duration::from_millis(50)).await;

        assert!(gateway.snapshot().draining);
        assert!(!serving.is_finished());
        assert!(check(address, Duration::from_millis(50)).await.is_err());

        connector_sdk
            .send(Frame::Open {
                connection_id: 2,
                client_id: "echo.alpha".to_owned(),
            })
            .await?;
        assert!(matches!(
            timeout(Duration::from_secs(1), connector_sdk.next()).await?,
            Some(Ok(Frame::OpenFailed {
                connection_id: 2,
                code: relaygate_protocol::ErrorCode::Unavailable,
                ..
            }))
        ));
        listener_sdk
            .send(Frame::Register {
                request_id: 2,
                client_id: "echo.alpha".to_owned(),
                client_key: ClientKey::new("secret"),
            })
            .await?;
        assert!(matches!(
            timeout(Duration::from_secs(1), listener_sdk.next()).await?,
            Some(Ok(Frame::RegisterFailed {
                request_id: 2,
                code: relaygate_protocol::ErrorCode::Unavailable,
                ..
            }))
        ));

        connector_sdk.send(Frame::Close { pipe_id }).await?;
        assert!(matches!(
            timeout(Duration::from_secs(1), listener_sdk.next()).await?,
            Some(Ok(Frame::Close { pipe_id: closed })) if closed == pipe_id
        ));
        timeout(Duration::from_secs(1), serving).await???;
        assert_eq!(gateway.snapshot().live_pipes, 0);
        Ok(())
    }

    #[tokio::test]
    async fn graceful_shutdown_forces_remaining_pipe_closed_at_deadline()
    -> Result<(), Box<dyn std::error::Error>> {
        let gateway = Gateway::new(
            GatewayConfig::new([("echo.alpha".to_owned(), "secret".to_owned())])
                .with_drain_timeout(Duration::from_millis(50)),
        )?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let shutdown = CancellationToken::new();
        let serving_gateway = gateway.clone();
        let serving_shutdown = shutdown.clone();
        let serving =
            tokio::spawn(
                async move { serving_gateway.serve_sdk(listener, serving_shutdown).await },
            );

        let (_listener_sdk, _connector_sdk, _pipe_id) = open_pipe(address).await?;
        shutdown.cancel();
        timeout(Duration::from_secs(1), serving).await???;
        assert!(gateway.snapshot().draining);
        assert_eq!(gateway.snapshot().sessions, 0);
        assert_eq!(gateway.snapshot().live_pipes, 0);
        Ok(())
    }

    #[tokio::test]
    async fn forced_shutdown_skips_drain_and_closes_existing_pipe()
    -> Result<(), Box<dyn std::error::Error>> {
        let gateway = Gateway::new(
            GatewayConfig::new([("echo.alpha".to_owned(), "secret".to_owned())])
                .with_drain_timeout(Duration::from_secs(10)),
        )?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let graceful = CancellationToken::new();
        let force = CancellationToken::new();
        let serving_gateway = gateway.clone();
        let serving_force = force.clone();
        let serving = tokio::spawn(async move {
            serving_gateway
                .serve_sdk_with_force(listener, graceful, serving_force)
                .await
        });

        let (_listener_sdk, _connector_sdk, _pipe_id) = open_pipe(address).await?;
        force.cancel();
        timeout(Duration::from_secs(1), serving).await???;

        assert!(!gateway.snapshot().draining);
        assert_eq!(gateway.snapshot().sessions, 0);
        assert_eq!(gateway.snapshot().live_pipes, 0);
        Ok(())
    }

    async fn open_pipe(
        address: std::net::SocketAddr,
    ) -> Result<
        (
            Framed<TcpStream, FrameCodec>,
            Framed<TcpStream, FrameCodec>,
            relaygate_protocol::PipeId,
        ),
        Box<dyn std::error::Error>,
    > {
        let mut listener = open_sdk_session(address, SessionRole::Listener).await?;
        listener
            .send(Frame::Register {
                request_id: 1,
                client_id: "echo.alpha".to_owned(),
                client_key: ClientKey::new("secret"),
            })
            .await?;
        assert!(matches!(
            listener.next().await,
            Some(Ok(Frame::Registered { .. }))
        ));

        let mut connector = open_sdk_session(address, SessionRole::Connector).await?;
        connector
            .send(Frame::Open {
                connection_id: 1,
                client_id: "echo.alpha".to_owned(),
            })
            .await?;
        let Some(Ok(Frame::Offer { pipe_id, .. })) = listener.next().await else {
            return Err("Listener did not receive an OFFER".into());
        };
        listener.send(Frame::OfferAccepted { pipe_id }).await?;
        assert!(matches!(
            connector.next().await,
            Some(Ok(Frame::Opened { pipe_id: opened })) if opened == pipe_id
        ));
        Ok((listener, connector, pipe_id))
    }

    async fn open_sdk_session(
        address: std::net::SocketAddr,
        role: SessionRole,
    ) -> Result<Framed<TcpStream, FrameCodec>, Box<dyn std::error::Error>> {
        let stream = TcpStream::connect(address).await?;
        let mut framed = Framed::new(stream, FrameCodec::default());
        framed.send(Frame::Hello { role }).await?;
        assert!(matches!(
            framed.next().await,
            Some(Ok(Frame::Welcome { .. }))
        ));
        Ok(framed)
    }
}
