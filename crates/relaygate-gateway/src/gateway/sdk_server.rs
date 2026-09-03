use std::{sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{Frame, FrameCodec, SessionRole};
use tokio::{
    net::{TcpListener, TcpStream, ToSocketAddrs},
    task::{JoinError, JoinSet},
    time::timeout,
};
use tokio_util::{codec::Framed, sync::CancellationToken};

use crate::GatewayError;

use super::{Gateway, Inner};

const MIN_OFFER_SWEEP_INTERVAL: Duration = Duration::from_millis(10);
const MAX_OFFER_SWEEP_INTERVAL: Duration = Duration::from_millis(100);

impl Gateway {
    pub(super) async fn serve_sdk(
        &self,
        listener: TcpListener,
        shutdown: CancellationToken,
    ) -> Result<(), GatewayError> {
        let mut sessions = JoinSet::new();
        let mut offer_sweep = tokio::time::interval(offer_sweep_interval(self.inner.offer_timeout));
        offer_sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut first_error = None;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
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
                            first_error.get_or_insert_with(|| GatewayError::Io(error));
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
                    let session_shutdown = shutdown.child_token();
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
                        first_error.get_or_insert_with(|| session_task_failure(error));
                        shutdown.cancel();
                        break;
                    }
                }
            }
        }

        shutdown.cancel();
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
                    shutdown_error.get_or_insert_with(|| session_shutdown_task_failure(error));
                }
            }
        }
        match shutdown_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
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
    use futures_util::{SinkExt, StreamExt};
    use relaygate_protocol::{Frame, FrameCodec, SessionRole};
    use tokio::{
        net::{TcpListener, TcpStream},
        time::{Duration, timeout},
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

        let mut sibling = open_sdk_session(address).await?;
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

    async fn open_sdk_session(
        address: std::net::SocketAddr,
    ) -> Result<Framed<TcpStream, FrameCodec>, Box<dyn std::error::Error>> {
        let stream = TcpStream::connect(address).await?;
        let mut framed = Framed::new(stream, FrameCodec::default());
        framed
            .send(Frame::Hello {
                role: SessionRole::Connector,
            })
            .await?;
        assert!(matches!(
            framed.next().await,
            Some(Ok(Frame::Welcome { .. }))
        ));
        Ok(framed)
    }
}
