use std::sync::Arc;

use crate::state::GatewayAction;
use relaygate_protocol::{DestinationId, ErrorCode, Frame, PeerObservation};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{Gateway, GatewayConfig};

const CLUSTER_TOKEN: &str = "gateway-test-token";

#[tokio::test]
async fn snapshot_admission_requires_capacity_and_non_draining_state()
-> Result<(), Box<dyn std::error::Error>> {
    let gateway = Gateway::new(GatewayConfig::new(CLUSTER_TOKEN).with_max_sessions(1))?;
    assert!(gateway.snapshot().sdk_admission_ready);

    let permit = Arc::clone(&gateway.inner.session_slots).try_acquire_owned()?;
    assert!(!gateway.snapshot().sdk_admission_ready);

    drop(permit);
    assert!(gateway.snapshot().sdk_admission_ready);

    gateway.inner.begin_draining();
    assert!(!gateway.snapshot().sdk_admission_ready);
    Ok(())
}

#[tokio::test]
async fn full_offer_queue_rejects_only_the_dial_and_preserves_the_listener()
-> Result<(), Box<dyn std::error::Error>> {
    let gateway = Gateway::new(GatewayConfig::new(CLUSTER_TOKEN))?;
    let destination_id = DestinationId::new();
    let (listener_sender, _listener_receiver) = mpsc::channel(1);
    listener_sender.try_send(Frame::Ping { nonce: 1 })?;
    let (connector_sender, mut connector_receiver) = mpsc::channel(8);
    let (listener, connector, offer) = {
        let mut state = gateway.inner.lock_state();
        let listener = state
            .add_session(listener_sender, CancellationToken::new())
            .ok_or("missing listener session")?;
        let connector = state
            .add_session(connector_sender, CancellationToken::new())
            .ok_or("missing connector session")?;
        let _registration = state.handle(
            listener,
            Frame::Publish {
                request_id: 1,
                destination_id,
            },
        )?;
        let offer = state.handle(
            connector,
            Frame::Dial {
                connection_id: 1,
                destination_id,
            },
        )?;
        (listener, connector, offer)
    };

    gateway.inner.execute_all(offer).await;

    assert!(matches!(
        connector_receiver.try_recv()?,
        Frame::DialFailed {
            connection_id: 1,
            code: ErrorCode::ResourceExhausted,
            observation: PeerObservation::NotObserved,
            ..
        }
    ));
    let after_rejection = gateway.inner.lock_state().handle(
        connector,
        Frame::Dial {
            connection_id: 2,
            destination_id,
        },
    )?;
    assert!(matches!(
        after_rejection.first().and_then(|action| match action {
            GatewayAction::SendSdkFrame(delivery) => Some(&delivery.frame),
            GatewayAction::PublishRegistration { .. }
            | GatewayAction::ResolveRoute { .. }
            | GatewayAction::OpenPeer { .. }
            | GatewayAction::CancelPeerOpen { .. }
            | GatewayAction::SendPeerFrame(_) => None,
        }),
        Some(Frame::Offer { .. })
    ));
    assert_eq!(gateway.snapshot().sessions, 2);
    assert_eq!(gateway.snapshot().bindings, 1);
    assert_eq!(gateway.snapshot().pending_offers, 1);
    assert!(
        !gateway
            .inner
            .lock_state()
            .handle(listener, Frame::Ping { nonce: 2 })?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn full_non_offer_queue_still_removes_the_failed_session_state()
-> Result<(), Box<dyn std::error::Error>> {
    let gateway = Gateway::new(GatewayConfig::new(CLUSTER_TOKEN))?;
    let (sender, _receiver) = mpsc::channel(1);
    sender.try_send(Frame::Ping { nonce: 1 })?;
    let (session, delivery) = {
        let mut state = gateway.inner.lock_state();
        let session = state
            .add_session(sender, CancellationToken::new())
            .ok_or("missing session")?;
        let delivery = state
            .handle(session, Frame::Ping { nonce: 2 })?
            .into_iter()
            .next()
            .ok_or("missing PONG delivery")?;
        (session, delivery)
    };

    gateway.inner.execute_all(vec![delivery]).await;

    assert!(
        gateway
            .inner
            .lock_state()
            .handle(session, Frame::Ping { nonce: 3 })?
            .is_empty()
    );
    assert_eq!(gateway.snapshot().sessions, 0);
    Ok(())
}
