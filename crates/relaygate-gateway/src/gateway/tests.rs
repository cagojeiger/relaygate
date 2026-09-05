use crate::state::GatewayAction;
use relaygate_protocol::{DestinationId, ErrorCode, Frame};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{Gateway, GatewayConfig};

const CLUSTER_TOKEN: &str = "gateway-test-token";

#[tokio::test]
async fn full_delivery_queue_immediately_removes_the_failed_session_state()
-> Result<(), Box<dyn std::error::Error>> {
    let gateway = Gateway::new(GatewayConfig::new(CLUSTER_TOKEN))?;
    let destination_id = DestinationId::new();
    let (listener_sender, _listener_receiver) = mpsc::channel(1);
    listener_sender.try_send(Frame::Ping { nonce: 1 })?;
    let (connector_sender, _connector_receiver) = mpsc::channel(8);
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

    let after_cleanup = gateway.inner.lock_state().handle(
        connector,
        Frame::Dial {
            connection_id: 2,
            destination_id,
        },
    )?;
    assert!(matches!(
        after_cleanup.first().and_then(|action| match action {
            GatewayAction::SendSdkFrame(delivery) => Some(&delivery.frame),
            GatewayAction::PublishRegistration { .. }
            | GatewayAction::ResolveRoute { .. }
            | GatewayAction::OpenPeer { .. }
            | GatewayAction::CancelPeerOpen { .. }
            | GatewayAction::SendPeerFrame(_) => None,
        }),
        Some(Frame::DialFailed {
            code: ErrorCode::NotFound,
            ..
        })
    ));
    assert!(
        gateway
            .inner
            .lock_state()
            .handle(listener, Frame::Ping { nonce: 2 })?
            .is_empty()
    );
    Ok(())
}
