mod support;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{ClientKey, Frame, SessionRole};
use tokio::time::{Duration, timeout};

use support::{TestGateway, TestResult, next_frame, sdk_session};

#[tokio::test]
async fn local_pipe_opens_only_after_listener_acceptance_and_relays_bytes() -> TestResult {
    let gateway = TestGateway::start(&[("echo.alpha", "secret")]).await?;
    let mut listener = sdk_session(gateway.address, SessionRole::Listener).await?;
    listener
        .send(Frame::Register {
            request_id: 1,
            client_id: "echo.alpha".to_owned(),
            client_key: ClientKey::new("secret"),
        })
        .await?;
    let Frame::Registered { binding_id, .. } = next_frame(&mut listener).await? else {
        return Err("listener registration did not succeed".into());
    };

    let mut connector = sdk_session(gateway.address, SessionRole::Connector).await?;
    connector
        .send(Frame::Open {
            connection_id: 7,
            client_id: "echo.alpha".to_owned(),
        })
        .await?;
    let Frame::Offer {
        pipe_id,
        binding_id: offered_binding,
        ..
    } = next_frame(&mut listener).await?
    else {
        return Err("listener did not receive an offer".into());
    };
    assert_eq!(offered_binding, binding_id);
    assert!(
        timeout(Duration::from_millis(20), connector.next())
            .await
            .is_err()
    );

    listener.send(Frame::OfferAccepted { pipe_id }).await?;
    assert!(matches!(
        next_frame(&mut connector).await?,
        Frame::Opened { pipe_id: opened } if opened == pipe_id
    ));

    let snapshot = gateway.snapshot();
    assert_eq!(snapshot.route_registrations_synced, 0);
    assert_eq!(snapshot.route_registrations_unsynced, 0);
    assert_eq!(snapshot.remote_open_attempts, 0);
    assert_eq!(snapshot.peer_transports_connecting, 0);
    assert_eq!(snapshot.peer_transports_ready, 0);
    assert_eq!(snapshot.peer_streams, 0);

    let request = Bytes::from_static(b"hello relaygate");
    connector
        .send(Frame::Data {
            pipe_id,
            payload: request.clone(),
        })
        .await?;
    assert!(matches!(
        next_frame(&mut listener).await?,
        Frame::Data { pipe_id: received, payload } if received == pipe_id && payload == request
    ));

    let response = Bytes::from_static(b"echo response");
    listener
        .send(Frame::Data {
            pipe_id,
            payload: response.clone(),
        })
        .await?;
    assert!(matches!(
        next_frame(&mut connector).await?,
        Frame::Data { pipe_id: received, payload } if received == pipe_id && payload == response
    ));

    connector.send(Frame::Fin { pipe_id }).await?;
    assert!(matches!(
        next_frame(&mut listener).await?,
        Frame::Fin { pipe_id: received } if received == pipe_id
    ));
    listener.send(Frame::Fin { pipe_id }).await?;
    assert!(matches!(
        next_frame(&mut connector).await?,
        Frame::Fin { pipe_id: received } if received == pipe_id
    ));

    gateway.stop().await?;
    Ok(())
}
