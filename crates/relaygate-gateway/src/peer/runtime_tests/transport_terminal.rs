use std::{
    collections::BTreeSet,
    sync::{Arc, Barrier},
};

use relaygate_protocol::PeerObservation;

use super::*;

fn assert_transport_loss(
    event: PeerEvent,
    expected_peer_gateway_id: GatewayId,
    expected_transport_id: PeerTransportId,
    expected_streams: [PeerStreamKey; 2],
) -> TestResult {
    let PeerEvent::TransportLost {
        peer_gateway_id,
        peer_transport_id,
        streams,
    } = event
    else {
        return Err(format!("expected TransportLost, got {event:?}").into());
    };

    assert_eq!(peer_gateway_id, expected_peer_gateway_id);
    assert_eq!(peer_transport_id, expected_transport_id);
    assert_eq!(streams.len(), expected_streams.len());
    let actual_keys: BTreeSet<_> = streams.iter().map(|stream| stream.key).collect();
    let expected_keys: BTreeSet<_> = expected_streams.into_iter().collect();
    assert_eq!(actual_keys.len(), streams.len(), "duplicate lost stream");
    assert_eq!(actual_keys, expected_keys);
    assert!(
        streams.iter().all(|stream| {
            stream.progress.failure_observation() == PeerObservation::MaybeObserved
        })
    );
    Ok(())
}

#[tokio::test]
async fn simultaneous_force_close_emits_one_loss_per_endpoint_and_reconnects_lazily() -> TestResult
{
    let mut pair = RuntimePair::start().await?;

    let first_request = pair.request_a_to_b(1)?;
    let first_identity = first_request.open_identity();
    let first_open = {
        let handle = pair.handle_a.clone();
        tokio::spawn(async move { handle.open(first_request).await })
    };
    let (first_owner_key, first_incoming_identity) =
        accept_one(&mut pair.events_b, &pair.handle_b).await?;
    let first_entry_key = first_open.await??;
    let first_opened = next_event(&mut pair.events_a).await?;
    assert_eq!(first_incoming_identity, first_identity);
    assert!(matches!(
        first_opened,
        PeerEvent::Opened {
            key,
            open_identity,
        } if key == first_entry_key && open_identity == first_identity
    ));

    let second_request = pair.request_a_to_b(2)?;
    let second_identity = second_request.open_identity();
    let second_open = {
        let handle = pair.handle_a.clone();
        tokio::spawn(async move { handle.open(second_request).await })
    };
    let (second_owner_key, second_incoming_identity) =
        accept_one(&mut pair.events_b, &pair.handle_b).await?;
    let second_entry_key = second_open.await??;
    let second_opened = next_event(&mut pair.events_a).await?;
    assert_eq!(second_incoming_identity, second_identity);
    assert!(matches!(
        second_opened,
        PeerEvent::Opened {
            key,
            open_identity,
        } if key == second_entry_key && open_identity == second_identity
    ));

    let old_transport_id = first_entry_key.peer_transport_id();
    assert_eq!(second_entry_key.peer_transport_id(), old_transport_id);
    assert_eq!(first_owner_key.peer_transport_id(), old_transport_id);
    assert_eq!(second_owner_key.peer_transport_id(), old_transport_id);
    let active_counts = PeerCounts {
        connecting: 0,
        ready: 1,
        streams: 2,
    };
    wait_for_counts(&pair.handle_a, active_counts).await?;
    wait_for_counts(&pair.handle_b, active_counts).await?;

    // Block the single-threaded Tokio runtime while both OS threads cross the
    // barrier and force-close their local view of the same transport. When the
    // actors resume, local cancellation and peer EOF are both eligible terminal
    // causes; either may win, but each actor must publish exactly one loss.
    let barrier = Arc::new(Barrier::new(3));
    let close_a = {
        let barrier = Arc::clone(&barrier);
        let handle = pair.handle_a.clone();
        std::thread::spawn(move || {
            barrier.wait();
            handle.close_transport(first_entry_key)
        })
    };
    let close_b = {
        let barrier = Arc::clone(&barrier);
        let handle = pair.handle_b.clone();
        std::thread::spawn(move || {
            barrier.wait();
            handle.close_transport(first_owner_key)
        })
    };
    barrier.wait();
    assert!(
        close_a
            .join()
            .map_err(|_| "Gateway A close thread panicked")?
    );
    assert!(
        close_b
            .join()
            .map_err(|_| "Gateway B close thread panicked")?
    );

    assert_transport_loss(
        next_event(&mut pair.events_a).await?,
        pair.gateway_b,
        old_transport_id,
        [first_entry_key, second_entry_key],
    )?;
    assert_transport_loss(
        next_event(&mut pair.events_b).await?,
        pair.gateway_a,
        old_transport_id,
        [first_owner_key, second_owner_key],
    )?;
    wait_for_counts(&pair.handle_a, PeerCounts::default()).await?;
    wait_for_counts(&pair.handle_b, PeerCounts::default()).await?;
    assert!(!pair.handle_a.close_transport(first_entry_key));
    assert!(!pair.handle_a.close_transport(second_entry_key));
    assert!(!pair.handle_b.close_transport(first_owner_key));
    assert!(!pair.handle_b.close_transport(second_owner_key));

    // A fresh application OPEN is the FIFO sentinel for both event streams.
    // Any duplicate loss notice would be observed instead of IncomingOpen or
    // Opened, while this retry also proves reconnect is lazy and uses a new id.
    let retry_request = pair.request_a_to_b(3)?;
    let retry_identity = retry_request.open_identity();
    let retry_open = {
        let handle = pair.handle_a.clone();
        tokio::spawn(async move { handle.open(retry_request).await })
    };
    let (retry_owner_key, retry_incoming_identity) =
        accept_one(&mut pair.events_b, &pair.handle_b).await?;
    let retry_entry_key = retry_open.await??;
    assert_eq!(retry_incoming_identity, retry_identity);
    assert_ne!(retry_entry_key.peer_transport_id(), old_transport_id);
    assert_eq!(
        retry_owner_key.peer_transport_id(),
        retry_entry_key.peer_transport_id()
    );
    let retry_opened = next_event(&mut pair.events_a).await?;
    assert!(matches!(
        retry_opened,
        PeerEvent::Opened {
            key,
            open_identity,
        } if key == retry_entry_key && open_identity == retry_identity
    ));

    pair.handle_b.send_close(retry_owner_key).await?;
    let retry_closed = next_event(&mut pair.events_a).await?;
    assert!(matches!(retry_closed, PeerEvent::Close { key } if key == retry_entry_key));
    pair.shutdown().await
}
