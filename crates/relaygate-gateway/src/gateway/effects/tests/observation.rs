use std::{
    io::{self, Write},
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use relaygate_protocol::SessionId;
use relaygate_route_table::GatewayId;
use serde_json::Value;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::{
    GatewayPeerConfig,
    auth::ClientKeyStore,
    gateway::Inner,
    peer::{OpenIdentity, PeerHandle, PeerRuntime, PeerStreamKey, PeerTransportId},
    state::{GatewayLimits, GatewayState, PeerDelivery},
};

use super::super::ControlAction;

type CapturedOutput = Arc<Mutex<Vec<u8>>>;
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const INTERNAL_GATEWAY_SECRET: &str = "internal-gateway-secret-must-not-be-logged";
const PAYLOAD_SENTINEL: &str = "peer-payload-must-not-be-logged";

#[derive(Clone)]
struct CapturedWriter {
    output: CapturedOutput,
}

impl Write for CapturedWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        match self.output.lock() {
            Ok(mut output) => output.extend_from_slice(buffer),
            Err(poisoned) => poisoned.into_inner().extend_from_slice(buffer),
        }
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn peer_open_cancel_failure_reports_full_open_identity_without_secrets() -> TestResult {
    let gateway_id = GatewayId::new();
    let connector_session_id = SessionId::new();
    let open_identity = OpenIdentity::new(gateway_id, connector_session_id, 41);
    let config = GatewayPeerConfig::new("gateway-a", INTERNAL_GATEWAY_SECRET, [])?;
    let (peer, _events, runtime) =
        PeerRuntime::start(config, GatewayId::new(), CancellationToken::new())?;
    drop(runtime);
    let inner = test_inner(peer);
    let (output, dispatch) = captured_dispatch();
    let _guard = tracing::dispatcher::set_default(&dispatch);

    let actions = inner
        .run_control_effect(ControlAction::CancelPeerOpen { open_identity })
        .await;

    assert!(actions.is_empty());
    let (record, logs) = single_event(&output, "gateway.peer_open.cancel_failed")?;
    assert_eq!(record["component"], "gateway", "{record}");
    assert_eq!(
        record["entry_gateway_id"],
        gateway_id.as_uuid().to_string(),
        "{record}"
    );
    assert_eq!(
        record["connector_session_id"],
        connector_session_id.as_uuid().to_string(),
        "{record}"
    );
    assert_eq!(record["connection_id"], 41, "{record}");
    assert_eq!(record["error_code"], "Unavailable", "{record}");
    assert_eq!(record["observation"], "MaybeObserved", "{record}");
    assert_safe_event(&record, &logs);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn peer_frame_commit_failure_reports_full_stream_identity_without_payload() -> TestResult {
    let config = GatewayPeerConfig::new("gateway-a", INTERNAL_GATEWAY_SECRET, [])?;
    let (peer, _events, runtime) =
        PeerRuntime::start(config, GatewayId::new(), CancellationToken::new())?;
    drop(runtime);
    let inner = test_inner(peer);
    let peer_gateway_id = GatewayId::new();
    let peer_transport_id = PeerTransportId::new();
    let key = PeerStreamKey::for_test(peer_gateway_id, peer_transport_id, 18);
    let (output, dispatch) = captured_dispatch();
    let _guard = tracing::dispatcher::set_default(&dispatch);

    let actions = inner
        .send_peer_delivery(PeerDelivery::Data {
            key,
            payload: Bytes::from_static(PAYLOAD_SENTINEL.as_bytes()),
        })
        .await;

    assert!(actions.is_empty());
    let (record, logs) = single_event(&output, "gateway.peer_frame.commit_failed")?;
    assert_eq!(record["component"], "gateway", "{record}");
    assert_eq!(
        record["peer_gateway_id"],
        peer_gateway_id.as_uuid().to_string(),
        "{record}"
    );
    assert_eq!(
        record["peer_transport_id"],
        peer_transport_id.as_uuid().to_string(),
        "{record}"
    );
    assert_eq!(record["stream_id"], 18, "{record}");
    assert_eq!(record["error_code"], "Unavailable", "{record}");
    assert_eq!(record["observation"], "MaybeObserved", "{record}");
    assert_safe_event(&record, &logs);
    Ok(())
}

fn test_inner(peer: PeerHandle) -> Arc<Inner> {
    Arc::new(Inner {
        state: Mutex::new(GatewayState::new_distributed(
            ClientKeyStore::new(Default::default()),
            GatewayLimits::default(),
            GatewayId::new(),
        )),
        writer_queue_capacity: 8,
        max_frame_len: 64 * 1024,
        offer_timeout: Duration::from_secs(1),
        heartbeat_idle_interval: Duration::from_secs(60),
        heartbeat_response_timeout: Duration::from_secs(20),
        session_slots: Arc::new(Semaphore::new(8)),
        routing: None,
        peer: Some(peer),
        control_effects: None,
        distributed_runtime: Mutex::new(None),
        panic_next_session_after_admission: std::sync::atomic::AtomicBool::new(false),
    })
}

fn captured_dispatch() -> (CapturedOutput, tracing::Dispatch) {
    let output = Arc::new(Mutex::new(Vec::new()));
    let writer_output = Arc::clone(&output);
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            .without_time()
            .with_target(false)
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(move || CapturedWriter {
                output: Arc::clone(&writer_output),
            })
            .finish(),
    );
    (output, dispatch)
}

fn single_event(output: &CapturedOutput, event: &str) -> TestResult<(Value, String)> {
    let bytes = match output.lock() {
        Ok(output) => output.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    };
    let logs = String::from_utf8(bytes)?;
    let mut matching = logs
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|record| record["event"] == event);
    let record = matching.next().ok_or("missing structured event")?;
    assert!(matching.next().is_none(), "duplicate {event} event: {logs}");
    Ok((record, logs))
}

fn assert_safe_event(record: &Value, logs: &str) {
    for forbidden in [
        "client_key",
        "internal_gateway_key",
        "payload",
        "application_data",
        "delivery_acknowledgement",
    ] {
        assert!(record.get(forbidden).is_none(), "{record}");
    }
    assert!(!logs.contains(INTERNAL_GATEWAY_SECRET), "{logs}");
    assert!(!logs.contains(PAYLOAD_SENTINEL), "{logs}");
}
