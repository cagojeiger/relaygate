use std::{
    error::Error,
    io::{self, Write},
    sync::{Arc, Mutex},
};

use relaygate_route_table::GatewayId;
use serde_json::Value;

use crate::peer::identity::PeerTransportId;

pub(super) type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
pub(super) type CapturedOutput = Arc<Mutex<Vec<u8>>>;

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

pub(super) fn captured_dispatch() -> (CapturedOutput, tracing::Dispatch) {
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

pub(super) fn assert_transport_lifecycle_event(
    output: &CapturedOutput,
    event: &str,
    peer_gateway_id: GatewayId,
    peer_transport_id: PeerTransportId,
    streams: usize,
) -> TestResult {
    let bytes = match output.lock() {
        Ok(output) => output.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    };
    let logs = String::from_utf8(bytes)?;
    let records = logs
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let matching = records
        .iter()
        .filter(|record| record["event"] == event)
        .collect::<Vec<_>>();
    assert_eq!(matching.len(), 1, "{logs}");
    let record = matching[0];

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
    assert_eq!(record["streams"], streams, "{record}");
    for forbidden in [
        "client_key",
        "internal_gateway_key",
        "payload",
        "application_data",
        "delivery_acknowledgement",
    ] {
        assert!(record.get(forbidden).is_none(), "{record}");
    }
    assert!(!logs.contains("key-a"), "{logs}");
    assert!(!logs.contains("key-b"), "{logs}");
    Ok(())
}
