#[allow(dead_code)]
mod support;

use std::{
    io::{self, Write},
    sync::{Arc, Mutex},
    time::Duration,
};

use relaygate_sdk::{Config, Connector, ErrorCode, ListenerRuntime, PeerObservation};
use serde_json::Value;
use tokio::{
    sync::watch,
    time::{Instant, timeout, timeout_at},
};

use support::{TestGateway, TestResult};

const CLIENT_ID: &str = "observability.alpha";
const MISSING_CLIENT_ID: &str = "observability.missing";
const CLIENT_KEY_SENTINEL: &str = "client-key-sentinel-containing-internal-gateway-key-sentinel";
const REJECTED_CLIENT_KEY_SENTINEL: &str = "rejected-client-key-sentinel";
const INTERNAL_GATEWAY_KEY_SENTINEL: &str = "internal-gateway-key-sentinel";
const PAYLOAD_SENTINEL: &[u8] = b"application-payload-sentinel";
const RESET_MESSAGE_SENTINEL: &str = "counterpart session disconnected";

#[derive(Clone)]
struct CapturedLogs {
    inner: Arc<CapturedLogsInner>,
}

struct CapturedLogsInner {
    bytes: Mutex<Vec<u8>>,
    version: watch::Sender<u64>,
}

#[derive(Clone)]
struct CapturedWriter {
    logs: CapturedLogs,
}

impl Write for CapturedWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        match self.logs.inner.bytes.lock() {
            Ok(mut bytes) => bytes.extend_from_slice(buffer),
            Err(poisoned) => poisoned.into_inner().extend_from_slice(buffer),
        }
        self.logs
            .inner
            .version
            .send_modify(|version| *version = version.wrapping_add(1));
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn sdk_gateway_lifecycle_events_preserve_identity_and_hide_wire_contents() -> TestResult {
    let (logs, dispatch) = captured_dispatch();
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let gateway = TestGateway::start(&[(CLIENT_ID, CLIENT_KEY_SENTINEL)]).await?;
    let config = Config::new(gateway.address.to_string())
        .with_operation_timeout(Duration::from_secs(1))
        .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20));

    let rejected_runtime = ListenerRuntime::connect(config.clone()).await?;
    let registration_error = rejected_runtime
        .listen(CLIENT_ID, REJECTED_CLIENT_KEY_SENTINEL)
        .await
        .err()
        .ok_or_else(|| io::Error::other("registration with an invalid ClientKey succeeded"))?;
    assert_eq!(registration_error.code(), ErrorCode::Unauthenticated);
    assert_eq!(
        registration_error.observation(),
        PeerObservation::NotObserved
    );
    let rejected_registration = logs
        .wait_for("Gateway Listener registration rejection", |record| {
            is_event(record, "gateway.listener.registration_rejected")
                && string_field(record, "client_id") == Some(CLIENT_ID)
                && string_field(record, "error_code") == Some("Unauthenticated")
        })
        .await?;
    assert_eq!(rejected_registration["component"], "gateway");
    required_string(&rejected_registration, "session_id")?;
    assert!(rejected_registration["request_id"].is_number());
    assert!(rejected_registration.get("observation").is_none());
    rejected_runtime.close();

    let listener_runtime = ListenerRuntime::connect(config.clone()).await?;
    let listener = listener_runtime
        .listen(CLIENT_ID, CLIENT_KEY_SENTINEL)
        .await?;
    let gateway_registered = logs
        .wait_for("Gateway Listener registration", |record| {
            is_event(record, "gateway.listener.registered")
                && string_field(record, "client_id") == Some(CLIENT_ID)
        })
        .await?;
    let sdk_registration = logs
        .wait_for("SDK Listener registration", |record| {
            is_event(record, "sdk.listener_registration.active")
                && string_field(record, "client_id") == Some(CLIENT_ID)
        })
        .await?;
    assert_eq!(gateway_registered["component"], "gateway");
    assert_eq!(sdk_registration["component"], "sdk");
    assert_same_fields(
        &gateway_registered,
        &sdk_registration,
        &["session_id", "request_id", "client_id", "binding_id"],
    );

    let listener_session_id = required_string(&gateway_registered, "session_id")?;
    let binding_id = required_string(&gateway_registered, "binding_id")?;
    let connector = Connector::connect(config).await?;
    let connector_ready = logs
        .wait_for("SDK Connector session", |record| {
            is_event(record, "sdk.session.ready")
                && string_field(record, "role") == Some("connector")
        })
        .await?;
    let connector_session_id = required_string(&connector_ready, "session_id")?;

    let open_error = connector
        .open(MISSING_CLIENT_ID)
        .await
        .err()
        .ok_or_else(|| io::Error::other("OPEN for an unknown ClientId unexpectedly succeeded"))?;
    assert_eq!(open_error.code(), ErrorCode::NotFound);
    assert_eq!(open_error.observation(), PeerObservation::NotObserved);
    let error_code = format!("{:?}", open_error.code());
    let observation = format!("{:?}", open_error.observation());
    let gateway_open_failed = logs
        .wait_for("Gateway OPEN failure", |record| {
            is_event(record, "gateway.open.failed")
                && string_field(record, "connector_session_id")
                    == Some(connector_session_id.as_str())
                && string_field(record, "error_code") == Some(error_code.as_str())
                && string_field(record, "observation") == Some(observation.as_str())
        })
        .await?;
    let failed_connection_id = gateway_open_failed["connection_id"].clone();
    let sdk_open_failed = logs
        .wait_for("SDK OPEN failure", |record| {
            is_event(record, "sdk.open.failed")
                && record["connection_id"] == failed_connection_id
                && string_field(record, "connector_session_id")
                    == Some(connector_session_id.as_str())
        })
        .await?;
    assert_same_fields(
        &gateway_open_failed,
        &sdk_open_failed,
        &[
            "connector_session_id",
            "connection_id",
            "error_code",
            "observation",
        ],
    );

    let mut connector_pipe = connector.open(CLIENT_ID).await?;
    let mut listener_pipe = timeout(Duration::from_secs(2), listener.accept()).await??;
    let sdk_opened = logs
        .wait_for("SDK successful OPEN", |record| {
            is_event(record, "sdk.open.succeeded")
                && string_field(record, "connector_session_id")
                    == Some(connector_session_id.as_str())
                && record["connection_id"] != failed_connection_id
        })
        .await?;
    let opened_connection_id = sdk_opened["connection_id"].clone();
    let gateway_opened = logs
        .wait_for("Gateway opened Pipe", |record| {
            is_event(record, "gateway.pipe.opened")
                && string_field(record, "connector_session_id")
                    == Some(connector_session_id.as_str())
                && record["connection_id"] == opened_connection_id
        })
        .await?;
    let sdk_admitted = logs
        .wait_for("SDK admitted Pipe", |record| {
            is_event(record, "sdk.pipe.admitted")
                && string_field(record, "connector_session_id")
                    == Some(connector_session_id.as_str())
                && record["connection_id"] == opened_connection_id
        })
        .await?;
    assert_eq!(gateway_opened["listener_session_id"], listener_session_id);
    assert_eq!(gateway_opened["binding_id"], binding_id);
    assert_eq!(sdk_admitted["binding_id"], binding_id);
    assert_same_fields(
        &gateway_opened,
        &sdk_admitted,
        &["connector_session_id", "connection_id", "binding_id"],
    );

    connector_pipe.write_all_bytes(PAYLOAD_SENTINEL).await?;
    let mut received = vec![0_u8; PAYLOAD_SENTINEL.len()];
    let read = listener_pipe.read_into(&mut received).await?;
    assert_eq!(read, PAYLOAD_SENTINEL.len());
    assert_eq!(received, PAYLOAD_SENTINEL);

    listener.close().await?;
    let gateway_unregistered = logs
        .wait_for("Gateway Listener unregistration", |record| {
            is_event(record, "gateway.listener.unregistered")
                && string_field(record, "session_id") == Some(listener_session_id.as_str())
                && string_field(record, "binding_id") == Some(binding_id.as_str())
        })
        .await?;
    assert_eq!(gateway_unregistered["removed"], true);

    listener_runtime.close();
    let mut byte = [0_u8; 1];
    let terminal_error = timeout(Duration::from_secs(2), connector_pipe.read_into(&mut byte))
        .await?
        .err()
        .ok_or_else(|| {
            io::Error::other("Connector Pipe did not fail after ListenerSession loss")
        })?;
    let terminal_error_code = format!("{:?}", terminal_error.code());
    let terminal_observation = format!("{:?}", terminal_error.observation());
    let terminal = logs
        .wait_for("SDK Pipe terminal failure", |record| {
            is_event(record, "sdk.pipe.terminal")
                && string_field(record, "connector_session_id")
                    == Some(connector_session_id.as_str())
                && record["connection_id"] == opened_connection_id
                && string_field(record, "outcome") == Some("failed")
                && string_field(record, "error_code") == Some(terminal_error_code.as_str())
                && string_field(record, "observation") == Some(terminal_observation.as_str())
        })
        .await?;
    assert_eq!(terminal["error_code"], terminal_error_code);
    assert_eq!(terminal["observation"], terminal_observation);

    let sdk_session_ended = logs
        .wait_for("SDK Listener session end", |record| {
            is_event(record, "sdk.session.ended")
                && string_field(record, "role") == Some("listener")
                && string_field(record, "session_id") == Some(listener_session_id.as_str())
        })
        .await?;
    let gateway_session_removed = logs
        .wait_for("Gateway Listener session removal", |record| {
            is_event(record, "gateway.session.removed")
                && string_field(record, "role") == Some("listener")
                && string_field(record, "session_id") == Some(listener_session_id.as_str())
        })
        .await?;
    assert_same_fields(
        &sdk_session_ended,
        &gateway_session_removed,
        &["session_id", "role"],
    );

    connector.close();
    drop(listener_pipe);
    gateway.stop().await?;

    assert_safe_logs(&logs)?;
    Ok(())
}

impl CapturedLogs {
    fn text(&self) -> TestResult<String> {
        let bytes = match self.inner.bytes.lock() {
            Ok(bytes) => bytes.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        Ok(String::from_utf8(bytes)?)
    }

    fn records(&self) -> TestResult<Vec<Value>> {
        self.text()?
            .lines()
            .filter(|line| !line.is_empty())
            .map(serde_json::from_str)
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    async fn wait_for(
        &self,
        description: &str,
        predicate: impl Fn(&Value) -> bool,
    ) -> TestResult<Value> {
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut version = self.inner.version.subscribe();
        loop {
            if let Some(record) = self.records()?.into_iter().find(|record| predicate(record)) {
                return Ok(record);
            }
            match timeout_at(deadline, version.changed()).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    return Err(io::Error::other(format!(
                        "captured log stream ended before {description}"
                    ))
                    .into());
                }
                Err(_) => {
                    return Err(io::Error::other(format!(
                        "timed out waiting for {description}; captured logs: {}",
                        self.text()?
                    ))
                    .into());
                }
            }
        }
    }
}

fn captured_dispatch() -> (CapturedLogs, tracing::Dispatch) {
    let (version, _) = watch::channel(0);
    let logs = CapturedLogs {
        inner: Arc::new(CapturedLogsInner {
            bytes: Mutex::new(Vec::new()),
            version,
        }),
    };
    let writer_logs = logs.clone();
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            .without_time()
            .with_target(false)
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(move || CapturedWriter {
                logs: writer_logs.clone(),
            })
            .finish(),
    );
    (logs, dispatch)
}

fn is_event(record: &Value, event: &str) -> bool {
    string_field(record, "event") == Some(event)
}

fn string_field<'a>(record: &'a Value, field: &str) -> Option<&'a str> {
    record.get(field).and_then(Value::as_str)
}

fn required_string(record: &Value, field: &str) -> TestResult<String> {
    string_field(record, field)
        .map(str::to_owned)
        .ok_or_else(|| io::Error::other(format!("missing string field {field}: {record}")).into())
}

fn assert_same_fields(first: &Value, second: &Value, fields: &[&str]) {
    for field in fields {
        assert_eq!(first[*field], second[*field], "field {field}");
    }
}

fn assert_safe_logs(logs: &CapturedLogs) -> TestResult {
    let text = logs.text()?;
    for sentinel in [
        CLIENT_KEY_SENTINEL,
        REJECTED_CLIENT_KEY_SENTINEL,
        INTERNAL_GATEWAY_KEY_SENTINEL,
        std::str::from_utf8(PAYLOAD_SENTINEL)?,
        RESET_MESSAGE_SENTINEL,
    ] {
        assert!(
            !text.contains(sentinel),
            "secret or wire content leaked: {text}"
        );
    }
    for record in logs.records()? {
        for forbidden in [
            "client_key",
            "internal_gateway_key",
            "payload",
            "application_data",
            "delivery_acknowledgement",
        ] {
            assert!(record.get(forbidden).is_none(), "{record}");
        }
    }
    Ok(())
}
