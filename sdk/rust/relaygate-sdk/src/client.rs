use std::sync::{Arc, Mutex as StdMutex, atomic::AtomicBool};

use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{ClientTlsConfig, Endpoint};
use uuid::Uuid;

use crate::{
    BindError, Config, ConnectError, Listener, OpenError, Pipe, SessionError,
    runtime::{
        BindingOperationGuard, BindingPending, MAX_ENDPOINT_BYTES, MAX_IDENTITY_BYTES,
        MAX_LISTENERS, MAX_OPEN_REQUESTS, OUTBOUND_CAPACITY, OpenGuard, PendingOpen, Shared,
        receive_responses, request, valid_text,
    },
    wire::{self, connect_request, connect_response},
};

/// Metadata for the authenticated stream. It never contains the API key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Session {
    pub(crate) client_session_id: String,
    pub(crate) client_id: String,
    pub(crate) api_key_id: String,
    pub(crate) auth_revision: String,
}

impl Session {
    pub fn client_session_id(&self) -> &str {
        &self.client_session_id
    }

    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    pub fn api_key_id(&self) -> &str {
        &self.api_key_id
    }

    pub fn auth_revision(&self) -> &str {
        &self.auth_revision
    }
}

/// One authenticated RelayGate stream.
pub struct Client {
    pub(crate) shared: Arc<Shared>,
}

impl Client {
    pub(crate) fn managed_handle(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Connects, authenticates with the first stream message, and waits for the
    /// authenticated session response before returning.
    pub async fn connect(config: Config) -> Result<Self, ConnectError> {
        config.validate()?;
        let Config {
            endpoint,
            client_id,
            api_key_id,
            api_key,
            insecure_local,
        } = config;

        let mut endpoint_builder = Endpoint::from_shared(endpoint)?;
        if !insecure_local {
            endpoint_builder =
                endpoint_builder.tls_config(ClientTlsConfig::new().with_native_roots())?;
        }
        let channel = endpoint_builder.connect().await?;
        let (outbound, inbound_requests) = mpsc::channel(OUTBOUND_CAPACITY);

        outbound
            .send(request(connect_request::Message::Authenticate(
                wire::Authenticate {
                    client_id: client_id.clone(),
                    api_key_id: api_key_id.clone(),
                    api_key,
                },
            )))
            .await
            .map_err(|_| ConnectError::Protocol("authentication request stream closed"))?;

        let mut relay = wire::relay_client::RelayClient::new(channel);
        let response = relay.connect(ReceiverStream::new(inbound_requests)).await?;
        let mut inbound = response.into_inner();
        let first = inbound.message().await?.ok_or(ConnectError::Protocol(
            "stream ended before authentication response",
        ))?;
        let opened = match first.message {
            Some(connect_response::Message::ClientSessionOpened(opened)) => opened,
            _ => {
                return Err(ConnectError::Protocol(
                    "first response was not ClientSessionOpened",
                ));
            }
        };
        let session_ref = opened.session.ok_or(ConnectError::Protocol(
            "ClientSessionOpened omitted session",
        ))?;
        if session_ref.client_session_id.is_empty()
            || session_ref.client_id != client_id
            || session_ref.api_key_id != api_key_id
        {
            return Err(ConnectError::Protocol(
                "ClientSessionOpened identity did not match authentication",
            ));
        }

        let shared = Arc::new(Shared::new(
            outbound,
            Session {
                client_session_id: session_ref.client_session_id,
                client_id: session_ref.client_id,
                api_key_id: session_ref.api_key_id,
                auth_revision: session_ref.auth_revision,
            },
        ));
        let weak = Arc::downgrade(&shared);
        let dispatcher = tokio::spawn(async move {
            receive_responses(weak, &mut inbound).await;
        });
        *shared.dispatcher.lock().expect("dispatcher lock poisoned") = Some(dispatcher);
        Ok(Self { shared })
    }

    pub fn session(&self) -> &Session {
        &self.shared.session
    }

    /// Resolves when the authenticated stream becomes terminal.
    pub async fn done(&self) -> SessionError {
        self.shared.wait_done().await
    }

    /// Terminates this stream and all listeners, pending operations, and Pipes.
    pub async fn close(self) {
        self.shared.terminate(SessionError::Closed);
        let dispatcher = self
            .shared
            .dispatcher
            .lock()
            .expect("dispatcher lock poisoned")
            .take();
        if let Some(dispatcher) = dispatcher {
            dispatcher.abort();
            let _ = dispatcher.await;
        }
    }

    /// Binds one literal endpoint pattern and exact target.
    pub async fn bind(
        &self,
        endpoint_pattern: impl Into<String>,
        target_id: impl Into<String>,
    ) -> Result<Listener, BindError> {
        let endpoint_pattern = endpoint_pattern.into();
        let target_id = target_id.into();
        if !valid_text(&endpoint_pattern, MAX_ENDPOINT_BYTES)
            || !valid_text(&target_id, MAX_IDENTITY_BYTES)
        {
            return Err(BindError::InvalidRequest);
        }
        let _lane = self.shared.binding_lane.lock().await;
        self.shared.ensure_active().map_err(BindError::Session)?;
        if self
            .shared
            .listeners
            .lock()
            .expect("listeners lock poisoned")
            .len()
            >= MAX_LISTENERS
        {
            return Err(BindError::CapacityReached);
        }

        let (tx, rx) = oneshot::channel();
        let operation_id = Uuid::new_v4().to_string();
        {
            let mut pending = self
                .shared
                .binding_pending
                .lock()
                .expect("binding pending lock poisoned");
            if pending.is_some() {
                return Err(BindError::OperationPending);
            }
            *pending = Some(BindingPending::Bind {
                operation_id: operation_id.clone(),
                endpoint_pattern: endpoint_pattern.clone(),
                target_id: target_id.clone(),
                response: tx,
            });
        }
        let mut guard = BindingOperationGuard {
            shared: Arc::downgrade(&self.shared),
            operation_id,
            sent: false,
            armed: true,
        };
        if let Err(error) = self
            .shared
            .send(request(connect_request::Message::BindListener(
                wire::BindListener {
                    endpoint_pattern,
                    target_id,
                },
            )))
            .await
        {
            return Err(BindError::Session(error));
        }
        guard.sent = true;
        let result = rx
            .await
            .unwrap_or_else(|_| Err(self.shared.terminal_or_transport().into()));
        guard.armed = false;
        result
    }

    /// Opens exactly one endpoint and target. Dropping this future issues a
    /// best-effort CancelOpen while retaining bounded correlation state for a
    /// possible late terminal result.
    pub async fn open(
        &self,
        endpoint: impl Into<String>,
        target_id: impl Into<String>,
    ) -> Result<Pipe, OpenError> {
        let endpoint = endpoint.into();
        let target_id = target_id.into();
        if !valid_text(&endpoint, MAX_ENDPOINT_BYTES) || !valid_text(&target_id, MAX_IDENTITY_BYTES)
        {
            return Err(OpenError::InvalidRequest);
        }
        self.shared.ensure_active()?;
        if !self.shared.reserve_pipe_slot() {
            return Err(OpenError::CapacityReached);
        }
        let request_id = Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        let pending = Arc::new(PendingOpen {
            request_id: request_id.clone(),
            endpoint: endpoint.clone(),
            target_id: target_id.clone(),
            response: StdMutex::new(Some(tx)),
            cancelled: AtomicBool::new(false),
            slot_reserved: AtomicBool::new(true),
        });
        {
            let mut opens = self.shared.opens.lock().expect("opens lock poisoned");
            if opens.len() >= MAX_OPEN_REQUESTS {
                pending.release_slot(&self.shared);
                return Err(OpenError::CapacityReached);
            }
            if opens.contains_key(&request_id) {
                pending.release_slot(&self.shared);
                return Err(OpenError::DuplicateInFlight);
            }
            opens.insert(request_id.clone(), Arc::clone(&pending));
        }
        let mut guard = OpenGuard {
            shared: Arc::downgrade(&self.shared),
            pending: Arc::clone(&pending),
            sent: false,
            armed: true,
        };
        if let Err(error) = self
            .shared
            .send(request(connect_request::Message::Open(wire::Open {
                request_id: request_id.clone(),
                endpoint,
                target_id,
            })))
            .await
        {
            self.shared.remove_open(&request_id);
            pending.release_slot(&self.shared);
            guard.armed = false;
            return Err(OpenError::Session(error));
        }
        guard.sent = true;
        let result = rx
            .await
            .unwrap_or_else(|_| Err(OpenError::Session(self.shared.terminal_or_transport())));
        guard.armed = false;
        result
    }
}
