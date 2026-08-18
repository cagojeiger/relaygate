use thiserror::Error;

/// A terminal authenticated-session failure.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum SessionError {
    #[error("the client was closed")]
    Closed,
    #[error("relay transport ended: {0}")]
    Transport(String),
    #[error("Relay.Connect ended with gRPC {code}: {message}")]
    Rpc { code: tonic::Code, message: String },
    #[error("relay protocol violation: {0}")]
    Protocol(&'static str),
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ConnectError {
    #[error("invalid RelayGate configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("failed to configure or connect transport: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("Relay.Connect failed: {0}")]
    Rpc(#[from] tonic::Status),
    #[error("relay protocol violation: {0}")]
    Protocol(&'static str),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum OpenFailure {
    InvalidRequest,
    RouteNotFound,
    Unavailable,
    CapacityReached,
    ListenerRejected,
    DeadlineExceeded,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum OpenError {
    #[error("invalid exact-target Open request")]
    InvalidRequest,
    #[error("too many SDK operations are in flight")]
    CapacityReached,
    #[error("Open failed: {0:?}")]
    Failed(OpenFailure),
    #[error("Open was stably cancelled before acceptance")]
    Cancelled,
    #[error("Open outcome is unknown; retrying creates a new operation")]
    Unknown,
    #[error("the generated request ID was already in flight")]
    DuplicateInFlight,
    #[error(transparent)]
    Session(#[from] SessionError),
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum BindError {
    #[error("invalid listener binding")]
    InvalidRequest,
    #[error("a bind or unbind operation is already awaiting its wire response")]
    OperationPending,
    #[error("listener capacity reached")]
    CapacityReached,
    #[error("another live binding owns the requested route")]
    Conflict,
    #[error("the binding authority is temporarily unavailable")]
    Unavailable,
    #[error(transparent)]
    Session(#[from] SessionError),
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum UnbindError {
    #[error("invalid listener unbind request")]
    InvalidRequest,
    #[error("another bind or unbind operation is awaiting its wire response")]
    OperationPending,
    #[error("listener capacity reached")]
    CapacityReached,
    #[error("the listener binding conflicts with current ownership")]
    Conflict,
    #[error("the binding authority is temporarily unavailable")]
    Unavailable,
    #[error(transparent)]
    Session(#[from] SessionError),
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum AcceptError {
    #[error("the offer is no longer pending")]
    NotPending,
    #[error("pipe capacity reached")]
    CapacityReached,
    #[error(transparent)]
    Session(#[from] SessionError),
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum RejectError {
    #[error("the offer is no longer pending")]
    NotPending,
    #[error(transparent)]
    Session(#[from] SessionError),
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum PipeError {
    #[error("payload must contain 1 through 60 KiB")]
    InvalidPayload,
    #[error("the Pipe is not owned by this session")]
    NotOwned,
    #[error("local payload backpressure was exhausted")]
    Backpressure,
    #[error("the relay rejected a payload as unavailable")]
    Unavailable,
    #[error("the Pipe is terminal")]
    Terminal,
    #[error(transparent)]
    Session(#[from] SessionError),
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum CloseError {
    #[error("the Pipe is not owned by this session")]
    NotOwned,
    #[error("a close is already pending for this Pipe")]
    AlreadyPending,
    #[error(transparent)]
    Session(#[from] SessionError),
}

/// A connection-supervision or current-session operation failure.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum ManagedError {
    #[error("the managed client is closed")]
    Closed,
    #[error("the managed client has no Ready session")]
    NotReady,
    #[error("the managed listener already exists")]
    BindingExists,
    #[error("managed listener capacity reached")]
    CapacityReached,
    #[error("managed connection failed permanently: {0}")]
    Failed(String),
    #[error(transparent)]
    Bind(#[from] BindError),
    #[error(transparent)]
    Open(#[from] OpenError),
    #[error(transparent)]
    Session(#[from] SessionError),
    #[error(transparent)]
    Unbind(#[from] UnbindError),
}
