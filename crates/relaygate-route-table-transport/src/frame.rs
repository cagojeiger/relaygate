use serde::{Deserialize, Serialize};

use crate::{
    ErrorCode,
    dto::{WireRequest, WireResponse},
};

pub(crate) const GATEWAY_ROLE: &str = "gateway";
pub(crate) const ROUTE_TABLE_ROLE: &str = "route_table";

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "frame",
    rename_all = "SCREAMING_SNAKE_CASE",
    deny_unknown_fields
)]
pub(crate) enum WireFrame {
    Hello {
        role: String,
        gateway_name: String,
        gateway_id: String,
        internal_gateway_key: String,
    },
    Welcome {
        role: String,
    },
    HandshakeRejected {
        role: String,
        code: ErrorCode,
        message: String,
    },
    Request {
        role: String,
        request_id: u64,
        request: WireRequest,
    },
    Response {
        role: String,
        request_id: u64,
        result: WireResult,
    },
    ProtocolFault {
        role: String,
        code: ErrorCode,
        message: String,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "status",
    rename_all = "SCREAMING_SNAKE_CASE",
    deny_unknown_fields
)]
pub(crate) enum WireResult {
    Ok { response: WireResponse },
    Error { code: ErrorCode, message: String },
}
