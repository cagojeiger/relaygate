use std::time::Duration;

use relaygate_route_table::{
    BindingId, BindingSet, ClientId, GatewayId, GatewayLocator, LeaseId, ListenerSessionId,
    MappingEntry, MappingSnapshot, RegistrationAck, RegistrationKey, RegistrationRevision,
    RequestContext, RouteTableError, ShardDirectoryGeneration, ShardId,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::TransportError;

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "operation",
    rename_all = "SCREAMING_SNAKE_CASE",
    deny_unknown_fields
)]
pub(crate) enum WireRequest {
    Register {
        generation: String,
        key: WireRegistrationKey,
    },
    Update {
        generation: String,
        key: WireRegistrationKey,
        lease_id: String,
        revision: u64,
        snapshot: Vec<WireMappingEntry>,
    },
    KeepAlive {
        generation: String,
        key: WireRegistrationKey,
        lease_id: String,
    },
    Deregister {
        generation: String,
        key: WireRegistrationKey,
        lease_id: String,
    },
    Resolve {
        generation: String,
        client_id: String,
    },
}

impl WireRequest {
    pub(crate) const fn operation_name(&self) -> &'static str {
        match self {
            Self::Register { .. } => "register",
            Self::Update { .. } => "update",
            Self::KeepAlive { .. } => "keep_alive",
            Self::Deregister { .. } => "deregister",
            Self::Resolve { .. } => "resolve",
        }
    }

    pub(crate) fn validate_preconditions(
        &self,
        context: RequestContext,
        configured_generation: ShardDirectoryGeneration,
    ) -> Result<(), TransportError> {
        if let Some(claimed_gateway_id) = self.claimed_gateway_id() {
            let claimed_gateway_id = Uuid::parse_str(claimed_gateway_id)
                .ok()
                .map(GatewayId::from_uuid);
            if claimed_gateway_id != Some(context.authenticated_gateway_id().gateway_id()) {
                return Err(TransportError::new(
                    crate::ErrorCode::PermissionDenied,
                    "authenticated Gateway does not own the requested registration",
                ));
            }
        }

        let requested_generation = parse_generation(self.generation())?;
        if requested_generation != configured_generation {
            return Err(TransportError::new(
                crate::ErrorCode::FailedPrecondition,
                "operation is not valid for the current RouteTable state: ShardDirectoryGeneration mismatch",
            ));
        }
        Ok(())
    }

    pub(crate) fn register(generation: ShardDirectoryGeneration, key: &RegistrationKey) -> Self {
        Self::Register {
            generation: generation.to_string(),
            key: WireRegistrationKey::from_domain(key),
        }
    }

    pub(crate) fn update(
        generation: ShardDirectoryGeneration,
        key: &RegistrationKey,
        lease_id: LeaseId,
        revision: RegistrationRevision,
        snapshot: &MappingSnapshot,
    ) -> Self {
        Self::Update {
            generation: generation.to_string(),
            key: WireRegistrationKey::from_domain(key),
            lease_id: lease_id.to_string(),
            revision: revision.get(),
            snapshot: snapshot
                .entries()
                .map(WireMappingEntry::from_domain)
                .collect(),
        }
    }

    pub(crate) fn keep_alive(
        generation: ShardDirectoryGeneration,
        key: &RegistrationKey,
        lease_id: LeaseId,
    ) -> Self {
        Self::KeepAlive {
            generation: generation.to_string(),
            key: WireRegistrationKey::from_domain(key),
            lease_id: lease_id.to_string(),
        }
    }

    pub(crate) fn deregister(
        generation: ShardDirectoryGeneration,
        key: &RegistrationKey,
        lease_id: LeaseId,
    ) -> Self {
        Self::Deregister {
            generation: generation.to_string(),
            key: WireRegistrationKey::from_domain(key),
            lease_id: lease_id.to_string(),
        }
    }

    pub(crate) fn resolve(generation: ShardDirectoryGeneration, client_id: &ClientId) -> Self {
        Self::Resolve {
            generation: generation.to_string(),
            client_id: client_id.as_str().to_owned(),
        }
    }

    pub(crate) fn into_domain(self) -> Result<DomainRequest, TransportError> {
        match self {
            Self::Register { generation, key } => Ok(DomainRequest::Register {
                generation: parse_generation(&generation)?,
                key: key.into_domain()?,
            }),
            Self::Update {
                generation,
                key,
                lease_id,
                revision,
                snapshot,
            } => {
                let entries = snapshot
                    .into_iter()
                    .map(WireMappingEntry::into_domain)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(DomainRequest::Update {
                    generation: parse_generation(&generation)?,
                    key: key.into_domain()?,
                    lease_id: parse_uuid(&lease_id, "LeaseId").map(LeaseId::from_uuid)?,
                    revision: RegistrationRevision::new(revision)?,
                    snapshot: MappingSnapshot::new(entries)?,
                })
            }
            Self::KeepAlive {
                generation,
                key,
                lease_id,
            } => Ok(DomainRequest::KeepAlive {
                generation: parse_generation(&generation)?,
                key: key.into_domain()?,
                lease_id: parse_uuid(&lease_id, "LeaseId").map(LeaseId::from_uuid)?,
            }),
            Self::Deregister {
                generation,
                key,
                lease_id,
            } => Ok(DomainRequest::Deregister {
                generation: parse_generation(&generation)?,
                key: key.into_domain()?,
                lease_id: parse_uuid(&lease_id, "LeaseId").map(LeaseId::from_uuid)?,
            }),
            Self::Resolve {
                generation,
                client_id,
            } => Ok(DomainRequest::Resolve {
                generation: parse_generation(&generation)?,
                client_id: ClientId::new(client_id)?,
            }),
        }
    }

    fn generation(&self) -> &str {
        match self {
            Self::Register { generation, .. }
            | Self::Update { generation, .. }
            | Self::KeepAlive { generation, .. }
            | Self::Deregister { generation, .. }
            | Self::Resolve { generation, .. } => generation,
        }
    }

    fn claimed_gateway_id(&self) -> Option<&str> {
        match self {
            Self::Register { key, .. }
            | Self::Update { key, .. }
            | Self::KeepAlive { key, .. }
            | Self::Deregister { key, .. } => Some(&key.gateway_id),
            Self::Resolve { .. } => None,
        }
    }
}

pub(crate) enum DomainRequest {
    Register {
        generation: ShardDirectoryGeneration,
        key: RegistrationKey,
    },
    Update {
        generation: ShardDirectoryGeneration,
        key: RegistrationKey,
        lease_id: LeaseId,
        revision: RegistrationRevision,
        snapshot: MappingSnapshot,
    },
    KeepAlive {
        generation: ShardDirectoryGeneration,
        key: RegistrationKey,
        lease_id: LeaseId,
    },
    Deregister {
        generation: ShardDirectoryGeneration,
        key: RegistrationKey,
        lease_id: LeaseId,
    },
    Resolve {
        generation: ShardDirectoryGeneration,
        client_id: ClientId,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "operation",
    rename_all = "SCREAMING_SNAKE_CASE",
    deny_unknown_fields
)]
pub(crate) enum WireResponse {
    Registered { ack: WireRegistrationAck },
    Updated { ack: WireRegistrationAck },
    KeptAlive { ack: WireRegistrationAck },
    Deregistered,
    Resolved { entries: Vec<WireMappingEntry> },
}

impl WireResponse {
    pub(crate) fn registered(ack: RegistrationAck) -> Self {
        Self::Registered {
            ack: WireRegistrationAck::from_domain(ack),
        }
    }

    pub(crate) fn updated(ack: RegistrationAck) -> Self {
        Self::Updated {
            ack: WireRegistrationAck::from_domain(ack),
        }
    }

    pub(crate) fn kept_alive(ack: RegistrationAck) -> Self {
        Self::KeptAlive {
            ack: WireRegistrationAck::from_domain(ack),
        }
    }

    pub(crate) fn resolved(bindings: &BindingSet) -> Self {
        Self::Resolved {
            entries: bindings
                .entries()
                .iter()
                .map(WireMappingEntry::from_domain)
                .collect(),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WireRegistrationKey {
    gateway_id: String,
    listener_session_id: String,
    shard_id: String,
}

impl WireRegistrationKey {
    fn from_domain(key: &RegistrationKey) -> Self {
        Self {
            gateway_id: key.gateway_id().to_string(),
            listener_session_id: key.listener_session_id().to_string(),
            shard_id: key.shard_id().as_str().to_owned(),
        }
    }

    fn into_domain(self) -> Result<RegistrationKey, TransportError> {
        Ok(RegistrationKey::new(
            parse_uuid(&self.gateway_id, "GatewayId").map(GatewayId::from_uuid)?,
            parse_uuid(&self.listener_session_id, "ListenerSessionId")
                .map(ListenerSessionId::from_uuid)?,
            ShardId::new(self.shard_id)?,
        ))
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WireMappingEntry {
    client_id: String,
    gateway_id: String,
    listener_session_id: String,
    binding_id: String,
    gateway_locator: String,
}

impl WireMappingEntry {
    fn from_domain(entry: &MappingEntry) -> Self {
        let identity = entry.identity();
        Self {
            client_id: entry.client_id().as_str().to_owned(),
            gateway_id: identity.gateway_id().to_string(),
            listener_session_id: identity.listener_session_id().to_string(),
            binding_id: identity.binding_id().to_string(),
            gateway_locator: entry.gateway_locator().as_str().to_owned(),
        }
    }

    fn into_domain(self) -> Result<MappingEntry, TransportError> {
        Ok(MappingEntry::new(
            ClientId::new(self.client_id)?,
            parse_uuid(&self.gateway_id, "GatewayId").map(GatewayId::from_uuid)?,
            parse_uuid(&self.listener_session_id, "ListenerSessionId")
                .map(ListenerSessionId::from_uuid)?,
            parse_uuid(&self.binding_id, "BindingId").map(BindingId::from_uuid)?,
            GatewayLocator::new(self.gateway_locator)?,
        ))
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WireRegistrationAck {
    lease_id: String,
    accepted_revision: Option<u64>,
    expires_in: WireDuration,
}

impl WireRegistrationAck {
    fn from_domain(ack: RegistrationAck) -> Self {
        Self {
            lease_id: ack.lease_id().to_string(),
            accepted_revision: ack.accepted_revision().map(RegistrationRevision::get),
            expires_in: WireDuration::from_domain(ack.expires_in()),
        }
    }

    pub(crate) fn into_domain(self) -> Result<RegistrationAck, TransportError> {
        let lease_id = parse_response_uuid(&self.lease_id, "LeaseId").map(LeaseId::from_uuid)?;
        let accepted_revision = self
            .accepted_revision
            .map(|revision| {
                RegistrationRevision::new(revision).map_err(|error| {
                    TransportError::protocol(format!(
                        "invalid RegistrationRevision in RouteTable response: {error}"
                    ))
                })
            })
            .transpose()?;
        let expires_in = self.expires_in.into_domain()?;
        Ok(RegistrationAck::from_parts(
            lease_id,
            accepted_revision,
            expires_in,
        ))
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireDuration {
    seconds: u64,
    nanoseconds: u32,
}

impl WireDuration {
    fn from_domain(duration: Duration) -> Self {
        Self {
            seconds: duration.as_secs(),
            nanoseconds: duration.subsec_nanos(),
        }
    }

    fn into_domain(self) -> Result<Duration, TransportError> {
        if self.nanoseconds >= 1_000_000_000 {
            return Err(TransportError::protocol(
                "invalid duration in RouteTable response",
            ));
        }
        Ok(Duration::new(self.seconds, self.nanoseconds))
    }
}

pub(crate) fn response_registration_ack(
    response: WireResponse,
    expected: &'static str,
    expected_lease_id: Option<LeaseId>,
    expected_revision: Option<RegistrationRevision>,
) -> Result<RegistrationAck, TransportError> {
    let ack = match response {
        WireResponse::Registered { ack } if expected == "REGISTER" => ack,
        WireResponse::Updated { ack } if expected == "UPDATE" => ack,
        WireResponse::KeptAlive { ack } if expected == "KEEP_ALIVE" => ack,
        _ => {
            return Err(TransportError::protocol(format!(
                "RouteTable response does not match {expected} request"
            )));
        }
    };
    let ack = ack.into_domain()?;
    if expected_lease_id.is_some_and(|lease_id| ack.lease_id() != lease_id) {
        return Err(TransportError::protocol(format!(
            "RouteTable {expected} response has a mismatched LeaseId"
        )));
    }
    if expected_revision.is_some_and(|revision| ack.accepted_revision() != Some(revision)) {
        return Err(TransportError::protocol(format!(
            "RouteTable {expected} response has a mismatched RegistrationRevision"
        )));
    }
    Ok(ack)
}

pub(crate) fn response_deregistered(response: WireResponse) -> Result<(), TransportError> {
    if matches!(response, WireResponse::Deregistered) {
        Ok(())
    } else {
        Err(TransportError::protocol(
            "RouteTable response does not match DEREGISTER request",
        ))
    }
}

pub(crate) fn response_bindings(
    response: WireResponse,
    expected_client_id: &ClientId,
) -> Result<BindingSet, TransportError> {
    let WireResponse::Resolved { entries } = response else {
        return Err(TransportError::protocol(
            "RouteTable response does not match RESOLVE request",
        ));
    };
    let entries = entries
        .into_iter()
        .map(|entry| {
            entry.into_domain().map_err(|error| {
                TransportError::protocol(format!(
                    "invalid MappingEntry in RouteTable response: {error}"
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let bindings = BindingSet::from_entries(entries).map_err(|error| {
        TransportError::protocol(format!(
            "invalid BindingSet in RouteTable response: {error}"
        ))
    })?;
    if bindings.entries()[0].client_id() != expected_client_id {
        return Err(TransportError::protocol(
            "RouteTable Resolve response has a mismatched ClientId",
        ));
    }
    Ok(bindings)
}

fn parse_generation(value: &str) -> Result<ShardDirectoryGeneration, TransportError> {
    if value.len() != 64 || !value.is_ascii() {
        return Err(TransportError::invalid_argument(
            "ShardDirectoryGeneration must be 64 hexadecimal characters",
        ));
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_value(pair[0]).ok_or_else(|| {
            TransportError::invalid_argument(
                "ShardDirectoryGeneration must contain only hexadecimal characters",
            )
        })?;
        let low = hex_value(pair[1]).ok_or_else(|| {
            TransportError::invalid_argument(
                "ShardDirectoryGeneration must contain only hexadecimal characters",
            )
        })?;
        bytes[index] = (high << 4) | low;
    }
    Ok(ShardDirectoryGeneration::from_bytes(bytes))
}

const fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn parse_uuid(value: &str, field: &'static str) -> Result<Uuid, TransportError> {
    Uuid::parse_str(value).map_err(|_| {
        TransportError::from(RouteTableError::InvalidArgument(format!(
            "{field} must be a UUID"
        )))
    })
}

fn parse_response_uuid(value: &str, field: &'static str) -> Result<Uuid, TransportError> {
    Uuid::parse_str(value)
        .map_err(|_| TransportError::protocol(format!("invalid {field} in RouteTable response")))
}

#[cfg(test)]
mod tests {
    use relaygate_route_table::{AuthenticatedGatewayId, RequestContext};

    use super::*;
    use crate::ErrorCode;

    #[test]
    fn owner_mismatch_precedes_malformed_later_fields() {
        let authenticated = GatewayId::from_uuid(Uuid::from_u128(1));
        let request = WireRequest::Update {
            generation: "not-hex".to_owned(),
            key: WireRegistrationKey {
                gateway_id: Uuid::from_u128(2).to_string(),
                listener_session_id: "not-a-uuid".to_owned(),
                shard_id: String::new(),
            },
            lease_id: "not-a-uuid".to_owned(),
            revision: 0,
            snapshot: Vec::new(),
        };

        let error = request
            .validate_preconditions(
                RequestContext::new(AuthenticatedGatewayId::from_verified_transport(
                    authenticated,
                )),
                ShardDirectoryGeneration::from_bytes([1; 32]),
            )
            .err();
        assert_eq!(
            error.map(|error| error.code()),
            Some(ErrorCode::PermissionDenied)
        );
    }

    #[test]
    fn generation_mismatch_precedes_malformed_operation_fields() {
        let authenticated = GatewayId::from_uuid(Uuid::from_u128(1));
        let request = WireRequest::KeepAlive {
            generation: ShardDirectoryGeneration::from_bytes([2; 32]).to_string(),
            key: WireRegistrationKey {
                gateway_id: authenticated.to_string(),
                listener_session_id: "not-a-uuid".to_owned(),
                shard_id: String::new(),
            },
            lease_id: "not-a-uuid".to_owned(),
        };

        let error = request
            .validate_preconditions(
                RequestContext::new(AuthenticatedGatewayId::from_verified_transport(
                    authenticated,
                )),
                ShardDirectoryGeneration::from_bytes([1; 32]),
            )
            .err();
        assert_eq!(
            error.map(|error| error.code()),
            Some(ErrorCode::FailedPrecondition)
        );
    }

    #[test]
    fn resolve_generation_precedes_client_id_validation() {
        let authenticated = GatewayId::from_uuid(Uuid::from_u128(1));
        let request = WireRequest::Resolve {
            generation: ShardDirectoryGeneration::from_bytes([2; 32]).to_string(),
            client_id: String::new(),
        };

        let error = request
            .validate_preconditions(
                RequestContext::new(AuthenticatedGatewayId::from_verified_transport(
                    authenticated,
                )),
                ShardDirectoryGeneration::from_bytes([1; 32]),
            )
            .err();
        assert_eq!(
            error.map(|error| error.code()),
            Some(ErrorCode::FailedPrecondition)
        );
    }
}
