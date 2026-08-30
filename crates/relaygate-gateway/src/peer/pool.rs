use std::collections::BTreeMap;

use relaygate_route_table::GatewayId;

use super::{error::PeerError, identity::PeerTransportId};

#[derive(Debug, Default)]
pub(crate) struct PeerPool {
    slots: BTreeMap<PeerSlotKey, PeerSlot>,
}

impl PeerPool {
    pub(crate) fn connect(
        &mut self,
        local_gateway: GatewayId,
        remote_gateway: GatewayId,
        transport_id: PeerTransportId,
    ) -> Result<(), PeerError> {
        let key = PeerSlotKey::new(local_gateway, remote_gateway);
        let slot = self.slots.entry(key).or_default();
        if !matches!(slot.state, PeerSlotState::Idle) {
            return Err(PeerError::AlreadyExists(
                "same-direction PeerTransport candidate already exists",
            ));
        }
        slot.state = PeerSlotState::Connecting(transport_id);
        Ok(())
    }

    #[must_use]
    pub(crate) fn ready_transport(&self, remote_gateway: GatewayId) -> Option<PeerTransportId> {
        self.slots
            .iter()
            .filter(|(key, _)| key.dialer == remote_gateway || key.acceptor == remote_gateway)
            .find_map(|(_, slot)| match slot.state {
                PeerSlotState::Ready(transport_id) => Some(transport_id),
                PeerSlotState::Idle | PeerSlotState::Connecting(_) => None,
            })
    }

    #[must_use]
    pub(crate) fn state_counts(&self) -> (usize, usize) {
        self.slots
            .values()
            .fold((0, 0), |(connecting, ready), slot| match slot.state {
                PeerSlotState::Idle => (connecting, ready),
                PeerSlotState::Connecting(_) => (connecting + 1, ready),
                PeerSlotState::Ready(_) => (connecting, ready + 1),
            })
    }

    pub(crate) fn ready(
        &mut self,
        local_gateway: GatewayId,
        remote_gateway: GatewayId,
        transport_id: PeerTransportId,
    ) -> Result<(), PeerError> {
        let key = PeerSlotKey::new(local_gateway, remote_gateway);
        let Some(slot) = self.slots.get_mut(&key) else {
            return Err(PeerError::FailedPrecondition(
                "PeerTransport candidate does not exist",
            ));
        };
        match slot.state {
            PeerSlotState::Connecting(current) if current == transport_id => {
                slot.state = PeerSlotState::Ready(transport_id);
                Ok(())
            }
            PeerSlotState::Idle | PeerSlotState::Connecting(_) | PeerSlotState::Ready(_) => {
                Err(PeerError::FailedPrecondition(
                    "PeerTransport candidate identity does not match current slot",
                ))
            }
        }
    }

    pub(crate) fn remove_transport(&mut self, transport_id: PeerTransportId) {
        self.slots
            .retain(|_, slot| slot.state.transport_id() != Some(transport_id));
    }

    #[cfg(test)]
    pub(crate) fn slot_count(&self) -> usize {
        self.slots.len()
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) fn ready_count_for_pair(&self, gateway_a: GatewayId, gateway_b: GatewayId) -> usize {
        [
            PeerSlotKey::new(gateway_a, gateway_b),
            PeerSlotKey::new(gateway_b, gateway_a),
        ]
        .into_iter()
        .filter(|key| {
            self.slots
                .get(key)
                .is_some_and(|slot| matches!(slot.state, PeerSlotState::Ready(_)))
        })
        .count()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct PeerSlotKey {
    dialer: GatewayId,
    acceptor: GatewayId,
}

impl PeerSlotKey {
    const fn new(dialer: GatewayId, acceptor: GatewayId) -> Self {
        Self { dialer, acceptor }
    }
}

#[derive(Debug, Default)]
struct PeerSlot {
    state: PeerSlotState,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum PeerSlotState {
    #[default]
    Idle,
    Connecting(PeerTransportId),
    Ready(PeerTransportId),
}

impl PeerSlotState {
    const fn transport_id(self) -> Option<PeerTransportId> {
        match self {
            Self::Idle => None,
            Self::Connecting(transport_id) | Self::Ready(transport_id) => Some(transport_id),
        }
    }
}
