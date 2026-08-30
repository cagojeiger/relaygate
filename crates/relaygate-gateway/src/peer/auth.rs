use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use super::{
    config::GatewayPeerConfig,
    identity::{PeerGatewayKey, PeerGatewayName},
};

#[derive(Clone)]
pub(super) struct TrustedPeers(Vec<TrustedPeerDigest>);

#[derive(Clone)]
struct TrustedPeerDigest {
    name: [u8; 32],
    key: [u8; 32],
}

impl TrustedPeers {
    #[must_use]
    pub(super) fn from_config(config: &GatewayPeerConfig) -> Self {
        Self(
            config
                .trusted_peers
                .iter()
                .map(|peer| TrustedPeerDigest {
                    name: digest(peer.gateway_name.as_str()),
                    key: digest(peer.internal_gateway_key.expose_secret()),
                })
                .collect(),
        )
    }

    pub(super) fn authenticate(&self, name: &PeerGatewayName, key: &PeerGatewayKey) -> bool {
        let candidate_name = digest(name.as_str());
        let candidate_key = digest(key.expose_secret());
        let mut authenticated = subtle::Choice::from(0);
        for candidate in &self.0 {
            authenticated |=
                candidate.name.ct_eq(&candidate_name) & candidate.key.ct_eq(&candidate_key);
        }
        bool::from(authenticated)
    }
}

fn digest(value: &str) -> [u8; 32] {
    Sha256::digest(value.as_bytes()).into()
}

impl std::fmt::Debug for TrustedPeers {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TrustedPeers")
            .field("gateway_count", &self.0.len())
            .finish_non_exhaustive()
    }
}
