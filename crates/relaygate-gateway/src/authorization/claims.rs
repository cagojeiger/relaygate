use relaygate_address::{DestinationName, NamespaceId, RouteAddress};
use serde::Deserialize;

const MAX_PERMISSIONS: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Action {
    Publish,
    Dial,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Claims {
    pub(crate) iss: String,
    pub(crate) aud: Audience,
    pub(crate) nbf: u64,
    pub(crate) exp: u64,
    #[serde(default)]
    permissions: Vec<Permission>,
}

impl Claims {
    pub(crate) fn registered_claims_are_sane(&self) -> bool {
        !self.iss.is_empty() && self.aud.is_non_empty() && self.nbf < self.exp
    }

    pub(crate) fn authorizes(&self, action: Action, address: &RouteAddress) -> bool {
        self.permissions.len() <= MAX_PERMISSIONS
            && self.permissions.iter().any(|permission| {
                permission.action == action
                    && permission.namespace == *address.namespace()
                    && permission.scope.contains(address.destination())
            })
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum Audience {
    One(String),
    Many(Vec<String>),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Permission {
    action: Action,
    namespace: NamespaceId,
    scope: DestinationScope,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
enum DestinationScope {
    Exact { destination: DestinationName },
    Subtree { destination: DestinationName },
    All,
}

impl DestinationScope {
    fn contains(&self, destination: &DestinationName) -> bool {
        match self {
            Self::Exact { destination: exact } => destination == exact,
            Self::Subtree {
                destination: prefix,
            } => destination == prefix || destination.is_descendant_of(prefix),
            Self::All => true,
        }
    }
}

impl Audience {
    fn is_non_empty(&self) -> bool {
        match self {
            Self::One(value) => !value.is_empty(),
            Self::Many(values) => {
                !values.is_empty() && values.iter().all(|value| !value.is_empty())
            }
        }
    }
}
