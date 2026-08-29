use std::collections::HashMap;

use relaygate_protocol::ClientKey;
use subtle::ConstantTimeEq;

pub(crate) struct ClientKeyStore {
    keys: HashMap<String, String>,
}

impl ClientKeyStore {
    pub(crate) fn new(keys: HashMap<String, String>) -> Self {
        Self { keys }
    }

    pub(crate) fn authorizes(&self, client_id: &str, presented: &ClientKey) -> bool {
        let Some(expected) = self.keys.get(client_id) else {
            return false;
        };
        let presented = presented.expose_secret();
        expected.len() == presented.len()
            && bool::from(expected.as_bytes().ct_eq(presented.as_bytes()))
    }
}

impl std::fmt::Debug for ClientKeyStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientKeyStore")
            .field("client_count", &self.keys.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::ClientKeyStore;
    use relaygate_protocol::ClientKey;

    #[test]
    fn exact_client_key_is_required() {
        let store = ClientKeyStore::new([("echo.alpha".to_owned(), "secret".to_owned())].into());

        assert!(store.authorizes("echo.alpha", &ClientKey::new("secret")));
        assert!(!store.authorizes("echo.alpha", &ClientKey::new("Secret")));
        assert!(!store.authorizes("echo.alpha", &ClientKey::new("short")));
        assert!(!store.authorizes("missing", &ClientKey::new("secret")));
        assert!(!format!("{store:?}").contains("secret"));
    }
}
