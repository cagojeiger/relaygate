use relaygate_protocol::ClusterToken;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

pub(crate) struct ClusterTokenSet {
    current: String,
    next: Option<String>,
}

impl ClusterTokenSet {
    pub(crate) fn new(current: String, next: Option<String>) -> Self {
        Self { current, next }
    }

    pub(crate) fn authorizes(&self, presented: &ClusterToken) -> bool {
        let presented = presented.expose_secret().as_bytes();
        constant_time_eq(self.current.as_bytes(), presented)
            | self
                .next
                .as_deref()
                .is_some_and(|next| constant_time_eq(next.as_bytes(), presented))
    }
}

impl std::fmt::Debug for ClusterTokenSet {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClusterTokenSet")
            .field("token_count", &(1 + usize::from(self.next.is_some())))
            .finish()
    }
}

fn constant_time_eq(expected: &[u8], presented: &[u8]) -> bool {
    let expected = Sha256::digest(expected);
    let presented = Sha256::digest(presented);
    bool::from(expected.ct_eq(&presented))
}

#[cfg(test)]
mod tests {
    use relaygate_protocol::ClusterToken;

    use super::ClusterTokenSet;

    #[test]
    fn current_and_next_tokens_are_admitted_without_being_debugged() {
        let tokens =
            ClusterTokenSet::new("current-secret".to_owned(), Some("next-secret".to_owned()));

        assert!(tokens.authorizes(&ClusterToken::new("current-secret")));
        assert!(tokens.authorizes(&ClusterToken::new("next-secret")));
        assert!(!tokens.authorizes(&ClusterToken::new("wrong-secret")));
        assert!(!tokens.authorizes(&ClusterToken::new("x")));
        let rendered = format!("{tokens:?}");
        assert!(!rendered.contains("current-secret"));
        assert!(!rendered.contains("next-secret"));
    }
}
