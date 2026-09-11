use std::{future::Future, pin::Pin, sync::Arc};

use relaygate_protocol::{BearerToken, MAX_BEARER_TOKEN_BYTES};

use crate::{Error, ErrorCode, PeerObservation, RouteAddress};

/// An access token supplied for one `listen` or `dial` admission decision.
#[derive(Clone, PartialEq, Eq)]
pub struct AccessToken(BearerToken);

impl AccessToken {
    pub fn new(value: impl Into<String>) -> Result<Self, AccessTokenError> {
        let value = value.into();
        if value.is_empty() {
            return Err(AccessTokenError::Empty);
        }
        let actual = value.len();
        BearerToken::new(value)
            .map(Self)
            .map_err(|_| AccessTokenError::TooLong {
                actual,
                maximum: MAX_BEARER_TOKEN_BYTES,
            })
    }
}

impl std::fmt::Debug for AccessToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AccessToken([REDACTED])")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AccessTokenError {
    #[error("access token must not be empty")]
    Empty,
    #[error("access token is {actual} bytes, maximum {maximum}")]
    TooLong { actual: usize, maximum: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AccessAction {
    Publish,
    Dial,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessTokenRequest {
    pub action: AccessAction,
    pub address: RouteAddress,
}

/// Failure reported by an application-owned dynamic token source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("access token source is unavailable")]
pub struct AccessTokenSourceError;

type AccessTokenFuture = Pin<
    Box<
        dyn Future<Output = std::result::Result<AccessToken, AccessTokenSourceError>>
            + Send
            + 'static,
    >,
>;
type AccessTokenCallback = dyn Fn(AccessTokenRequest) -> AccessTokenFuture + Send + Sync + 'static;

#[derive(Clone)]
enum AccessTokenSourceInner {
    Static(AccessToken),
    Dynamic(Arc<AccessTokenCallback>),
}

/// Supplies a token for each new admission attempt without owning refresh policy.
#[derive(Clone)]
pub struct AccessTokenSource(AccessTokenSourceInner);

impl AccessTokenSource {
    #[must_use]
    pub fn static_token(token: AccessToken) -> Self {
        Self(AccessTokenSourceInner::Static(token))
    }

    pub fn dynamic<F, Fut>(callback: F) -> Self
    where
        F: Fn(AccessTokenRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<AccessToken, AccessTokenSourceError>>
            + Send
            + 'static,
    {
        Self(AccessTokenSourceInner::Dynamic(Arc::new(move |request| {
            Box::pin(callback(request))
        })))
    }

    pub(crate) async fn supply(&self, request: AccessTokenRequest) -> Result<BearerToken, Error> {
        let token = match &self.0 {
            AccessTokenSourceInner::Static(token) => token.clone(),
            AccessTokenSourceInner::Dynamic(callback) => callback(request).await.map_err(|_| {
                Error::new(
                    ErrorCode::Unavailable,
                    PeerObservation::NotObserved,
                    "access token source is unavailable",
                )
            })?,
        };
        Ok(token.0)
    }
}

impl From<AccessToken> for AccessTokenSource {
    fn from(value: AccessToken) -> Self {
        Self::static_token(value)
    }
}

impl std::fmt::Debug for AccessTokenSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            AccessTokenSourceInner::Static(_) => {
                formatter.write_str("AccessTokenSource::Static([REDACTED])")
            }
            AccessTokenSourceInner::Dynamic(_) => {
                formatter.write_str("AccessTokenSource::Dynamic(..)")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    fn address() -> Result<RouteAddress, relaygate_address::AddressError> {
        "inference/stt.seoul".parse()
    }

    #[test]
    fn access_token_is_bounded_and_redacted() -> Result<(), AccessTokenError> {
        assert_eq!(AccessToken::new(""), Err(AccessTokenError::Empty));
        assert!(matches!(
            AccessToken::new("x".repeat(MAX_BEARER_TOKEN_BYTES + 1)),
            Err(AccessTokenError::TooLong { .. })
        ));
        let token = AccessToken::new("must-not-appear")?;
        assert_eq!(format!("{token:?}"), "AccessToken([REDACTED])");
        Ok(())
    }

    #[tokio::test]
    async fn dynamic_source_runs_for_each_supply() -> Result<(), Box<dyn std::error::Error>> {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let expected = address()?;
        let source = AccessTokenSource::dynamic(move |request| {
            let observed = Arc::clone(&observed);
            let expected = expected.clone();
            async move {
                assert_eq!(request.address, expected);
                observed.fetch_add(1, Ordering::Relaxed);
                AccessToken::new("grant").map_err(|_| AccessTokenSourceError)
            }
        });
        for action in [AccessAction::Publish, AccessAction::Dial] {
            source
                .supply(AccessTokenRequest {
                    action,
                    address: address()?,
                })
                .await?;
        }
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        Ok(())
    }
}
