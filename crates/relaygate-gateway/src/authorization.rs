mod claims;
mod config;

use std::{sync::Arc, time::Duration};

use jsonwebtoken::{Algorithm, Validation, decode, decode_header};
use relaygate_destination::Destination;
use relaygate_protocol::{BearerToken, ErrorCode, Frame, PeerObservation};
use tokio::{sync::Semaphore, task::JoinHandle, time::Instant};

pub use config::{AuthorizationConfig, Es256PublicKey, TrustedIssuer};

use claims::{Action, Claims};

pub(crate) const TOKEN_TYPE: &str = "relaygate-operation+jwt";
const TOKEN_MEDIA_TYPE: &str = "application/relaygate-operation+jwt";

#[derive(Debug, Clone)]
pub(crate) enum ControlOperation {
    Publish {
        request_id: u64,
        destination: Destination,
    },
    Dial {
        connection_id: u64,
        destination: Destination,
        started_at: std::time::Instant,
    },
}

impl ControlOperation {
    pub(crate) fn take(frame: Frame) -> Result<(Self, BearerToken), Frame> {
        match frame {
            Frame::Publish {
                request_id,
                destination,
                access_token,
            } => Ok((
                Self::Publish {
                    request_id,
                    destination,
                },
                access_token,
            )),
            Frame::Dial {
                connection_id,
                destination,
                access_token,
            } => Ok((
                Self::Dial {
                    connection_id,
                    destination,
                    started_at: std::time::Instant::now(),
                },
                access_token,
            )),
            frame => Err(frame),
        }
    }

    pub(crate) fn destination(&self) -> &Destination {
        match self {
            Self::Publish { destination, .. } | Self::Dial { destination, .. } => destination,
        }
    }

    fn action(&self) -> Action {
        match self {
            Self::Publish { .. } => Action::Publish,
            Self::Dial { .. } => Action::Dial,
        }
    }

    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::Publish { .. } => "publish",
            Self::Dial { .. } => "dial",
        }
    }

    pub(crate) fn failure(&self, code: ErrorCode) -> Frame {
        let message = "operation authorization failed".to_owned();
        match self {
            Self::Publish { request_id, .. } => Frame::PublishFailed {
                request_id: *request_id,
                code,
                message,
            },
            Self::Dial { connection_id, .. } => Frame::DialFailed {
                connection_id: *connection_id,
                code,
                observation: PeerObservation::NotObserved,
                message,
            },
        }
    }
}

#[derive(Clone)]
struct Verifier {
    config: AuthorizationConfig,
}

impl Verifier {
    fn verify(
        &self,
        token: &BearerToken,
        operation: &ControlOperation,
    ) -> Result<VerifiedAuthorization, ErrorCode> {
        let header =
            decode_header(token.expose_secret()).map_err(|_| ErrorCode::Unauthenticated)?;
        if header.alg != Algorithm::ES256
            || !header.typ.as_deref().is_some_and(is_supported_token_type)
            || header.crit.is_some()
        {
            return Err(ErrorCode::Unauthenticated);
        }
        let kid = header.kid.ok_or(ErrorCode::Unauthenticated)?;
        let namespace = operation.destination().namespace();

        let trusted = self
            .config
            .issuers
            .iter()
            .find(|trusted| &trusted.namespace == namespace)
            .ok_or(ErrorCode::Unauthenticated)?;
        let key = trusted
            .keys
            .iter()
            .find(|key| key.kid.as_ref() == kid)
            .ok_or(ErrorCode::Unauthenticated)?;
        let mut validation = Validation::new(Algorithm::ES256);
        validation.validate_nbf = true;
        validation.leeway = self.config.clock_skew.as_secs();
        validation.set_required_spec_claims(&["iss", "aud", "nbf", "exp"]);
        validation.set_issuer(&[trusted.issuer.as_ref()]);
        validation.set_audience(&[self.config.audience.as_ref()]);
        let data = decode::<Claims>(token.expose_secret(), &key.decoding_key, &validation)
            .map_err(|_| ErrorCode::Unauthenticated)?;
        if !data.claims.registered_claims_are_sane() {
            return Err(ErrorCode::Unauthenticated);
        }
        let now = jsonwebtoken::get_current_timestamp();
        let checked_at = Instant::now();
        let expires_unix = data
            .claims
            .exp
            .checked_add(self.config.clock_skew.as_secs())
            .ok_or(ErrorCode::Unauthenticated)?;
        let remaining = expires_unix
            .checked_sub(now)
            .ok_or(ErrorCode::Unauthenticated)?;
        let expires_at = checked_at
            .checked_add(Duration::from_secs(remaining))
            .ok_or(ErrorCode::Unauthenticated)?;
        if !data
            .claims
            .authorizes(operation.action(), operation.destination())
        {
            return Err(ErrorCode::PermissionDenied);
        }
        Ok(VerifiedAuthorization {
            destination: operation.destination().clone(),
            action: operation.action(),
            expires_at,
        })
    }
}

fn is_supported_token_type(token_type: &str) -> bool {
    token_type.eq_ignore_ascii_case(TOKEN_TYPE) || token_type.eq_ignore_ascii_case(TOKEN_MEDIA_TYPE)
}

pub(crate) struct Authorization {
    verifier: Arc<Verifier>,
    slots: Arc<Semaphore>,
}

impl Authorization {
    pub(crate) fn new(config: AuthorizationConfig, concurrency: usize) -> Self {
        Self {
            verifier: Arc::new(Verifier { config }),
            slots: Arc::new(Semaphore::new(concurrency)),
        }
    }

    pub(crate) fn start(
        &self,
        token: BearerToken,
        operation: &ControlOperation,
    ) -> Result<VerificationJob, ErrorCode> {
        let verifier = Arc::clone(&self.verifier);
        let operation = operation.clone();
        let permit = Arc::clone(&self.slots)
            .try_acquire_owned()
            .map_err(|_| ErrorCode::ResourceExhausted)?;
        Ok(VerificationJob(tokio::task::spawn_blocking(move || {
            let _permit = permit;
            verifier.verify(&token, &operation)
        })))
    }
}

pub(crate) struct VerificationJob(JoinHandle<Result<VerifiedAuthorization, ErrorCode>>);

impl VerificationJob {
    pub(crate) async fn finish(
        mut self,
        deadline: Instant,
    ) -> Result<VerifiedAuthorization, ErrorCode> {
        if Instant::now() >= deadline {
            return Err(ErrorCode::DeadlineExceeded);
        }
        let result = tokio::time::timeout_at(deadline, &mut self.0)
            .await
            .map_err(|_| ErrorCode::DeadlineExceeded)?
            .map_err(|_| ErrorCode::Internal)?;
        if Instant::now() >= deadline {
            return Err(ErrorCode::DeadlineExceeded);
        }
        result
    }
}

impl Drop for VerificationJob {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Debug)]
pub(crate) struct VerifiedAuthorization {
    destination: Destination,
    action: Action,
    expires_at: Instant,
}

impl VerifiedAuthorization {
    pub(crate) fn authorizes(&self, operation: &ControlOperation, now: Instant) -> bool {
        self.destination == *operation.destination()
            && self.action == operation.action()
            && now < self.expires_at
    }
}

#[cfg(test)]
mod tests;
