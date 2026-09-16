//! Configuration bounds shared by the client and the service halves so the
//! two cannot drift apart.

use std::time::Duration;

use tokio::sync::Semaphore;

use crate::TransportError;

/// A count that must be positive.
fn validate_nonzero(name: &'static str, value: usize) -> Result<(), TransportError> {
    if value == 0 {
        Err(TransportError::invalid_argument(format!(
            "{name} must be greater than zero"
        )))
    } else {
        Ok(())
    }
}

/// A queue or connection capacity, bounded by the tightest runtime ceiling
/// either half can impose (`Semaphore::MAX_PERMITS`), so the two cannot drift.
pub(crate) fn validate_capacity(name: &'static str, value: usize) -> Result<(), TransportError> {
    validate_nonzero(name, value)?;
    if value > Semaphore::MAX_PERMITS {
        return Err(TransportError::invalid_argument(format!(
            "{name} exceeds the runtime limit"
        )));
    }
    Ok(())
}

/// A maximum frame length that fits the wire's 32-bit length prefix.
pub(crate) fn validate_frame_len(value: usize) -> Result<(), TransportError> {
    validate_nonzero("maximum frame length", value)?;
    if value > u32::MAX as usize {
        return Err(TransportError::invalid_argument(
            "maximum frame length exceeds the wire limit",
        ));
    }
    Ok(())
}

/// A timeout or interval that must be positive.
pub(crate) fn validate_duration(name: &'static str, value: Duration) -> Result<(), TransportError> {
    if value.is_zero() {
        Err(TransportError::invalid_argument(format!(
            "{name} must be greater than zero"
        )))
    } else {
        Ok(())
    }
}
