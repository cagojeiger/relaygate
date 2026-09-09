use std::sync::{Mutex, MutexGuard};

use tokio::time::Instant;

use crate::rate_limit::TokenBucket;

pub(super) struct ConnectionRateLimit {
    bucket: Mutex<TokenBucket>,
}

impl ConnectionRateLimit {
    pub(super) fn new(per_second: usize, burst: usize) -> Self {
        Self {
            bucket: Mutex::new(TokenBucket::new(
                per_second,
                burst,
                Instant::now().into_std(),
            )),
        }
    }

    pub(super) fn try_acquire(&self) -> bool {
        self.lock().try_take(Instant::now().into_std())
    }

    pub(super) fn has_capacity(&self) -> bool {
        self.lock().has_capacity(Instant::now().into_std())
    }

    fn lock(&self) -> MutexGuard<'_, TokenBucket> {
        self.bucket
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }
}

#[cfg(test)]
mod tests;
