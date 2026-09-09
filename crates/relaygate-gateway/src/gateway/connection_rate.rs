use std::sync::{Mutex, MutexGuard};

use tokio::time::Instant;

const TOKEN: u128 = 1_000_000_000;

pub(super) struct ConnectionRateLimit {
    bucket: Mutex<TokenBucket>,
}

impl ConnectionRateLimit {
    pub(super) fn new(per_second: usize, burst: usize) -> Self {
        Self {
            bucket: Mutex::new(TokenBucket {
                per_second: per_second as u128,
                capacity: burst as u128 * TOKEN,
                credit: burst as u128 * TOKEN,
                updated_at: Instant::now(),
            }),
        }
    }

    pub(super) fn try_acquire(&self) -> bool {
        self.lock().try_take(Instant::now())
    }

    pub(super) fn has_capacity(&self) -> bool {
        let mut bucket = self.lock();
        bucket.refill(Instant::now());
        bucket.credit >= TOKEN
    }

    fn lock(&self) -> MutexGuard<'_, TokenBucket> {
        self.bucket
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }
}

struct TokenBucket {
    per_second: u128,
    capacity: u128,
    // Fractional tokens are retained as integer nanosecond credits.
    credit: u128,
    updated_at: Instant,
}

impl TokenBucket {
    fn refill(&mut self, now: Instant) {
        let added = now
            .saturating_duration_since(self.updated_at)
            .as_nanos()
            .saturating_mul(self.per_second);
        self.credit = self.capacity.min(self.credit.saturating_add(added));
        self.updated_at = now;
    }

    fn try_take(&mut self, now: Instant) -> bool {
        self.refill(now);
        if self.credit < TOKEN {
            return false;
        }
        self.credit -= TOKEN;
        true
    }
}

#[cfg(test)]
mod tests;
