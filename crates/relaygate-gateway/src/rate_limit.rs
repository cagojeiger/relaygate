use std::time::Instant;

const TOKEN: u128 = 1_000_000_000;

#[derive(Debug, Clone)]
pub(crate) struct TokenBucket {
    per_second: u128,
    capacity: u128,
    // Fractional tokens are retained as integer nanosecond credits.
    credit: u128,
    updated_at: Instant,
}

impl TokenBucket {
    pub(crate) fn new(per_second: usize, burst: usize, now: Instant) -> Self {
        Self {
            per_second: per_second as u128,
            capacity: burst as u128 * TOKEN,
            credit: burst as u128 * TOKEN,
            updated_at: now,
        }
    }

    pub(crate) fn has_capacity(&mut self, now: Instant) -> bool {
        let added = now
            .saturating_duration_since(self.updated_at)
            .as_nanos()
            .saturating_mul(self.per_second);
        self.credit = self.capacity.min(self.credit.saturating_add(added));
        self.updated_at = now;
        self.credit >= TOKEN
    }

    pub(crate) fn try_take(&mut self, now: Instant) -> bool {
        if !self.has_capacity(now) {
            return false;
        }
        self.credit -= TOKEN;
        true
    }
}

#[cfg(test)]
mod tests;
