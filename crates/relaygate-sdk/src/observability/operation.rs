use std::{future::Future, time::Instant};

use crate::{ErrorCode, Result};

/// Measures the polled operation, including queue/session wait and cancellation.
pub(crate) async fn observe<T>(
    operation: &'static str,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    let mut guard = Operation {
        operation,
        started: Instant::now(),
        outcome: "cancelled",
        code: "cancelled",
    };
    let result = future.await;
    (guard.outcome, guard.code) = match &result {
        Ok(_) => ("success", "ok"),
        Err(error) if error.code() == ErrorCode::Cancelled => ("cancelled", "cancelled"),
        Err(error) => ("error", error.code().metric_name()),
    };
    result
}

struct Operation {
    operation: &'static str,
    started: Instant,
    outcome: &'static str,
    code: &'static str,
}

impl Drop for Operation {
    fn drop(&mut self) {
        metrics::counter!("relaygate_sdk_operation_results_total", "operation" => self.operation,
            "outcome" => self.outcome, "code" => self.code)
        .increment(1);
        metrics::histogram!("relaygate_sdk_operation_duration_seconds", "operation" => self.operation,
            "outcome" => self.outcome).record(self.started.elapsed().as_secs_f64());
    }
}
