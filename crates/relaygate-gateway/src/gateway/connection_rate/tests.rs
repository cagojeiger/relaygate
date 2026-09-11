use std::time::Duration;

use crate::{Gateway, GatewayConfig, test_support::authorization_config};

#[tokio::test(start_paused = true)]
async fn gateway_clones_share_rate_budget_and_readiness_does_not_consume_it()
-> Result<(), Box<dyn std::error::Error>> {
    let gateway = Gateway::new(
        GatewayConfig::new(authorization_config()).with_sdk_connection_rate_limit(2, 3),
    )?;
    let mut tasks = Vec::new();
    for _ in 0..32 {
        let clone = gateway.clone();
        tasks.push(tokio::spawn(async move {
            clone.inner.connection_rate.try_acquire()
        }));
    }
    let mut admitted = 0;
    for task in tasks {
        admitted += usize::from(task.await?);
    }
    assert_eq!(admitted, 3);
    assert!(!gateway.snapshot().sdk_admission_ready);
    assert_eq!(gateway.snapshot().pending_handshakes, 0);
    tokio::time::advance(Duration::from_millis(500)).await;
    for _ in 0..10 {
        assert!(gateway.snapshot().sdk_admission_ready);
    }
    assert!(gateway.inner.connection_rate.try_acquire());
    assert!(!gateway.inner.connection_rate.try_acquire());
    Ok(())
}
