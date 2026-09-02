use std::time::Duration;

use relaygate_sdk::{Config, Connector, Error, ErrorCode, ListenerRuntime, PeerObservation};

#[tokio::test]
async fn unrepresentable_deadline_configuration_is_rejected_before_connect() {
    let address = "127.0.0.1:1";
    let second = Duration::from_secs(1);
    for config in [
        Config::new(address).with_connect_timeout(Duration::MAX),
        Config::new(address).with_operation_timeout(Duration::MAX),
        Config::new(address).with_heartbeat(Duration::MAX, second),
        Config::new(address).with_heartbeat(second, Duration::MAX),
        Config::new(address).with_reconnect_backoff(Duration::MAX, Duration::MAX),
        Config::new(address).with_offer_timeout(Duration::MAX),
    ] {
        assert_invalid_configuration(Connector::connect(config.clone()).await.err());
        assert_invalid_configuration(ListenerRuntime::connect(config).await.err());
    }
}

fn assert_invalid_configuration(error: Option<Error>) {
    assert_eq!(
        error.map(|error| (error.code(), error.observation())),
        Some((ErrorCode::InvalidArgument, PeerObservation::NotObserved))
    );
}
