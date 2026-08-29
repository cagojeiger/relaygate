use std::{io, time::Duration};

use relaygate_protocol::SessionId;
use tokio::{
    sync::{Mutex, mpsc},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use super::{Connector, ConnectorCommand, ConnectorInner, ConnectorSession};
use crate::{Config, Error, ErrorCode, PeerObservation};

#[tokio::test]
async fn precommit_closed_actor_retries_on_replacement_session()
-> Result<(), Box<dyn std::error::Error>> {
    let (stale_control, stale_receiver) = mpsc::channel(1);
    drop(stale_receiver);
    let stale = std::sync::Arc::new(ConnectorSession {
        id: SessionId::new(),
        next_connection_id: Mutex::new(1),
        control: stale_control,
        cancellations: mpsc::unbounded_channel().0,
        cancel: CancellationToken::new(),
    });
    let (current, _) = tokio::sync::watch::channel(Some(stale));
    let connector = Connector {
        inner: std::sync::Arc::new(ConnectorInner {
            config: Config::new("unused").with_operation_timeout(Duration::from_secs(5)),
            current: current.clone(),
            cancel: CancellationToken::new(),
            lifetime: std::sync::Weak::new(),
        }),
        _lifetime: std::sync::Arc::new(crate::lifetime::RuntimeLifetime::new(
            CancellationToken::new(),
        )),
    };

    let opening = tokio::spawn({
        let connector = connector.clone();
        async move { connector.open("echo.alpha").await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (replacement_control, mut replacement_receiver) = mpsc::channel(1);
    current.send_replace(Some(std::sync::Arc::new(ConnectorSession {
        id: SessionId::new(),
        next_connection_id: Mutex::new(1),
        control: replacement_control,
        cancellations: mpsc::unbounded_channel().0,
        cancel: CancellationToken::new(),
    })));
    let command = timeout(Duration::from_secs(2), replacement_receiver.recv())
        .await?
        .ok_or_else(|| io::Error::other("replacement actor did not receive OPEN"))?;
    let ConnectorCommand::Open { response, .. } = command;
    let _ = response.send(Err(Error::new(
        ErrorCode::NotFound,
        PeerObservation::NotObserved,
        "replacement actor observed the operation",
    )));

    let error = opening
        .await?
        .err()
        .ok_or_else(|| io::Error::other("replacement operation unexpectedly succeeded"))?;
    assert_eq!(error.code(), ErrorCode::NotFound);
    assert_eq!(error.observation(), PeerObservation::NotObserved);
    connector.close();
    Ok(())
}

#[tokio::test]
async fn concurrent_opens_commit_monotonic_connection_ids() -> Result<(), Box<dyn std::error::Error>>
{
    let (control, mut commands) = mpsc::channel(1);
    let (cancellations, _cancelled) = mpsc::unbounded_channel();
    let (current, _) = tokio::sync::watch::channel(Some(std::sync::Arc::new(ConnectorSession {
        id: SessionId::new(),
        next_connection_id: Mutex::new(1),
        control,
        cancellations,
        cancel: CancellationToken::new(),
    })));
    let connector = Connector {
        inner: std::sync::Arc::new(ConnectorInner {
            config: Config::new("unused").with_operation_timeout(Duration::from_secs(5)),
            current,
            cancel: CancellationToken::new(),
            lifetime: std::sync::Weak::new(),
        }),
        _lifetime: std::sync::Arc::new(crate::lifetime::RuntimeLifetime::new(
            CancellationToken::new(),
        )),
    };

    let mut openings = Vec::new();
    for _ in 0..32 {
        let connector = connector.clone();
        openings.push(tokio::spawn(
            async move { connector.open("echo.alpha").await },
        ));
    }

    for expected in 1..=32 {
        let command = timeout(Duration::from_secs(2), commands.recv())
            .await?
            .ok_or_else(|| io::Error::other("Connector actor closed before all OPENs"))?;
        let ConnectorCommand::Open {
            connection_id,
            response,
            ..
        } = command;
        assert_eq!(connection_id, expected);
        let _ = response.send(Err(Error::new(
            ErrorCode::NotFound,
            PeerObservation::NotObserved,
            "test response",
        )));
    }

    for opening in openings {
        let error = opening
            .await?
            .err()
            .ok_or_else(|| io::Error::other("OPEN unexpectedly succeeded"))?;
        assert_eq!(error.code(), ErrorCode::NotFound);
    }
    connector.close();
    Ok(())
}
