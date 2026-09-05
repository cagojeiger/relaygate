use anyhow::{Context, ensure};
use relaygate_sdk::{DestinationId, Listener, Pipe, Relay};
use tokio::{io::AsyncReadExt, io::AsyncWriteExt, time::timeout};

use crate::{
    config::{
        DESTINATION_IDS, ECHO_DEADLINE, ROUTE_WAIT, SHARED_DESTINATION_ID, gateway_addresses,
    },
    probe::{connect, dial_when_available},
};

pub(crate) async fn run_chat() -> anyhow::Result<()> {
    let addresses = gateway_addresses()?;
    let mut relays = Vec::with_capacity(addresses.len());
    let mut listeners = Vec::with_capacity(addresses.len());
    for (index, address) in addresses.iter().enumerate() {
        let relay = connect(address).await?;
        let destination = parse_destination(DESTINATION_IDS[index])?;
        let listener = relay
            .listen(destination)
            .await
            .with_context(|| format!("chat participant {index} failed to publish {destination}"))?;
        relays.push(relay);
        listeners.push(listener);
    }

    for from in 0..relays.len() {
        for to in 0..relays.len() {
            if from == to {
                continue;
            }
            exchange(
                &relays[from],
                &listeners[to],
                parse_destination(DESTINATION_IDS[to])?,
                from,
                to,
            )
            .await?;
        }
    }

    let shared = parse_destination(SHARED_DESTINATION_ID)?;
    let shared_one = relays[1].listen(shared).await?;
    let shared_two = relays[2].listen(shared).await?;
    let first_incoming = async {
        tokio::select! {
            pipe = shared_one.accept() => {
                Ok::<_, anyhow::Error>(SelectedIncoming::One(pipe?))
            }
            pipe = shared_two.accept() => {
                Ok(SelectedIncoming::Two(pipe?))
            }
        }
    };
    let (first_outgoing, selected) = tokio::try_join!(
        dial_when_available(&relays[0], SHARED_DESTINATION_ID, ROUTE_WAIT),
        first_incoming
    )?;
    match selected {
        SelectedIncoming::One(first_pipe) => {
            verify_shared_failover(
                &relays[0],
                first_outgoing,
                first_pipe,
                shared_one,
                shared_two,
                1,
                2,
            )
            .await?;
        }
        SelectedIncoming::Two(first_pipe) => {
            verify_shared_failover(
                &relays[0],
                first_outgoing,
                first_pipe,
                shared_two,
                shared_one,
                2,
                1,
            )
            .await?;
        }
    }
    for listener in listeners {
        listener.close().await?;
    }
    for relay in relays {
        relay.close();
    }
    println!(
        "relaygate host SDK chat verified: 3 participants, 6 directed 1:1 chats, local and one-hop paths, N:M selected-binding removal and survivor failover"
    );
    Ok(())
}

enum SelectedIncoming {
    One(Pipe),
    Two(Pipe),
}

async fn verify_shared_failover(
    caller: &Relay,
    first_outgoing: Pipe,
    first_incoming: Pipe,
    selected: Listener,
    survivor: Listener,
    selected_owner: usize,
    survivor_owner: usize,
) -> anyhow::Result<()> {
    exchange_pipes(first_outgoing, first_incoming, 0, selected_owner, 0).await?;
    selected.close().await?;

    let survivor_incoming = async { Ok::<_, anyhow::Error>(survivor.accept().await?) };
    let (second_outgoing, second_incoming) = tokio::try_join!(
        dial_when_available(caller, SHARED_DESTINATION_ID, ROUTE_WAIT),
        survivor_incoming
    )?;
    exchange_pipes(second_outgoing, second_incoming, 0, survivor_owner, 1).await?;
    survivor.close().await?;
    Ok(())
}

async fn exchange(
    caller: &Relay,
    listener: &Listener,
    destination: DestinationId,
    from: usize,
    to: usize,
) -> anyhow::Result<()> {
    let destination = destination.to_string();
    let (outgoing, incoming) = tokio::try_join!(
        dial_when_available(caller, &destination, ROUTE_WAIT),
        async { Ok::<_, anyhow::Error>(listener.accept().await?) }
    )?;
    exchange_pipes(outgoing, incoming, from, to, 0).await
}

async fn exchange_pipes(
    mut outgoing: Pipe,
    mut incoming: Pipe,
    from: usize,
    to: usize,
    sequence: usize,
) -> anyhow::Result<()> {
    timeout(ECHO_DEADLINE, async {
        let request = format!("chat from={from} to={to} sequence={sequence}").into_bytes();
        outgoing.write_all(&request).await?;

        let mut received = vec![0_u8; request.len()];
        incoming.read_exact(&mut received).await?;
        ensure!(received == request, "chat request changed in transit");

        let response = format!("ack from={to} to={from} sequence={sequence}").into_bytes();
        incoming.write_all(&response).await?;
        incoming.shutdown().await?;

        let mut acknowledged = vec![0_u8; response.len()];
        outgoing.read_exact(&mut acknowledged).await?;
        ensure!(acknowledged == response, "chat response changed in transit");
        outgoing.close().await?;
        incoming.close().await?;
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("chat Pipe timed out")?
}

fn parse_destination(value: &str) -> anyhow::Result<DestinationId> {
    value
        .parse()
        .with_context(|| format!("invalid UUIDv4 DestinationId {value:?}"))
}
