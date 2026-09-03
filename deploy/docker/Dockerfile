FROM rust:1.96.1-bookworm AS builder

WORKDIR /workspace

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY examples ./examples
COPY tools/test-contract ./tools/test-contract

RUN cargo build --locked --release \
    --bin relaygate-server \
    --bin relaygate-echo-listener \
    --bin relaygate-echo-probe

FROM debian:bookworm-slim AS server-runtime

RUN groupadd --system --gid 10001 relaygate \
    && useradd --system --uid 10001 --gid relaygate --no-create-home relaygate

COPY --from=builder /workspace/target/release/relaygate-server /usr/local/bin/relaygate-server

USER 10001:10001

FROM server-runtime AS gateway

EXPOSE 27420 27421

ENTRYPOINT ["relaygate-server"]
CMD ["gateway"]

FROM server-runtime AS route-table

EXPOSE 27430

ENTRYPOINT ["relaygate-server"]
CMD ["route-table"]

FROM server-runtime AS test

COPY --from=builder /workspace/target/release/relaygate-echo-listener /usr/local/bin/relaygate-echo-listener
COPY --from=builder /workspace/target/release/relaygate-echo-probe /usr/local/bin/relaygate-echo-probe

CMD ["relaygate-server"]
