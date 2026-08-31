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

FROM debian:bookworm-slim AS runtime

RUN groupadd --system --gid 10001 relaygate \
    && useradd --system --uid 10001 --gid relaygate --no-create-home relaygate

COPY --from=builder /workspace/target/release/relaygate-server /usr/local/bin/relaygate-server
COPY --from=builder /workspace/target/release/relaygate-echo-listener /usr/local/bin/relaygate-echo-listener
COPY --from=builder /workspace/target/release/relaygate-echo-probe /usr/local/bin/relaygate-echo-probe

USER 10001:10001

EXPOSE 27420

CMD ["relaygate-server"]
