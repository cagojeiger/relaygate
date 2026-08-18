ARG GO_VERSION=1.26.6
ARG RUST_VERSION=1.96.1

FROM golang:${GO_VERSION}-alpine AS go-builder
WORKDIR /src
COPY sdk/go/go.mod sdk/go/go.sum ./sdk/go/
COPY examples/echo/go/go.mod examples/echo/go/go.sum ./examples/echo/go/
WORKDIR /src/examples/echo/go
RUN GOWORK=off go mod download
WORKDIR /src
COPY sdk/go ./sdk/go
COPY examples/echo/go ./examples/echo/go
WORKDIR /src/examples/echo/go
RUN GOWORK=off CGO_ENABLED=0 go build -trimpath -o /out/relaygate-echo .

FROM gcr.io/distroless/static-debian12:nonroot AS go-runtime
COPY --from=go-builder /out/relaygate-echo /relaygate-echo
USER nonroot:nonroot
ENTRYPOINT ["/relaygate-echo"]

FROM rust:${RUST_VERSION}-slim-bookworm AS rust-builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY proto ./proto
COPY sdk/rust ./sdk/rust
COPY examples/echo/rust ./examples/echo/rust
RUN cargo build --locked --release --package relaygate-echo-rust

FROM gcr.io/distroless/cc-debian12:nonroot AS rust-runtime
COPY --from=rust-builder /src/target/release/relaygate-echo-rust /relaygate-echo
USER nonroot:nonroot
ENTRYPOINT ["/relaygate-echo"]
