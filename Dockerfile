ARG GO_VERSION=1.26.6
FROM golang:${GO_VERSION}-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION=dev
RUN mkdir -p /out/data && \
    CGO_ENABLED=0 go build -trimpath \
      -ldflags="-s -w -X main.version=${VERSION}" \
      -o /out/relaygate ./cmd/relaygate

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /
COPY --from=builder /out/relaygate /relaygate
COPY --from=builder /src/configs/relaygate.yaml /etc/relaygate/relaygate.yaml
COPY --from=builder --chown=65532:65532 /out/data /var/lib/relaygate

USER nonroot:nonroot
ENV RELAYGATE_RAFT_DATA_DIR=/var/lib/relaygate
VOLUME ["/var/lib/relaygate"]
EXPOSE 7000 7100 7200 7300 9090
ENTRYPOINT ["/relaygate"]
CMD ["-config", "/etc/relaygate/relaygate.yaml"]
