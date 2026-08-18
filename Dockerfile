ARG GO_VERSION=1.26.6
FROM golang:${GO_VERSION}-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION=dev
RUN GOWORK=off CGO_ENABLED=0 go build -trimpath \
      -ldflags="-s -w -X main.version=${VERSION}" \
      -o /out/relaygate ./cmd/relaygate && mkdir -p /out/raft-data

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /
COPY --from=builder /out/relaygate /relaygate
COPY --from=builder /src/configs/relaygate.yaml /etc/relaygate/relaygate.yaml
COPY --from=builder --chown=nonroot:nonroot /out/raft-data /var/lib/relaygate

ENV RELAYGATE_RAFT_DATA_DIR=/var/lib/relaygate
USER nonroot:nonroot
EXPOSE 27400 27410 27420 27430 27490
ENTRYPOINT ["/relaygate"]
CMD ["-config", "/etc/relaygate/relaygate.yaml"]
