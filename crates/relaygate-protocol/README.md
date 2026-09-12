# relaygate-protocol

SDK-Gateway wire framing support for RelayGate.

This is an implementation-detail support crate. It owns frame encoding,
decoding, wire identifiers and protocol errors shared by the SDK and Gateway.
It does not own sockets, TLS policy, reconnect policy, route-table state,
admission or application payload semantics.

Applications should normally use `relaygate-sdk` instead of depending on this
crate directly.

This crate is currently guarded with `publish = false`; the metadata and
package checks exist so crate archives can be validated before a future public
release decision.

## License

Licensed under the Apache License, Version 2.0. The package includes the
workspace `LICENSE` file through Cargo's `license-file` metadata, so the full
license text is present inside the packaged crate archive.
