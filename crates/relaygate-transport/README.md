# relaygate-transport

TLS and mTLS transport helpers for RelayGate.

This crate owns reusable Rustls configuration and ALPN checks for RelayGate
client and server streams. It does not own Relay sessions, Gateway routing,
authorization, certificate rotation policy or platform secret management.

## License

Licensed under the Apache License, Version 2.0. The package includes the
workspace `LICENSE` file through Cargo's `license-file` metadata, so the full
license text is present inside the packaged crate archive.
