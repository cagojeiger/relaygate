# relaygate-destination

Canonical logical destination types shared by RelayGate crates.

This crate owns parsing, validation and canonical formatting for `Namespace`,
`DestinationName` and `Destination`. It does not own routing, authorization,
registration, discovery or token issuance.

## License

Licensed under the Apache License, Version 2.0. The package includes the
workspace `LICENSE` file through Cargo's `license-file` metadata, so the full
license text is present inside the packaged crate archive.
