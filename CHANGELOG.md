# Changelog

All notable RelayGate changes are documented here.

## 0.1.0 - 2026-08-18

- Split the Go runtime into durable `controller` and stateless `gateway` roles.
- Persist only current Gateway sessions and exact routes in the embedded Raft FSM.
- Add local leader-only Raft membership operations over a protected Unix socket.
- Add bounded public Go and Rust SDKs with managed session reconnect and current Listener rebind.
- Add same-Gateway and cross-Gateway bidirectional Pipes without queue, resume, or payload replay.
- Add three-controller/two-gateway Compose failure, SDK conformance, and echo-example evidence.
