# Architecture

This package is a complete standalone Go WebSocket listener built on `net/http` and
`coder/websocket`. It accepts a single configured route, validates origin policy before exposing
application sessions, disables per-message compression, applies a bounded connection semaphore and
message read limit, and exposes async-like context-aware receive and send methods.

`Settings` is validated before the server is constructed. Paths are bounded visible ASCII request
paths. `AllowedOrigins` contains up to 256 unique, exact HTTP(S) origins; wildcard patterns,
credentials, paths, queries, and fragments are rejected. `Subprotocols` contains up to 64 unique
HTTP tokens of at most 128 bytes. The server negotiates only values configured by the application.

It is deployed behind ingress and is not supervised by Core's local gRPC process host. Core gRPC is
not a general application microservice protocol. Cross-language behavior is expressed through the
WebSocket wire protocol and the shared message bounds documented in `orbit-sockets`.
