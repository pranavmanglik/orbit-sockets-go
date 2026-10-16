# Orbit Sockets Go

`orbit-sockets-go` is the Go standalone WebSocket service option in the Orbit Sockets capability.
It uses [`coder/websocket`](https://github.com/coder/websocket) for HTTP upgrade and protocol
handling. The package does not depend on Python Core or run as a Core process plugin; deploy it
behind an ingress that supports WebSocket upgrades.

## Install and use

```sh
go get github.com/orbit-projects/orbit-sockets-go
```

```go
package main

import (
    "context"

    "github.com/orbit-projects/orbit-sockets-go"
)

func main() {
    settings := sockets.NewSettings()
    settings.AllowedOrigins = []string{"https://app.example"}
    server, err := sockets.NewServer(settings, func(ctx context.Context, session *sockets.Session) error {
        message, err := session.Receive(ctx)
        if err != nil {
            return err
        }
        return session.Send(ctx, message)
    })
    if err != nil {
        panic("invalid socket server configuration")
    }
    if err := server.ListenAndServe(); err != nil {
        panic("socket server stopped")
    }
}
```

Set `AllowedOrigins` to the exact browser origins permitted by the application. Without an explicit
list, `coder/websocket` applies its same-host origin check. Message size, connection count, route,
and compression are bounded or configured explicitly; compression is disabled by default. Use
`Shutdown(ctx)` during process termination to stop accepts, cancel handlers, and close active
sessions.

`AllowedOrigins` accepts at most 256 unique `http://` or `https://` origins without paths, queries,
fragments, credentials, or wildcard host patterns. `Subprotocols` accepts at most 64 unique HTTP
tokens, each at most 128 bytes. Invalid paths, origins, and protocol names fail `NewServer` before
the listener is exposed.

## Reliability and security

The server bounds active sessions before upgrade and applies a read limit to complete messages.
`Receive` and `Send` accept contexts for caller deadlines and cancellation. Callers must serialize
concurrent reads. Handler errors close with a sanitized 1011 reason; payloads are excluded from
`Message.String()` and are never logged by this package. The service does not provide identity,
authorization, shared fanout, durable delivery, or application acknowledgements.

## Documentation and checks

See the [architecture](docs/architecture/overview.md), [operations](docs/operations/README.md),
[security](docs/security/overview.md), and [development](docs/development/README.md) guides.

```sh
go test ./...
go test -race ./...
go vet ./...
go build ./...
```

This package is alpha. Hosted CI, Autobahn conformance, ingress integration, multi-instance
load/soak tests, and release provenance remain open release gates.

Licensed under Apache-2.0.
