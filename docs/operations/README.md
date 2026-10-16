# Operations

Terminate public TLS at a trusted ingress or provide a separately managed TLS listener. Configure
the ingress to forward WebSocket upgrade headers, preserve the `Origin` header, and use idle and
drain timeouts suitable for long-lived sessions. Keep the service on a private network and set
`AllowedOrigins` explicitly for browser clients.

Size `MaxConnections` from measured memory and file descriptor budgets. Keep message bounds no larger
than application needs. Call `Shutdown` with a deadline on termination; the library closes active
sessions after stopping the HTTP listener. Clients remain responsible for reconnect policy.

`AllowedOrigins` is exact: up to 256 unique absolute HTTP(S) origins without paths, queries,
fragments, userinfo, or glob metacharacters. An empty list retains `coder/websocket`'s same-host
origin check. `Subprotocols` supports up to 64 unique HTTP token values, each at most 128 bytes;
the server negotiates only a configured token offered by the client. Invalid configuration fails
before a handler is exposed.

The session limit is acquired before the HTTP upgrade. Requests beyond capacity receive 503 without
creating a WebSocket session. Handler callbacks receive the server context, which is canceled by
`Shutdown`; use it for all receive/send operations and release upstream subscriptions on return.
Shutdown closes active sessions and may abort connections when the supplied deadline expires.
