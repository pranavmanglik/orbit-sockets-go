# Security

The listener rejects unrelated paths and enforces `coder/websocket` origin checks. Configure exact
browser origins where possible. An Origin check is not authentication; authenticate connections and
authorize every subscription in the application handler. Do not put credentials in URLs. Keep the
service private behind a TLS-terminating ingress, limit connection and message sizes, and use
timeouts on reads and writes.

The default empty allowlist permits same-host browser origins and clients with no Origin according
to the underlying library. An explicit allowlist permits exact origins only; wildcard host patterns
are rejected. Native clients may omit Origin, so never use Origin as an identity signal. Authenticate
them separately and authorize each message or subscription.

Errors returned to clients use fixed close reasons. The package never logs payloads or formats raw
transport errors. Application logging should also avoid message bodies, tokens, and provider errors.
