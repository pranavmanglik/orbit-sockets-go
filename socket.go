// Package sockets provides a bounded standalone WebSocket server for Orbit applications.
package sockets

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/coder/websocket"
)

// MaxMessageBytes is the hard package ceiling for complete text or binary messages.
const MaxMessageBytes = 1 << 20

// MaxCloseReasonBytes is the maximum UTF-8 byte size in a close control frame.
const MaxCloseReasonBytes = 123

const (
	maxPathBytes       = 2048
	maxAllowedOrigins  = 256
	maxSubprotocols    = 64
	maxSubprotocolSize = 128
)

var (
	// ErrClosed reports an operation on a closed WebSocket session.
	ErrClosed = errors.New("socket session is closed")
	// ErrMessageSize reports a message that exceeds its configured byte bound.
	ErrMessageSize = errors.New("socket message exceeds configured limit")
	// ErrProtocol reports an invalid socket value or configuration.
	ErrProtocol = errors.New("socket protocol value is invalid")
)

// MessageType identifies the WebSocket data opcode exposed by Message.
type MessageType uint8

const (
	// TextMessage marks a valid UTF-8 text frame.
	TextMessage MessageType = iota + 1
	// BinaryMessage marks an opaque binary frame.
	BinaryMessage
)

// Message is one bounded WebSocket text or binary message.
// String representations intentionally omit payload bytes.
type Message struct {
	typ  MessageType
	data []byte
}

// NewMessage validates and snapshots a text or binary message.
func NewMessage(typ MessageType, data []byte) (Message, error) {
	if typ != TextMessage && typ != BinaryMessage {
		return Message{}, ErrProtocol
	}
	if len(data) > MaxMessageBytes {
		return Message{}, ErrMessageSize
	}
	if typ == TextMessage && !utf8.Valid(data) {
		return Message{}, ErrProtocol
	}
	return Message{typ: typ, data: append([]byte(nil), data...)}, nil
}

// Type returns whether the payload is text or binary.
func (m Message) Type() MessageType { return m.typ }

// Bytes returns a copy of the payload so callers cannot mutate the stored snapshot.
func (m Message) Bytes() []byte { return append([]byte(nil), m.data...) }

// String returns payload-safe metadata without exposing message contents.
func (m Message) String() string {
	kind := "unknown"
	if m.typ == TextMessage {
		kind = "text"
	} else if m.typ == BinaryMessage {
		kind = "binary"
	}
	return fmt.Sprintf("Message{type:%s size:%d}", kind, len(m.data))
}

// GoString keeps %#v formatting from exposing message payloads.
func (m Message) GoString() string { return m.String() }

// Close contains an RFC 6455 close status and a bounded UTF-8 reason.
type Close struct {
	code   websocket.StatusCode
	reason string
}

// NewClose validates a close status and snapshots its bounded reason.
func NewClose(code websocket.StatusCode, reason string) (Close, error) {
	close := Close{code: code, reason: reason}
	if err := close.validate(); err != nil {
		return Close{}, err
	}
	return close, nil
}

// Code returns the validated protocol close status.
func (c Close) Code() websocket.StatusCode { return c.code }

// Reason returns the close reason for explicit transport use.
func (c Close) Reason() string { return c.reason }

// String omits the close reason to avoid accidental sensitive-data logging.
func (c Close) String() string {
	return fmt.Sprintf("Close{code:%d reason_bytes:%d}", c.code, len(c.reason))
}

// GoString keeps %#v formatting from exposing close reasons.
func (c Close) GoString() string { return c.String() }

func (c Close) validate() error {
	if c.code < 1000 || c.code > 4999 || c.code == 1004 || c.code == 1005 || c.code == 1006 || c.code == 1015 || c.code >= 1016 && c.code <= 2999 {
		return fmt.Errorf("%w: close status is reserved or outside protocol range", ErrProtocol)
	}
	if !utf8.ValidString(c.reason) || len(c.reason) > MaxCloseReasonBytes {
		return fmt.Errorf("%w: close reason is invalid or exceeds %d bytes", ErrProtocol, MaxCloseReasonBytes)
	}
	return nil
}

// Handler owns a session until it returns. Receive and Send apply the supplied context deadline.
type Handler func(context.Context, *Session) error

// Settings bounds listener exposure and per-connection resource use.
type Settings struct {
	Address string
	Path    string
	// AllowedOrigins is a bounded list of exact HTTP(S) origins; wildcards are rejected.
	AllowedOrigins []string
	// Subprotocols lists unique HTTP tokens the server may negotiate, in preference order.
	Subprotocols    []string
	MaxConnections  int
	MaxMessageBytes int
}

// NewSettings returns conservative listener defaults. An empty origin list permits only
// same-host origins; configure exact origins to permit cross-origin browser clients.
func NewSettings() Settings {
	return Settings{
		Address:         "127.0.0.1:8765",
		Path:            "/ws",
		MaxConnections:  256,
		MaxMessageBytes: 256 * 1024,
	}
}

func (s Settings) validate() error {
	if strings.TrimSpace(s.Address) == "" || !strings.HasPrefix(s.Path, "/") || len(s.Path) > maxPathBytes || strings.ContainsAny(s.Path, "?#") || strings.IndexFunc(s.Path, func(r rune) bool { return r < 0x21 || r > 0x7e }) >= 0 {
		return fmt.Errorf("%w: address or path is invalid", ErrProtocol)
	}
	if s.MaxConnections < 1 || s.MaxConnections > 65536 || s.MaxMessageBytes < 1 || s.MaxMessageBytes > MaxMessageBytes {
		return fmt.Errorf("%w: connection or message limit is outside supported bounds", ErrProtocol)
	}
	if len(s.AllowedOrigins) > maxAllowedOrigins || len(s.Subprotocols) > maxSubprotocols {
		return fmt.Errorf("%w: origin or subprotocol list exceeds its bound", ErrProtocol)
	}
	origins := make(map[string]struct{}, len(s.AllowedOrigins))
	for _, origin := range s.AllowedOrigins {
		u, err := url.Parse(origin)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.Hostname() == "" || strings.HasSuffix(u.Host, ":") || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" || strings.ContainsAny(u.Host, "*?[]") {
			return fmt.Errorf("%w: allowed origins must be absolute HTTP(S) origins", ErrProtocol)
		}
		key := strings.ToLower(origin)
		if _, duplicate := origins[key]; duplicate {
			return fmt.Errorf("%w: allowed origins must be unique", ErrProtocol)
		}
		origins[key] = struct{}{}
	}
	protocols := make(map[string]struct{}, len(s.Subprotocols))
	for _, protocol := range s.Subprotocols {
		if len(protocol) == 0 || len(protocol) > maxSubprotocolSize || !isHTTPToken(protocol) {
			return fmt.Errorf("%w: subprotocol must be a bounded HTTP token", ErrProtocol)
		}
		if _, duplicate := protocols[protocol]; duplicate {
			return fmt.Errorf("%w: subprotocols must be unique", ErrProtocol)
		}
		protocols[protocol] = struct{}{}
	}
	return nil
}

func isHTTPToken(value string) bool {
	for _, char := range []byte(value) {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || strings.ContainsRune("!#$%&'*+-.^_`|~", rune(char)) {
			continue
		}
		return false
	}
	return true
}

// Server provides a standalone HTTP listener with a bounded WebSocket endpoint.
type Server struct {
	settings Settings
	handler  Handler
	slots    chan struct{}
	mu       sync.Mutex
	active   map[*Session]struct{}
	server   *http.Server
	started  bool
	stopped  bool
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewServer validates configuration and constructs the standalone listener.
func NewServer(settings Settings, handler Handler) (*Server, error) {
	if err := settings.validate(); err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, fmt.Errorf("%w: handler is required", ErrProtocol)
	}
	settings.AllowedOrigins = append([]string(nil), settings.AllowedOrigins...)
	settings.Subprotocols = append([]string(nil), settings.Subprotocols...)
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		settings: settings,
		handler:  handler,
		slots:    make(chan struct{}, settings.MaxConnections),
		active:   make(map[*Session]struct{}),
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

// Handler returns the HTTP handler for mounting behind an ingress or another Go HTTP server.
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != s.settings.Path {
			http.NotFound(w, r)
			return
		}
		s.mu.Lock()
		if s.stopped {
			s.mu.Unlock()
			http.Error(w, "socket server is shutting down", http.StatusServiceUnavailable)
			return
		}
		select {
		case s.slots <- struct{}{}:
		default:
			s.mu.Unlock()
			http.Error(w, "socket capacity reached", http.StatusServiceUnavailable)
			return
		}
		s.mu.Unlock()
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			OriginPatterns:  s.settings.AllowedOrigins,
			Subprotocols:    s.settings.Subprotocols,
			CompressionMode: websocket.CompressionDisabled,
		})
		if err != nil {
			<-s.slots
			return
		}
		conn.SetReadLimit(int64(s.settings.MaxMessageBytes))
		session := &Session{conn: conn, maxBytes: s.settings.MaxMessageBytes}
		s.mu.Lock()
		if s.stopped {
			s.mu.Unlock()
			_ = conn.CloseNow()
			<-s.slots
			return
		}
		s.active[session] = struct{}{}
		s.mu.Unlock()
		defer func() {
			s.mu.Lock()
			delete(s.active, session)
			s.mu.Unlock()
			_ = session.Close(Close{code: websocket.StatusNormalClosure})
			<-s.slots
		}()
		if err := s.handler(s.ctx, session); err != nil {
			_ = session.Close(Close{code: websocket.StatusInternalError, reason: "handler failed"})
			slog.Error("WebSocket application handler failed")
		}
	})
}

// Serve owns listener lifecycle and blocks until Shutdown is called.
func (s *Server) Serve(listener net.Listener) error {
	s.mu.Lock()
	if s.started || s.stopped {
		s.mu.Unlock()
		return fmt.Errorf("%w: server is already started or has been shut down", ErrProtocol)
	}
	s.started = true
	s.server = &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 10 * time.Second}
	httpServer := s.server
	s.mu.Unlock()
	err := httpServer.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// ListenAndServe binds the configured address and blocks until Shutdown is called.
func (s *Server) ListenAndServe() error {
	listener, err := net.Listen("tcp", s.settings.Address)
	if err != nil {
		return err
	}
	return s.Serve(listener)
}

// Shutdown stops accepting new connections, cancels handlers, and closes active sessions.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.stopped = true
	httpServer := s.server
	active := make([]*Session, 0, len(s.active))
	for session := range s.active {
		active = append(active, session)
	}
	s.mu.Unlock()
	s.cancel()
	var shutdownErr error
	if httpServer != nil {
		shutdownErr = httpServer.Shutdown(ctx)
	}
	var closeGroup sync.WaitGroup
	for _, session := range active {
		closeGroup.Add(1)
		go func(session *Session) {
			defer closeGroup.Done()
			_ = session.Close(Close{code: websocket.StatusGoingAway, reason: "server shutting down"})
		}(session)
	}
	done := make(chan struct{})
	go func() {
		closeGroup.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		for _, session := range active {
			session.Abort()
		}
		if shutdownErr == nil {
			shutdownErr = ctx.Err()
		}
	}
	return shutdownErr
}

// Session adapts one coder/websocket connection to the Orbit socket message contract.
type Session struct {
	conn     *websocket.Conn
	maxBytes int
	readMu   sync.Mutex
	closed   atomic.Bool
	closeMu  sync.Mutex
}

// Receive returns one complete bounded message. Only one concurrent Receive is permitted; callers
// that need fan-in should serialize reads in their handler.
func (s *Session) Receive(ctx context.Context) (Message, error) {
	s.readMu.Lock()
	defer s.readMu.Unlock()
	if s.closed.Load() {
		return Message{}, ErrClosed
	}
	typ, data, err := s.conn.Read(ctx)
	if err != nil {
		s.closed.Store(true)
		if websocket.CloseStatus(err) == websocket.StatusMessageTooBig {
			return Message{}, ErrMessageSize
		}
		return Message{}, ErrClosed
	}
	if len(data) > s.maxBytes {
		_ = s.Close(Close{code: websocket.StatusMessageTooBig, reason: "message too large"})
		return Message{}, ErrMessageSize
	}
	var messageType MessageType
	switch typ {
	case websocket.MessageText:
		if !utf8.Valid(data) {
			return Message{}, ErrProtocol
		}
		messageType = TextMessage
	case websocket.MessageBinary:
		messageType = BinaryMessage
	default:
		return Message{}, ErrProtocol
	}
	return Message{typ: messageType, data: append([]byte(nil), data...)}, nil
}

// Send applies the caller's context to the transport write and never logs or formats payloads.
func (s *Session) Send(ctx context.Context, message Message) error {
	if s.closed.Load() {
		return ErrClosed
	}
	if len(message.data) > s.maxBytes {
		return ErrMessageSize
	}
	if message.typ != TextMessage && message.typ != BinaryMessage {
		return ErrProtocol
	}
	if message.typ == TextMessage && !utf8.Valid(message.data) {
		return ErrProtocol
	}
	typ := websocket.MessageText
	if message.typ == BinaryMessage {
		typ = websocket.MessageBinary
	}
	payload := append([]byte(nil), message.data...)
	if err := s.conn.Write(ctx, typ, payload); err != nil {
		s.closed.Store(true)
		return ErrClosed
	}
	return nil
}

// Close performs a bounded close handshake and is safe to call more than once.
func (s *Session) Close(close Close) error {
	if err := close.validate(); err != nil {
		return err
	}
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed.Swap(true) {
		return nil
	}
	if err := s.conn.Close(close.code, close.reason); err != nil {
		_ = s.conn.CloseNow()
		return ErrClosed
	}
	return nil
}

// Abort closes the underlying connection without waiting for a peer close frame.
func (s *Session) Abort() {
	s.closed.Store(true)
	_ = s.conn.CloseNow()
}
