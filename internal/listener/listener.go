// Package listener implements the gateway access layer (PDR §2.2, §4.1): a TCP
// listener that runs one goroutine per connection, enforces the heartbeat/idle
// timeout, serializes writes, and multiplexes concurrent requests by
// request_id. Per-message business logic is delegated to a Handler so this
// package stays transport-only and independently testable.
package listener

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	gatewaypb "dify_gateway/api/proto"

	"dify_gateway/internal/codec"
	"dify_gateway/internal/mux"
	"dify_gateway/internal/session"
	"dify_gateway/internal/telemetry"
)

// DefaultIdleTimeout disconnects a connection after this much inactivity
// (PDR §4.1: 60s no activity). Clients ping every ~15s to keep it alive.
const DefaultIdleTimeout = 60 * time.Second

// Handler contains the business logic invoked by the access layer. Both methods
// are implemented by later milestones (auth = M2-T3, chat = M4); the listener
// only handles transport, framing, heartbeat, and multiplexing.
type Handler interface {
	// Authenticate verifies an AuthRequest and, on success, should call
	// sess.Bind(playerID). It returns the result to relay as AuthResult.
	Authenticate(ctx context.Context, sess *session.Session, req *gatewaypb.AuthRequest) (ok bool, reason string)

	// HandleChat processes one chat request, emitting zero or more
	// ServerEnvelopes via send. ctx is cancelled when the request is stopped or
	// the connection closes; the implementation must return promptly on ctx.Done.
	HandleChat(ctx context.Context, sess *session.Session, req *gatewaypb.ChatRequest, send func(*gatewaypb.ServerEnvelope) error)

	// HandleReset clears the conversation mapping for the request's npc so the
	// next chat starts a fresh conversation, and acks via send.
	HandleReset(ctx context.Context, sess *session.Session, req *gatewaypb.ResetRequest, send func(*gatewaypb.ServerEnvelope) error)
}

// Server accepts client connections and serves them with the access protocol.
type Server struct {
	handler     Handler
	logger      *slog.Logger
	idleTimeout time.Duration
	connSeq     atomic.Uint64
}

// New creates a Server. A nil logger uses slog.Default().
func New(handler Handler, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		handler:     handler,
		logger:      logger,
		idleTimeout: DefaultIdleTimeout,
	}
}

// SetIdleTimeout overrides the heartbeat idle timeout (used by tests).
func (s *Server) SetIdleTimeout(d time.Duration) { s.idleTimeout = d }

// ListenAndServe listens on addr and serves connections until ctx is cancelled.
func (s *Server) ListenAndServe(ctx context.Context, addr string) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer ln.Close()

	// Unblock Accept when ctx is cancelled.
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	s.logger.Info("listener started", "addr", ln.Addr().String())
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return err
		}
		go s.ServeConn(ctx, conn)
	}
}

// ServeConn runs the access protocol on a single connection until the peer
// disconnects, the idle timeout fires, or ctx is cancelled. It is exported so
// it can be driven directly (e.g. over net.Pipe) in tests.
func (s *Server) ServeConn(ctx context.Context, conn net.Conn) {
	connID := s.nextConnID()
	sess := session.New(connID, conn.RemoteAddr().String())
	writer := mux.NewConnWriter(conn)
	registry := mux.NewRegistry()

	telemetry.ActiveConnections.Inc()
	s.logger.Info("connection opened", "conn_id", connID, "remote", sess.RemoteAddr)

	connCtx, cancelConn := context.WithCancel(ctx)
	defer func() {
		cancelConn()
		registry.CancelAll()
		registry.Wait() // no goroutine leak: wait for in-flight requests to exit
		_ = conn.Close()
		telemetry.ActiveConnections.Dec()
		s.logger.Info("connection closed", "conn_id", connID)
	}()

	for {
		if err := conn.SetReadDeadline(time.Now().Add(s.idleTimeout)); err != nil {
			return
		}
		env, err := codec.ReadClientEnvelope(conn)
		if err != nil {
			s.logReadExit(connID, err)
			return
		}
		if connCtx.Err() != nil {
			return
		}
		s.dispatch(connCtx, sess, writer, registry, env)
	}
}

func (s *Server) dispatch(ctx context.Context, sess *session.Session, writer *mux.ConnWriter, registry *mux.Registry, env *gatewaypb.ClientEnvelope) {
	switch body := env.GetBody().(type) {
	case *gatewaypb.ClientEnvelope_Ping:
		_ = writer.Send(pong())

	case *gatewaypb.ClientEnvelope_Auth:
		ok, reason := s.handler.Authenticate(ctx, sess, body.Auth)
		_ = writer.Send(authResult(ok, reason))

	case *gatewaypb.ClientEnvelope_Stop:
		registry.Stop(body.Stop.GetRequestId())

	case *gatewaypb.ClientEnvelope_Chat:
		s.handleChat(ctx, sess, writer, registry, body.Chat)

	case *gatewaypb.ClientEnvelope_Reset_:
		s.handleReset(ctx, sess, writer, body.Reset_)

	default:
		// Unknown/empty body: ignore to stay forward-compatible with new frames.
	}
}

func (s *Server) handleChat(ctx context.Context, sess *session.Session, writer *mux.ConnWriter, registry *mux.Registry, req *gatewaypb.ChatRequest) {
	reqID := req.GetRequestId()
	if reqID == "" {
		_ = writer.Send(errorMsg("", "BAD_REQUEST", "request_id is required"))
		return
	}
	if !sess.Authenticated() {
		_ = writer.Send(errorMsg(reqID, "UNAUTHENTICATED", "authenticate before chatting"))
		return
	}

	reqCtx, done, ok := registry.Begin(ctx, reqID)
	if !ok {
		_ = writer.Send(errorMsg(reqID, "BAD_REQUEST", "duplicate request_id in flight"))
		return
	}

	// One goroutine per request (PDR §3.3); writes go through the serialized
	// ConnWriter so concurrent requests' frames never interleave.
	go func() {
		defer done()
		s.handler.HandleChat(reqCtx, sess, req, writer.Send)
	}()
}

func (s *Server) handleReset(ctx context.Context, sess *session.Session, writer *mux.ConnWriter, req *gatewaypb.ResetRequest) {
	reqID := req.GetRequestId()
	if reqID == "" {
		_ = writer.Send(errorMsg("", "BAD_REQUEST", "request_id is required"))
		return
	}
	if !sess.Authenticated() {
		_ = writer.Send(errorMsg(reqID, "UNAUTHENTICATED", "authenticate before resetting"))
		return
	}
	// Reset is a fast state mutation (delete one mapping key); run it inline like
	// Auth rather than registering it as an in-flight request.
	s.handler.HandleReset(ctx, sess, req, writer.Send)
}

func (s *Server) logReadExit(connID string, err error) {
	switch {
	case errors.Is(err, io.EOF):
		s.logger.Info("connection EOF", "conn_id", connID)
	case errors.Is(err, io.ErrUnexpectedEOF):
		s.logger.Info("connection truncated", "conn_id", connID)
	default:
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			s.logger.Info("connection idle timeout", "conn_id", connID)
			return
		}
		s.logger.Warn("connection read error", "conn_id", connID, "err", err.Error())
	}
}

func (s *Server) nextConnID() string {
	return "conn-" + strconv.FormatUint(s.connSeq.Add(1), 10)
}
