// Package testsupport provides shared no-op gateway dependencies and
// client-side protocol helpers for tests that drive the full access stack
// (the listener + pipeline) against a mock Dify — e.g. the load harness and
// future end-to-end tests. The dependency types are deliberately no-ops (they
// admit/record nothing); tests that need to assert on limiter/store behaviour
// use the recording fakes in their own package instead.
package testsupport

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	gatewaypb "dify_gateway/api/proto"

	"dify_gateway/internal/codec"
)

// handshakeTimeout bounds the client-side auth/turn reads so a stalled server
// fails the test fast instead of hanging the suite.
const handshakeTimeout = 5 * time.Second

// Auth treats the session token as the player id, so each connection is its
// own player without any real verification.
type Auth struct{}

func (Auth) Verify(_ context.Context, token string) (string, error) { return token, nil }

// Limiter is a no-op limiter that always admits and records nothing.
type Limiter struct{}

func (Limiter) Allow(context.Context, string, int) (bool, error) { return true, nil }
func (Limiter) Record(context.Context, string, int) error        { return nil }
func (Limiter) RecordFailure(context.Context) error              { return nil }
func (Limiter) Release(context.Context) error                    { return nil }

// Store returns Conversation for every lookup. A non-empty Conversation makes
// chats skip the creation-lock path; the zero value models a new conversation.
type Store struct{ Conversation string }

func (s Store) GetConversation(context.Context, string, string) (string, error) {
	return s.Conversation, nil
}
func (Store) SetConversation(context.Context, string, string, string, time.Duration) error {
	return nil
}
func (Store) DeleteConversation(context.Context, string, string) error { return nil }
func (Store) AcquireConversationLock(context.Context, string, string, time.Duration) (func(), error) {
	return func() {}, nil
}

// Assembler returns empty inputs.
type Assembler struct{}

func (Assembler) Build(context.Context, string, string) (map[string]string, error) {
	return map[string]string{}, nil
}

// DiscardLogger returns a logger that drops all output.
func DiscardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// Authenticate performs the auth handshake over c, returning an error if the
// gateway rejects it.
func Authenticate(c net.Conn, player string) error {
	_ = c.SetDeadline(time.Now().Add(handshakeTimeout))
	if err := codec.WriteEnvelope(c, &gatewaypb.ClientEnvelope{Body: &gatewaypb.ClientEnvelope_Auth{
		Auth: &gatewaypb.AuthRequest{SessionToken: player, PlayerId: player},
	}}); err != nil {
		return err
	}
	env, err := codec.ReadServerEnvelope(c)
	if err != nil {
		return err
	}
	if !env.GetAuthResult().GetOk() {
		return fmt.Errorf("auth rejected: %s", env.GetAuthResult().GetReason())
	}
	return nil
}

// SendChat writes one chat request over c.
func SendChat(c net.Conn, reqID, npc, query string) error {
	_ = c.SetDeadline(time.Now().Add(handshakeTimeout))
	return codec.WriteEnvelope(c, &gatewaypb.ClientEnvelope{Body: &gatewaypb.ClientEnvelope_Chat{
		Chat: &gatewaypb.ChatRequest{RequestId: reqID, NpcId: npc, Query: query},
	}})
}

// ReadTurn reads server frames until a terminal one (Done/Blocked/Error). ok is
// false on a read error or an Error frame. sawToken reports whether a ChatChunk
// arrived; firstToken is the latency from start to that first chunk and is only
// meaningful when sawToken is true.
func ReadTurn(c net.Conn, start time.Time) (ok bool, firstToken time.Duration, sawToken bool) {
	for {
		env, err := codec.ReadServerEnvelope(c)
		if err != nil {
			return false, 0, sawToken
		}
		switch env.GetBody().(type) {
		case *gatewaypb.ServerEnvelope_Chunk:
			if !sawToken {
				firstToken = time.Since(start)
				sawToken = true
			}
		case *gatewaypb.ServerEnvelope_Done, *gatewaypb.ServerEnvelope_Blocked:
			return true, firstToken, sawToken
		case *gatewaypb.ServerEnvelope_Error:
			return false, firstToken, sawToken
		}
	}
}
