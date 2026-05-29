package listener

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"runtime"
	"testing"
	"time"

	gatewaypb "dify_gateway/api/proto"

	"dify_gateway/internal/codec"
	"dify_gateway/internal/session"
)

// --- test handler ---------------------------------------------------------

type testHandler struct {
	chunksPerReq int
	stepDelay    time.Duration
	blockChat    bool // if true, HandleChat blocks until ctx is cancelled
}

func (h *testHandler) Authenticate(_ context.Context, sess *session.Session, req *gatewaypb.AuthRequest) (bool, string) {
	if req.GetSessionToken() == "" {
		return false, "missing token"
	}
	sess.Bind(req.GetPlayerId())
	return true, ""
}

func (h *testHandler) HandleChat(ctx context.Context, _ *session.Session, req *gatewaypb.ChatRequest, send func(*gatewaypb.ServerEnvelope) error) {
	if h.blockChat {
		<-ctx.Done()
		return
	}
	reqID := req.GetRequestId()
	for i := 0; i < h.chunksPerReq; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}
		_ = send(&gatewaypb.ServerEnvelope{Body: &gatewaypb.ServerEnvelope_Chunk{
			Chunk: &gatewaypb.ChatChunk{RequestId: reqID, Delta: fmt.Sprintf("d%d", i)},
		}})
		if h.stepDelay > 0 {
			time.Sleep(h.stepDelay)
		}
	}
	_ = send(&gatewaypb.ServerEnvelope{Body: &gatewaypb.ServerEnvelope_Done{
		Done: &gatewaypb.ChatDone{RequestId: reqID, ConversationId: "conv-" + reqID},
	}})
}

// --- helpers --------------------------------------------------------------

func newServed(t *testing.T, h Handler) (client net.Conn, served chan struct{}, cancel context.CancelFunc) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	ctx, cancelFn := context.WithCancel(context.Background())
	srv := New(h, discardLogger())
	srv.SetIdleTimeout(5 * time.Second)

	served = make(chan struct{})
	go func() {
		srv.ServeConn(ctx, serverConn)
		close(served)
	}()
	return clientConn, served, cancelFn
}

func authenticate(t *testing.T, c net.Conn) {
	t.Helper()
	if err := codec.WriteEnvelope(c, &gatewaypb.ClientEnvelope{Body: &gatewaypb.ClientEnvelope_Auth{
		Auth: &gatewaypb.AuthRequest{SessionToken: "tok", PlayerId: "player-1"},
	}}); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	env, err := codec.ReadServerEnvelope(c)
	if err != nil {
		t.Fatalf("read auth result: %v", err)
	}
	if !env.GetAuthResult().GetOk() {
		t.Fatalf("auth not ok: %q", env.GetAuthResult().GetReason())
	}
}

// --- acceptance: concurrent multiplexing, no interleave, routable ---------

func TestConcurrentRequestsRoutableAndNotInterleaved(t *testing.T) {
	const reqs, chunks = 6, 10
	client, served, cancel := newServed(t, &testHandler{chunksPerReq: chunks, stepDelay: time.Millisecond})
	defer cancel()
	authenticate(t, client)

	// Fire several chat requests with distinct request_ids back to back.
	reqIDs := make([]string, reqs)
	for i := 0; i < reqs; i++ {
		reqIDs[i] = fmt.Sprintf("req-%d", i)
		if err := codec.WriteEnvelope(client, &gatewaypb.ClientEnvelope{Body: &gatewaypb.ClientEnvelope_Chat{
			Chat: &gatewaypb.ChatRequest{RequestId: reqIDs[i], NpcId: "npc", Query: "hi"},
		}}); err != nil {
			t.Fatalf("write chat %d: %v", i, err)
		}
	}

	// Each request yields `chunks` ChatChunk + 1 ChatDone.
	deltas := map[string][]string{}
	dones := map[string]bool{}
	total := reqs * (chunks + 1)
	for i := 0; i < total; i++ {
		env, err := codec.ReadServerEnvelope(client)
		if err != nil {
			t.Fatalf("read frame %d: %v", i, err)
		}
		switch body := env.GetBody().(type) {
		case *gatewaypb.ServerEnvelope_Chunk:
			id := body.Chunk.GetRequestId()
			if dones[id] {
				t.Fatalf("chunk for %s arrived after its done", id)
			}
			deltas[id] = append(deltas[id], body.Chunk.GetDelta())
		case *gatewaypb.ServerEnvelope_Done:
			dones[body.Done.GetRequestId()] = true
		default:
			t.Fatalf("unexpected envelope %T", body)
		}
	}

	// Every request fully accounted for, deltas in order (proves per-request
	// ordering preserved and frames not interleaved/corrupted).
	for _, id := range reqIDs {
		if !dones[id] {
			t.Fatalf("request %s missing done", id)
		}
		if len(deltas[id]) != chunks {
			t.Fatalf("request %s got %d deltas, want %d", id, len(deltas[id]), chunks)
		}
		for j, d := range deltas[id] {
			if d != fmt.Sprintf("d%d", j) {
				t.Fatalf("request %s delta %d = %q, want d%d", id, j, d, j)
			}
		}
	}

	client.Close()
	waitClosed(t, served)
}

// --- acceptance: disconnect releases resources (no goroutine leak) --------

func TestDisconnectCancelsInflightAndReleases(t *testing.T) {
	client, served, cancel := newServed(t, &testHandler{blockChat: true})
	defer cancel()
	authenticate(t, client)

	before := runtime.NumGoroutine()
	if err := codec.WriteEnvelope(client, &gatewaypb.ClientEnvelope{Body: &gatewaypb.ClientEnvelope_Chat{
		Chat: &gatewaypb.ChatRequest{RequestId: "req-block", NpcId: "npc", Query: "hi"},
	}}); err != nil {
		t.Fatalf("write chat: %v", err)
	}
	// Give the server time to register and spawn the blocked chat goroutine.
	time.Sleep(50 * time.Millisecond)

	// Disconnect: ServeConn must cancel the in-flight request, wait for its
	// goroutine, and return — otherwise served never closes.
	client.Close()
	waitClosed(t, served)

	// Goroutine count should settle back (the blocked handler + read loop exit).
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before+2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if leaked := runtime.NumGoroutine() - before; leaked > 2 {
		t.Fatalf("goroutines leaked: +%d after disconnect", leaked)
	}
}

// --- access-layer behaviors ----------------------------------------------

func TestPingPong(t *testing.T) {
	client, served, cancel := newServed(t, &testHandler{})
	defer cancel()

	if err := codec.WriteEnvelope(client, &gatewaypb.ClientEnvelope{Body: &gatewaypb.ClientEnvelope_Ping{Ping: &gatewaypb.Ping{}}}); err != nil {
		t.Fatalf("write ping: %v", err)
	}
	env, err := codec.ReadServerEnvelope(client)
	if err != nil {
		t.Fatalf("read pong: %v", err)
	}
	if env.GetPong() == nil {
		t.Fatalf("expected pong, got %T", env.GetBody())
	}
	client.Close()
	waitClosed(t, served)
}

func TestChatBeforeAuthRejected(t *testing.T) {
	client, served, cancel := newServed(t, &testHandler{chunksPerReq: 1})
	defer cancel()

	if err := codec.WriteEnvelope(client, &gatewaypb.ClientEnvelope{Body: &gatewaypb.ClientEnvelope_Chat{
		Chat: &gatewaypb.ChatRequest{RequestId: "req-1", Query: "hi"},
	}}); err != nil {
		t.Fatalf("write chat: %v", err)
	}
	env, err := codec.ReadServerEnvelope(client)
	if err != nil {
		t.Fatalf("read error msg: %v", err)
	}
	em := env.GetError()
	if em == nil || em.GetCode() != "UNAUTHENTICATED" {
		t.Fatalf("expected UNAUTHENTICATED error, got %#v", env.GetBody())
	}
	client.Close()
	waitClosed(t, served)
}

func waitClosed(t *testing.T, served chan struct{}) {
	t.Helper()
	select {
	case <-served:
	case <-time.After(3 * time.Second):
		t.Fatal("ServeConn did not return after disconnect")
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
