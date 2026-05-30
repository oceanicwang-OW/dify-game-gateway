// Package loadtest holds the M5-T2 load and stability harness. It drives the
// full access stack — codec framing, the TCP listener, per-connection and
// per-request goroutines, the mux, and the real Dify client — over many
// concurrent in-process connections (net.Pipe) against a mock Dify, then
// measures first-token latency percentiles, the error rate, and goroutine
// leakage after teardown (PDR ch.11: first-token P95 and zero leaks).
//
// Run: go test ./internal/loadtest -run TestLoad -v
// The leak/load test is skipped under -short.
package loadtest

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gatewaypb "dify_gateway/api/proto"

	"dify_gateway/internal/codec"
	"dify_gateway/internal/dify"
	"dify_gateway/internal/difymock"
	"dify_gateway/internal/listener"
	"dify_gateway/internal/moderation"
	"dify_gateway/internal/pipeline"
)

// --- minimal stand-ins so the load focuses on the access/stream machinery ---

type stubAuth struct{}

// Verify treats the token as the player id, so each connection is its own player.
func (stubAuth) Verify(_ context.Context, token string) (string, error) { return token, nil }

type stubLimiter struct{}

func (stubLimiter) Allow(context.Context, string, int) (bool, error) { return true, nil }
func (stubLimiter) Record(context.Context, string, int) error        { return nil }
func (stubLimiter) RecordFailure(context.Context) error              { return nil }
func (stubLimiter) Release(context.Context) error                    { return nil }

// stubStore returns a fixed conversation so chats skip the creation-lock path.
type stubStore struct{}

func (stubStore) GetConversation(context.Context, string, string) (string, error) {
	return "conv", nil
}
func (stubStore) SetConversation(context.Context, string, string, string, time.Duration) error {
	return nil
}
func (stubStore) DeleteConversation(context.Context, string, string) error { return nil }
func (stubStore) AcquireConversationLock(context.Context, string, string, time.Duration) (func(), error) {
	return func() {}, nil
}

type stubAssembler struct{}

func (stubAssembler) Build(context.Context, string, string) (map[string]string, error) {
	return map[string]string{}, nil
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// buildServer wires the real listener + pipeline against a mock Dify whose chat
// streams one short sentence then a usage-bearing message_end.
func buildServer(t *testing.T) (*listener.Server, *difymock.Server) {
	t.Helper()
	mock := difymock.New("k")
	mock.SetChat(difymock.ChatBehavior{Events: []difymock.Event{
		{Event: "message", TaskID: "task", ConversationID: "conv", Answer: "Hello there."},
		{Event: "message_end", ConversationID: "conv", TotalTokens: 11},
	}})

	hc := mock.Client()
	// Disable keep-alives so idle-connection goroutines don't linger and create
	// false-positive leaks; each request opens and closes its own connection.
	hc.Transport.(*http.Transport).DisableKeepAlives = true

	h := pipeline.New(pipeline.Config{
		Authenticator:    stubAuth{},
		Limiter:          stubLimiter{},
		Store:            stubStore{},
		ContextAssembler: stubAssembler{},
		Moderator:        moderation.AllowAll{},
		DifyClient:       dify.NewClient(mock.URL, "k", hc),
	})
	srv := listener.New(h, discardLogger())
	srv.SetIdleTimeout(30 * time.Second)
	return srv, mock
}

// runConn opens one connection, authenticates, runs `turns` chat turns, and
// records each turn's first-token latency. It returns the count of failed turns.
func runConn(t *testing.T, ctx context.Context, srv *listener.Server, wg *sync.WaitGroup, player string, turns int, rec *latencyRecorder) int {
	clientConn, serverConn := net.Pipe()
	wg.Add(1)
	go func() {
		defer wg.Done()
		srv.ServeConn(ctx, serverConn)
	}()
	defer clientConn.Close()

	if err := authenticate(clientConn, player); err != nil {
		t.Errorf("auth: %v", err)
		return turns
	}

	failed := 0
	for i := 0; i < turns; i++ {
		_ = clientConn.SetDeadline(time.Now().Add(5 * time.Second))
		reqID := fmt.Sprintf("%s-%d", player, i)
		start := time.Now()
		if err := codec.WriteEnvelope(clientConn, chatEnvelope(reqID)); err != nil {
			failed++
			continue
		}
		ok, firstToken := readTurn(clientConn, start)
		if !ok {
			failed++
			continue
		}
		rec.add(firstToken)
	}
	return failed
}

func authenticate(c net.Conn, player string) error {
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
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

func chatEnvelope(reqID string) *gatewaypb.ClientEnvelope {
	return &gatewaypb.ClientEnvelope{Body: &gatewaypb.ClientEnvelope_Chat{
		Chat: &gatewaypb.ChatRequest{RequestId: reqID, NpcId: "npc", Query: "hello"},
	}}
}

// readTurn reads frames until a terminal one, returning whether the turn
// succeeded and the latency to the first ChatChunk.
func readTurn(c net.Conn, start time.Time) (ok bool, firstToken time.Duration) {
	for {
		env, err := codec.ReadServerEnvelope(c)
		if err != nil {
			return false, 0
		}
		switch body := env.GetBody().(type) {
		case *gatewaypb.ServerEnvelope_Chunk:
			if firstToken == 0 {
				firstToken = time.Since(start)
			}
		case *gatewaypb.ServerEnvelope_Done:
			return true, firstToken
		case *gatewaypb.ServerEnvelope_Blocked:
			return true, firstToken
		case *gatewaypb.ServerEnvelope_Error:
			_ = body
			return false, firstToken
		}
	}
}

type latencyRecorder struct {
	mu   sync.Mutex
	data []time.Duration
}

func (r *latencyRecorder) add(d time.Duration) {
	r.mu.Lock()
	r.data = append(r.data, d)
	r.mu.Unlock()
}

func (r *latencyRecorder) percentile(p float64) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.data) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), r.data...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func TestLoadConcurrentConversationsNoLeak(t *testing.T) {
	if testing.Short() {
		t.Skip("load test skipped under -short")
	}

	const (
		conns     = 64
		turns     = 15
		leakSlack = 4 // tolerate a few transient runtime goroutines
	)

	srv, mock := buildServer(t)
	defer mock.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	// Warm up one full connection so lazy goroutines (mock server, transport)
	// exist before we record the baseline.
	warm := &latencyRecorder{}
	runConn(t, ctx, srv, &wg, "warmup", 2, warm)
	wg.Wait()
	runtime.GC()
	baseline := runtime.NumGoroutine()

	// Load phase: many concurrent connections, each holding a sustained chat.
	rec := &latencyRecorder{}
	var failed int64
	var workers sync.WaitGroup
	startAll := time.Now()
	for i := 0; i < conns; i++ {
		workers.Add(1)
		go func(id int) {
			defer workers.Done()
			f := runConn(t, ctx, srv, &wg, fmt.Sprintf("player-%d", id), turns, rec)
			atomic.AddInt64(&failed, int64(f))
		}(i)
	}
	workers.Wait()
	wg.Wait() // all per-connection server goroutines have returned
	elapsed := time.Since(startAll)

	total := conns * turns
	errRate := float64(failed) / float64(total)
	t.Logf("load: %d conns x %d turns = %d turns in %s (%.0f turns/s)",
		conns, turns, total, elapsed.Round(time.Millisecond), float64(total)/elapsed.Seconds())
	t.Logf("first-token latency: p50=%s p95=%s p99=%s",
		rec.percentile(0.50).Round(time.Microsecond),
		rec.percentile(0.95).Round(time.Microsecond),
		rec.percentile(0.99).Round(time.Microsecond))
	t.Logf("errors: %d/%d (%.4f)", failed, total, errRate)

	if errRate > 0 {
		t.Errorf("error rate = %.4f, want 0 when Dify is healthy", errRate)
	}

	// Leak check: after teardown the goroutine count must return to baseline.
	got := waitForGoroutines(baseline+leakSlack, 3*time.Second)
	t.Logf("goroutines: baseline=%d after-load=%d", baseline, got)
	if got > baseline+leakSlack {
		t.Errorf("goroutine leak: baseline=%d, after load+teardown=%d (slack=%d)", baseline, got, leakSlack)
	}
}

// waitForGoroutines polls until the goroutine count drops to <= target or the
// timeout elapses, returning the final count.
func waitForGoroutines(target int, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for {
		runtime.GC()
		n := runtime.NumGoroutine()
		if n <= target || time.Now().After(deadline) {
			return n
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// BenchmarkChatTurn measures one full chat turn (request -> done) through the
// listener stack, for tracking per-turn latency and allocations over time.
func BenchmarkChatTurn(b *testing.B) {
	t := &testing.T{}
	srv, mock := buildServer(t)
	defer mock.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientConn, serverConn := net.Pipe()
	go srv.ServeConn(ctx, serverConn)
	defer clientConn.Close()
	if err := authenticate(clientConn, "bench"); err != nil {
		b.Fatalf("auth: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = clientConn.SetDeadline(time.Now().Add(5 * time.Second))
		reqID := fmt.Sprintf("bench-%d", i)
		if err := codec.WriteEnvelope(clientConn, chatEnvelope(reqID)); err != nil {
			b.Fatalf("write: %v", err)
		}
		if ok, _ := readTurn(clientConn, time.Now()); !ok {
			b.Fatalf("turn %d failed", i)
		}
	}
	b.StopTimer()
}
