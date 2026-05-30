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
	"math"
	"net"
	"net/http"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dify_gateway/internal/dify"
	"dify_gateway/internal/difymock"
	"dify_gateway/internal/listener"
	"dify_gateway/internal/moderation"
	"dify_gateway/internal/pipeline"
	"dify_gateway/internal/testsupport"
)

// buildServer wires the real listener + pipeline against a mock Dify whose chat
// streams one short sentence then a usage-bearing message_end. It takes a
// testing.TB so both the test and the benchmark can call it without fabricating
// a fake *testing.T.
func buildServer(t testing.TB) (*listener.Server, *difymock.Server) {
	t.Helper()
	mock := difymock.New("k")
	mock.SetChat(difymock.ChatBehavior{Events: []difymock.Event{
		{Event: "message", TaskID: "task", ConversationID: "conv", Answer: "Hello there."},
		{Event: "message_end", ConversationID: "conv", TotalTokens: 11},
	}})

	hc := mock.Client()
	// Disable keep-alives so idle-connection goroutines don't linger and create
	// false-positive leaks; each request opens and closes its own connection.
	tr, ok := hc.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("mock client transport is %T, want *http.Transport", hc.Transport)
	}
	tr.DisableKeepAlives = true

	h := pipeline.New(pipeline.Config{
		Authenticator:    testsupport.Auth{},
		Limiter:          testsupport.Limiter{},
		Store:            testsupport.Store{Conversation: "conv"},
		ContextAssembler: testsupport.Assembler{},
		Moderator:        moderation.AllowAll{},
		DifyClient:       dify.NewClient(mock.URL, "k", hc),
	})
	srv := listener.New(h, testsupport.DiscardLogger())
	srv.SetIdleTimeout(30 * time.Second)
	return srv, mock
}

// runConn opens one connection, authenticates, runs `turns` chat turns, and
// records each turn's first-token latency into rec (when rec is non-nil). It
// returns the count of failed turns.
func runConn(t *testing.T, ctx context.Context, srv *listener.Server, wg *sync.WaitGroup, player string, turns int, rec *latencyRecorder) int {
	clientConn, serverConn := net.Pipe()
	wg.Add(1)
	go func() {
		defer wg.Done()
		srv.ServeConn(ctx, serverConn)
	}()
	defer clientConn.Close()

	if err := testsupport.Authenticate(clientConn, player); err != nil {
		t.Errorf("auth: %v", err)
		return turns
	}

	failed := 0
	for i := 0; i < turns; i++ {
		reqID := fmt.Sprintf("%s-%d", player, i)
		start := time.Now()
		if err := testsupport.SendChat(clientConn, reqID, "npc", "hello"); err != nil {
			failed++
			continue
		}
		ok, firstToken, sawToken := testsupport.ReadTurn(clientConn, start)
		if !ok {
			failed++
			continue
		}
		if rec != nil && sawToken {
			rec.add(firstToken)
		}
	}
	return failed
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

// percentile returns the nearest-rank value for p in [0,1] (rank = ceil(p*n)),
// so high percentiles include the tail (p=1.0 returns the max).
func (r *latencyRecorder) percentile(p float64) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.data) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), r.data...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
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

	// Warm up one full connection (latencies discarded) so lazy goroutines
	// (mock server, transport) exist before we record the baseline.
	runConn(t, ctx, srv, &wg, "warmup", 2, nil)
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
// timeout elapses, returning the final count. It does not force GC: goroutines
// finish when their functions return, not on collection, so polling the count
// is what matters.
func waitForGoroutines(target int, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for {
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
	srv, mock := buildServer(b)
	defer mock.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientConn, serverConn := net.Pipe()
	go srv.ServeConn(ctx, serverConn)
	defer clientConn.Close()
	if err := testsupport.Authenticate(clientConn, "bench"); err != nil {
		b.Fatalf("auth: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reqID := fmt.Sprintf("bench-%d", i)
		if err := testsupport.SendChat(clientConn, reqID, "npc", "hello"); err != nil {
			b.Fatalf("write: %v", err)
		}
		if ok, _, _ := testsupport.ReadTurn(clientConn, time.Now()); !ok {
			b.Fatalf("turn %d failed", i)
		}
	}
	b.StopTimer()
}
