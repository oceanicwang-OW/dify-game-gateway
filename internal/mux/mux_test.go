package mux

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	gatewaypb "dify_gateway/api/proto"

	"dify_gateway/internal/codec"
)

func chunk(reqID, delta string) *gatewaypb.ServerEnvelope {
	return &gatewaypb.ServerEnvelope{
		Body: &gatewaypb.ServerEnvelope_Chunk{Chunk: &gatewaypb.ChatChunk{RequestId: reqID, Delta: delta}},
	}
}

// TestConnWriterSerializesConcurrentSends proves concurrent Send calls produce
// cleanly-decodable, non-interleaved frames (PDR §3.3 write serialization).
func TestConnWriterSerializesConcurrentSends(t *testing.T) {
	var buf bytes.Buffer
	w := NewConnWriter(&buf)

	const writers, perWriter = 8, 25
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			reqID := "req-" + string(rune('A'+id))
			for j := 0; j < perWriter; j++ {
				if err := w.Send(chunk(reqID, "delta")); err != nil {
					t.Errorf("Send error: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	// Every frame must decode to a valid ServerEnvelope; corruption from
	// interleaving would surface as a decode error or wrong count.
	counts := map[string]int{}
	for i := 0; i < writers*perWriter; i++ {
		env, err := codec.ReadServerEnvelope(&buf)
		if err != nil {
			t.Fatalf("frame %d decode error: %v", i, err)
		}
		counts[env.GetChunk().GetRequestId()]++
	}
	if len(counts) != writers {
		t.Fatalf("distinct request_ids = %d, want %d", len(counts), writers)
	}
	for reqID, n := range counts {
		if n != perWriter {
			t.Fatalf("request %s got %d frames, want %d", reqID, n, perWriter)
		}
	}
}

func TestRegistryBeginStopAndDuplicate(t *testing.T) {
	r := NewRegistry()
	ctx, done, ok := r.Begin(context.Background(), "req-1")
	if !ok {
		t.Fatal("first Begin ok = false, want true")
	}
	if r.InflightCount() != 1 {
		t.Fatalf("inflight = %d, want 1", r.InflightCount())
	}

	// Duplicate while in flight is rejected.
	if _, _, ok := r.Begin(context.Background(), "req-1"); ok {
		t.Fatal("duplicate Begin ok = true, want false")
	}

	// Stop cancels the request's context.
	if !r.Stop("req-1") {
		t.Fatal("Stop(req-1) = false, want true")
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("ctx not cancelled after Stop")
	}
	if r.Stop("missing") {
		t.Fatal("Stop(missing) = true, want false")
	}

	done()
	if r.InflightCount() != 0 {
		t.Fatalf("inflight after done = %d, want 0", r.InflightCount())
	}

	// Same id can be reused after completion.
	if _, _, ok := r.Begin(context.Background(), "req-1"); !ok {
		t.Fatal("reuse Begin ok = false, want true")
	}
}

func TestRegistryCancelAllAndWait(t *testing.T) {
	r := NewRegistry()
	const n = 5
	released := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		ctx, done, ok := r.Begin(context.Background(), "req-"+string(rune('0'+i)))
		if !ok {
			t.Fatalf("Begin %d ok = false", i)
		}
		go func() {
			<-ctx.Done()
			done()
			released <- struct{}{}
		}()
	}

	r.CancelAll()

	waited := make(chan struct{})
	go func() { r.Wait(); close(waited) }()
	select {
	case <-waited:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after CancelAll")
	}
	if r.InflightCount() != 0 {
		t.Fatalf("inflight after CancelAll/Wait = %d, want 0", r.InflightCount())
	}
}
