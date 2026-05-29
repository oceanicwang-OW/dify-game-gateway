// Package mux provides single-connection write serialization and per-request
// multiplexing for the access layer (PDR §3.3). ConnWriter guarantees frames
// from concurrent per-request goroutines never interleave; Registry tracks
// in-flight requests by request_id so Stop can target one and a disconnect can
// cancel all of them without leaking goroutines.
package mux

import (
	"context"
	"io"
	"sync"

	gatewaypb "dify_gateway/api/proto"

	"dify_gateway/internal/codec"
)

// ConnWriter serializes ServerEnvelope writes on one connection.
type ConnWriter struct {
	mu sync.Mutex
	w  io.Writer
}

// NewConnWriter wraps a connection's write side.
func NewConnWriter(w io.Writer) *ConnWriter {
	return &ConnWriter{w: w}
}

// Send writes one ServerEnvelope as a length-prefixed frame, holding the
// connection write lock so frames are never interleaved.
func (c *ConnWriter) Send(env *gatewaypb.ServerEnvelope) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return codec.WriteEnvelope(c.w, env)
}

// Registry tracks in-flight requests on a connection by request_id.
type Registry struct {
	mu       sync.Mutex
	inflight map[string]context.CancelFunc
	wg       sync.WaitGroup
}

// NewRegistry creates an empty request registry.
func NewRegistry() *Registry {
	return &Registry{inflight: make(map[string]context.CancelFunc)}
}

// Begin registers requestID and returns a child context plus a done func that
// the caller must invoke when the request finishes. ok is false if requestID is
// already in flight (duplicate), in which case ctx/done are nil and the caller
// should reject the request. The internal WaitGroup lets Wait block until all
// in-flight requests have called done.
func (r *Registry) Begin(parent context.Context, requestID string) (ctx context.Context, done func(), ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.inflight[requestID]; exists {
		return nil, nil, false
	}

	ctx, cancel := context.WithCancel(parent)
	r.inflight[requestID] = cancel
	r.wg.Add(1)

	var once sync.Once
	done = func() {
		once.Do(func() {
			cancel()
			r.mu.Lock()
			// Safe to delete by key: Begin rejects a duplicate request_id while
			// one is in flight, so this entry is unique to the active request
			// and no newer request can have taken the key before done runs.
			delete(r.inflight, requestID)
			r.mu.Unlock()
			r.wg.Done()
		})
	}
	return ctx, done, true
}

// Stop cancels the in-flight request with requestID. It reports whether a
// matching request was found.
func (r *Registry) Stop(requestID string) bool {
	r.mu.Lock()
	cancel, ok := r.inflight[requestID]
	r.mu.Unlock()
	if ok {
		cancel()
	}
	return ok
}

// CancelAll cancels every in-flight request (used on disconnect).
func (r *Registry) CancelAll() {
	r.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(r.inflight))
	for _, cancel := range r.inflight {
		cancels = append(cancels, cancel)
	}
	r.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

// Wait blocks until all in-flight requests have completed (called done).
func (r *Registry) Wait() {
	r.wg.Wait()
}

// InflightCount returns the number of currently registered requests.
func (r *Registry) InflightCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.inflight)
}
