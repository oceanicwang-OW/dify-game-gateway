package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	gatewaypb "dify_gateway/api/proto"

	"dify_gateway/internal/dify"
	"dify_gateway/internal/difymock"
	"dify_gateway/internal/moderation"
)

// These tests exercise the full chat orchestration against a real dify.Client
// talking to a scriptable mock Dify over HTTP (PDR M5-T1): real SSE parsing,
// real retry/backoff, and the pipeline's error mapping end to end. Limiter/
// store/context use the package fakes; the integration surface under test is
// the gateway <-> Dify boundary.

func newIntegrationHandler(t *testing.T, mock *difymock.Server, lim *fakeLimiter, store *fakeStore, mod moderation.Moderator) *Handler {
	t.Helper()
	client := dify.NewClient(mock.URL, "test-key", mock.Client())
	return New(Config{
		Limiter:          lim,
		Store:            store,
		ContextAssembler: fakeAssembler{inputs: map[string]string{}},
		Moderator:        mod,
		DifyClient:       client,
	})
}

func chatText(sent []sentEnvelope) string {
	var b strings.Builder
	for _, e := range sent {
		if e.kind == "chunk" {
			b.WriteString(e.text)
		}
	}
	return b.String()
}

func TestIntegrationNormalStream(t *testing.T) {
	mock := difymock.New()
	defer mock.Close()
	mock.SetChat(difymock.ChatBehavior{Events: []difymock.Event{
		{Event: "message", TaskID: "task-1", ConversationID: "conv-1", Answer: "Hello"},
		{Event: "message", Answer: " world."},
		{Event: "message_end", ConversationID: "conv-1", TotalTokens: 7},
	}})

	lim := &fakeLimiter{}
	store := &fakeStore{conversation: "conv-1"}
	h := newIntegrationHandler(t, mock, lim, store, moderation.AllowAll{})
	send, sent := captureSend(t)

	h.HandleChat(context.Background(), authedSession("player-1"), &gatewaypb.ChatRequest{
		RequestId: "req-1", NpcId: "npc", Query: "hi",
	}, send)

	if got := chatText(*sent); got != "Hello world." {
		t.Fatalf("streamed text = %q, want %q", got, "Hello world.")
	}
	last := (*sent)[len(*sent)-1]
	if last.kind != "done" || last.text != "conv-1" || last.toks != 7 {
		t.Fatalf("final frame = %#v, want done conv-1 / 7 tokens", last)
	}
	if len(lim.recordCalls) != 1 || lim.recordCalls[0] != 7 {
		t.Fatalf("recordCalls = %#v, want [7]", lim.recordCalls)
	}
	// The real request reached the mock with the scoped user and streaming mode.
	reqs := mock.ChatRequests()
	if len(reqs) != 1 || reqs[0].User != "player-1:npc" || reqs[0].ResponseMode != "streaming" {
		t.Fatalf("mock chat request = %#v, want one streaming request for player-1:npc", reqs)
	}
	if reqs[0].Authorization != "Bearer test-key" {
		t.Fatalf("Authorization = %q, want Bearer test-key", reqs[0].Authorization)
	}
}

func TestIntegrationMessageReplaceBlocks(t *testing.T) {
	mock := difymock.New()
	defer mock.Close()
	mock.SetChat(difymock.ChatBehavior{Events: []difymock.Event{
		{Event: "message", TaskID: "task-1", ConversationID: "conv-1", Answer: "partial"},
		{Event: "message_replace", Answer: "内容已被替换"},
	}})

	lim := &fakeLimiter{}
	h := newIntegrationHandler(t, mock, lim, &fakeStore{conversation: "conv-1"}, moderation.AllowAll{})
	send, sent := captureSend(t)

	h.HandleChat(context.Background(), authedSession("player-1"), &gatewaypb.ChatRequest{
		RequestId: "req-1", NpcId: "npc", Query: "hi",
	}, send)

	if n := len(*sent); n == 0 || (*sent)[n-1].kind != "blocked" || (*sent)[n-1].text != "内容已被替换" {
		t.Fatalf("sent = %#v, want trailing blocked with replacement", *sent)
	}
	if lim.failureCalls != 0 {
		t.Fatalf("failureCalls = %d, want 0 (replace is not an upstream failure)", lim.failureCalls)
	}
}

func TestIntegrationErrorEvent(t *testing.T) {
	mock := difymock.New()
	defer mock.Close()
	mock.SetChat(difymock.ChatBehavior{Events: []difymock.Event{
		{Event: "error", Code: "provider_error", Message: "boom"},
	}})

	lim := &fakeLimiter{}
	h := newIntegrationHandler(t, mock, lim, &fakeStore{conversation: "conv-1"}, moderation.AllowAll{})
	send, sent := captureSend(t)

	h.HandleChat(context.Background(), authedSession("player-1"), &gatewaypb.ChatRequest{
		RequestId: "req-1", NpcId: "npc", Query: "hi",
	}, send)

	if n := len(*sent); n != 1 || (*sent)[0].kind != "error" || (*sent)[0].code != "UPSTREAM_ERROR" {
		t.Fatalf("sent = %#v, want UPSTREAM_ERROR", *sent)
	}
	if lim.failureCalls != 1 {
		t.Fatalf("failureCalls = %d, want 1", lim.failureCalls)
	}
}

func TestIntegrationRateLimitRetriesThenSucceeds(t *testing.T) {
	mock := difymock.New()
	defer mock.Close()
	mock.SetChat(difymock.ChatBehavior{
		Status:     429,
		RetryAfter: "0",
		FailTimes:  1, // first attempt 429, retry succeeds
		Events: []difymock.Event{
			{Event: "message", TaskID: "task-1", ConversationID: "conv-1", Answer: "ok."},
			{Event: "message_end", ConversationID: "conv-1", TotalTokens: 3},
		},
	})

	lim := &fakeLimiter{}
	h := newIntegrationHandler(t, mock, lim, &fakeStore{conversation: "conv-1"}, moderation.AllowAll{})
	send, sent := captureSend(t)

	h.HandleChat(context.Background(), authedSession("player-1"), &gatewaypb.ChatRequest{
		RequestId: "req-1", NpcId: "npc", Query: "hi",
	}, send)

	if got := chatText(*sent); got != "ok." {
		t.Fatalf("streamed text = %q, want recovered after retry", got)
	}
	if last := (*sent)[len(*sent)-1]; last.kind != "done" {
		t.Fatalf("final frame = %#v, want done after retry recovery", last)
	}
	if n := len(mock.ChatRequests()); n != 2 {
		t.Fatalf("mock chat attempts = %d, want 2 (one 429 + one success)", n)
	}
}

func TestIntegrationServerErrorExhaustedMapsUpstreamError(t *testing.T) {
	mock := difymock.New()
	defer mock.Close()
	mock.SetChat(difymock.ChatBehavior{Status: 500, RetryAfter: "0"}) // every attempt 5xx

	lim := &fakeLimiter{}
	h := newIntegrationHandler(t, mock, lim, &fakeStore{conversation: "conv-1"}, moderation.AllowAll{})
	send, sent := captureSend(t)

	h.HandleChat(context.Background(), authedSession("player-1"), &gatewaypb.ChatRequest{
		RequestId: "req-1", NpcId: "npc", Query: "hi",
	}, send)

	if n := len(*sent); n != 1 || (*sent)[0].kind != "error" || (*sent)[0].code != "UPSTREAM_ERROR" {
		t.Fatalf("sent = %#v, want UPSTREAM_ERROR after retry exhaustion", *sent)
	}
	if n := len(mock.ChatRequests()); n != 3 {
		t.Fatalf("mock chat attempts = %d, want 3 (retry exhaustion)", n)
	}
	if lim.failureCalls != 1 {
		t.Fatalf("failureCalls = %d, want 1", lim.failureCalls)
	}
}

func TestIntegrationUpstreamTimeout(t *testing.T) {
	mock := difymock.New()
	defer mock.Close()
	mock.SetChat(difymock.ChatBehavior{Hang: true}) // never responds

	lim := &fakeLimiter{}
	h := newIntegrationHandler(t, mock, lim, &fakeStore{conversation: "conv-1"}, moderation.AllowAll{})
	send, sent := captureSend(t)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	h.HandleChat(ctx, authedSession("player-1"), &gatewaypb.ChatRequest{
		RequestId: "req-1", NpcId: "npc", Query: "hi",
	}, send)

	if n := len(*sent); n != 1 || (*sent)[0].kind != "error" || (*sent)[0].code != "UPSTREAM_TIMEOUT" {
		t.Fatalf("sent = %#v, want UPSTREAM_TIMEOUT", *sent)
	}
	if lim.failureCalls != 1 {
		t.Fatalf("failureCalls = %d, want 1 (a timeout is an upstream failure)", lim.failureCalls)
	}
}

func TestIntegrationStopMidStreamAbortsUpstream(t *testing.T) {
	mock := difymock.New()
	defer mock.Close()
	mock.SetChat(difymock.ChatBehavior{Events: []difymock.Event{
		{Event: "message", TaskID: "task-9", ConversationID: "conv-1", Answer: "First sentence."},
		// The terminal event is delayed; the test cancels before it arrives.
		{Event: "message_end", Delay: 2 * time.Second, ConversationID: "conv-1", TotalTokens: 5},
	}})

	lim := &fakeLimiter{}
	h := newIntegrationHandler(t, mock, lim, &fakeStore{conversation: "conv-1"}, moderation.AllowAll{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var sent []sentEnvelope
	send := func(env *gatewaypb.ServerEnvelope) error {
		switch body := env.GetBody().(type) {
		case *gatewaypb.ServerEnvelope_Chunk:
			sent = append(sent, sentEnvelope{kind: "chunk", text: body.Chunk.GetDelta()})
			cancel() // simulate a client StopRequest after the first delta
		case *gatewaypb.ServerEnvelope_Done:
			sent = append(sent, sentEnvelope{kind: "done"})
		case *gatewaypb.ServerEnvelope_Error:
			sent = append(sent, sentEnvelope{kind: "error", code: body.Error.GetCode()})
		case *gatewaypb.ServerEnvelope_Blocked:
			sent = append(sent, sentEnvelope{kind: "blocked"})
		}
		return nil
	}

	h.HandleChat(ctx, authedSession("player-1"), &gatewaypb.ChatRequest{
		RequestId: "req-1", NpcId: "npc", Query: "hi",
	}, send)

	stops := mock.StopCalls()
	if len(stops) != 1 || stops[0].TaskID != "task-9" || stops[0].User != "player-1:npc" {
		t.Fatalf("stop calls = %#v, want one stop for task-9 / player-1:npc", stops)
	}
	for _, e := range sent {
		if e.kind == "error" {
			t.Fatalf("unexpected error frame on stop: %#v", sent)
		}
	}
	if lim.failureCalls != 0 || lim.releaseCalls != 1 {
		t.Fatalf("limiter = (failure %d, release %d), want neutral release", lim.failureCalls, lim.releaseCalls)
	}
}
