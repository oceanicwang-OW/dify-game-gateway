package pipeline

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	gatewaypb "dify_gateway/api/proto"

	contextasm "dify_gateway/internal/context"
	"dify_gateway/internal/dify"
	"dify_gateway/internal/limiter"
	"dify_gateway/internal/moderation"
	"dify_gateway/internal/session"
)

type fakeAuth struct {
	playerID string
	err      error
	token    string
}

func (a *fakeAuth) Verify(_ context.Context, token string) (string, error) {
	a.token = token
	if a.err != nil {
		return "", a.err
	}
	return a.playerID, nil
}

type fakeLimiter struct {
	allowErr      error
	allowCalls    int
	recordCalls   []int
	failureCalls  int
	releaseCalls  int
	allowedPlayer string
}

func (l *fakeLimiter) Allow(_ context.Context, playerID string, _ int) (bool, error) {
	l.allowCalls++
	l.allowedPlayer = playerID
	if l.allowErr != nil {
		return false, l.allowErr
	}
	return true, nil
}

func (l *fakeLimiter) Record(_ context.Context, _ string, usedTokens int) error {
	l.recordCalls = append(l.recordCalls, usedTokens)
	return nil
}

func (l *fakeLimiter) RecordFailure(context.Context) error {
	l.failureCalls++
	return nil
}

func (l *fakeLimiter) Release(context.Context) error {
	l.releaseCalls++
	return nil
}

type fakeStore struct {
	conversation string
	gets         int
	sets         []string
	lockCalls    int
	lockTTL      time.Duration
	deletes      int
}

func (s *fakeStore) GetConversation(context.Context, string, string) (string, error) {
	s.gets++
	return s.conversation, nil
}

func (s *fakeStore) SetConversation(_ context.Context, _ string, _ string, convID string, _ time.Duration) error {
	s.sets = append(s.sets, convID)
	s.conversation = convID
	return nil
}

func (s *fakeStore) DeleteConversation(context.Context, string, string) error {
	s.deletes++
	s.conversation = ""
	return nil
}

func (s *fakeStore) AcquireConversationLock(_ context.Context, _ string, _ string, ttl time.Duration) (func(), error) {
	s.lockCalls++
	s.lockTTL = ttl
	return func() {}, nil
}

type fakeAssembler struct {
	inputs map[string]string
	err    error
}

func (a fakeAssembler) Build(context.Context, string, string) (map[string]string, error) {
	if a.err != nil {
		return nil, a.err
	}
	return a.inputs, nil
}

type fakeDify struct {
	req          dify.ChatReq
	deltas       []string
	result       dify.ChatResult
	err          error
	callCount    int
	beforeReturn func() // invoked after deltas, before returning (e.g. to cancel ctx)
	stopCalls    int
	stopTaskID   string
	stopUser     string
}

func (d *fakeDify) ChatStream(ctx context.Context, req dify.ChatReq, onEvent func(taskID, convID string), onDelta func(delta string)) (dify.ChatResult, error) {
	d.callCount++
	d.req = req
	if onEvent != nil {
		onEvent("task-1", "conv-live")
	}
	for _, delta := range d.deltas {
		if onDelta != nil {
			onDelta(delta)
		}
	}
	if d.beforeReturn != nil {
		d.beforeReturn()
	}
	if err := ctx.Err(); err != nil {
		return d.result, err
	}
	return d.result, d.err
}

func (d *fakeDify) Stop(_ context.Context, taskID, user string) error {
	d.stopCalls++
	d.stopTaskID = taskID
	d.stopUser = user
	return nil
}

type sentEnvelope struct {
	kind string
	id   string
	text string
	code string
	toks uint32
}

// captureSend records every ServerEnvelope into the returned slice. Optional
// onChunk hooks fire after a ChatChunk is recorded (e.g. to cancel mid-stream).
func captureSend(t *testing.T, onChunk ...func()) (func(*gatewaypb.ServerEnvelope) error, *[]sentEnvelope) {
	t.Helper()
	var sent []sentEnvelope
	return func(env *gatewaypb.ServerEnvelope) error {
		switch body := env.GetBody().(type) {
		case *gatewaypb.ServerEnvelope_Chunk:
			sent = append(sent, sentEnvelope{kind: "chunk", id: body.Chunk.GetRequestId(), text: body.Chunk.GetDelta()})
			for _, hook := range onChunk {
				hook()
			}
		case *gatewaypb.ServerEnvelope_Done:
			sent = append(sent, sentEnvelope{kind: "done", id: body.Done.GetRequestId(), text: body.Done.GetConversationId(), toks: body.Done.GetTotalTokens()})
		case *gatewaypb.ServerEnvelope_Error:
			sent = append(sent, sentEnvelope{kind: "error", id: body.Error.GetRequestId(), code: body.Error.GetCode(), text: body.Error.GetMessage()})
		case *gatewaypb.ServerEnvelope_Blocked:
			sent = append(sent, sentEnvelope{kind: "blocked", id: body.Blocked.GetRequestId(), text: body.Blocked.GetFallback()})
		default:
			t.Fatalf("unexpected envelope %T", body)
		}
		return nil
	}, &sent
}

func authedSession(playerID string) *session.Session {
	s := session.New("conn-1", "pipe")
	s.Bind(playerID)
	return s
}

func TestAuthenticateBindsVerifiedPlayer(t *testing.T) {
	authn := &fakeAuth{playerID: "player-verified"}
	h := New(Config{Authenticator: authn})
	sess := session.New("conn-1", "pipe")

	ok, reason := h.Authenticate(context.Background(), sess, &gatewaypb.AuthRequest{SessionToken: "jwt", PlayerId: "client-claimed"})

	if !ok || reason != "" {
		t.Fatalf("Authenticate() = (%v, %q), want success", ok, reason)
	}
	if authn.token != "jwt" {
		t.Fatalf("auth token = %q, want jwt", authn.token)
	}
	if got := sess.PlayerID(); got != "player-verified" {
		t.Fatalf("session player = %q, want verified player", got)
	}
}

func TestAuthenticateFailsWhenAuthenticatorMissing(t *testing.T) {
	h := New(Config{})
	ok, reason := h.Authenticate(context.Background(), session.New("conn-1", "pipe"), &gatewaypb.AuthRequest{SessionToken: "jwt"})
	if ok || reason == "" {
		t.Fatalf("Authenticate() = (%v, %q), want failure reason", ok, reason)
	}
}

func TestHandleChatStreamsDifyThroughModerationAndRecordsUsage(t *testing.T) {
	lim := &fakeLimiter{}
	store := &fakeStore{}
	asm := fakeAssembler{inputs: map[string]string{contextasm.VarPlayerLevel: "12"}}
	difyClient := &fakeDify{
		deltas: []string{"Hello", " hero."},
		result: dify.ChatResult{ConversationID: "conv-new", TotalTokens: 37},
	}
	h := New(Config{
		Limiter:          lim,
		Store:            store,
		ContextAssembler: asm,
		Moderator:        moderation.NewPolicyModerator(moderation.NewKeywordPolicy(nil)),
		DifyClient:       difyClient,
	})
	send, sent := captureSend(t)

	h.HandleChat(context.Background(), authedSession("player-1"), &gatewaypb.ChatRequest{
		RequestId: "req-1",
		NpcId:     "npc-blacksmith",
		Query:     "hello",
	}, send)

	want := []sentEnvelope{
		{kind: "chunk", id: "req-1", text: "Hello hero."},
		{kind: "done", id: "req-1", text: "conv-new", toks: 37},
	}
	if !reflect.DeepEqual(*sent, want) {
		t.Fatalf("sent = %#v, want %#v", *sent, want)
	}
	if difyClient.callCount != 1 {
		t.Fatalf("Dify call count = %d, want 1", difyClient.callCount)
	}
	if difyClient.req.User != "player-1:npc-blacksmith" {
		t.Fatalf("Dify user = %q", difyClient.req.User)
	}
	if difyClient.req.Inputs[contextasm.VarPlayerLevel] != "12" {
		t.Fatalf("Dify inputs = %#v", difyClient.req.Inputs)
	}
	if store.lockCalls != 1 || !reflect.DeepEqual(store.sets, []string{"conv-new"}) {
		t.Fatalf("store lock/set = (%d, %#v), want lock and conv-new", store.lockCalls, store.sets)
	}
	if !reflect.DeepEqual(lim.recordCalls, []int{37}) {
		t.Fatalf("limiter records = %#v, want [37]", lim.recordCalls)
	}
}

func TestHandleChatBlocksInputWithoutCallingDify(t *testing.T) {
	lim := &fakeLimiter{}
	difyClient := &fakeDify{}
	h := New(Config{
		Limiter:          lim,
		Store:            &fakeStore{},
		ContextAssembler: fakeAssembler{inputs: map[string]string{}},
		Moderator:        moderation.NewPolicyModerator(moderation.NewKeywordPolicy([]string{"blocked"})).WithFallbacks("input fallback", "output fallback"),
		DifyClient:       difyClient,
	})
	send, sent := captureSend(t)

	h.HandleChat(context.Background(), authedSession("player-1"), &gatewaypb.ChatRequest{
		RequestId: "req-1",
		NpcId:     "npc",
		Query:     "blocked input",
	}, send)

	want := []sentEnvelope{{kind: "blocked", id: "req-1", text: "input fallback"}}
	if !reflect.DeepEqual(*sent, want) {
		t.Fatalf("sent = %#v, want %#v", *sent, want)
	}
	if difyClient.callCount != 0 {
		t.Fatalf("Dify called %d times, want 0", difyClient.callCount)
	}
	if !reflect.DeepEqual(lim.recordCalls, []int{0}) {
		t.Fatalf("limiter records = %#v, want release with 0 tokens", lim.recordCalls)
	}
}

func TestHandleChatMapsRateLimitToClientError(t *testing.T) {
	h := New(Config{
		Limiter:          &fakeLimiter{allowErr: limiter.ErrRateLimited},
		Store:            &fakeStore{},
		ContextAssembler: fakeAssembler{inputs: map[string]string{}},
		Moderator:        moderation.AllowAll{},
		DifyClient:       &fakeDify{},
	})
	send, sent := captureSend(t)

	h.HandleChat(context.Background(), authedSession("player-1"), &gatewaypb.ChatRequest{
		RequestId: "req-1",
		NpcId:     "npc",
		Query:     "hello",
	}, send)

	want := []sentEnvelope{{kind: "error", id: "req-1", code: "RATE_LIMITED"}}
	if len(*sent) != 1 || (*sent)[0].kind != want[0].kind || (*sent)[0].code != want[0].code {
		t.Fatalf("sent = %#v, want RATE_LIMITED error", *sent)
	}
}

func TestHandleChatMapsMessageReplaceToBlocked(t *testing.T) {
	lim := &fakeLimiter{}
	h := New(Config{
		Limiter:          lim,
		Store:            &fakeStore{conversation: "conv-existing"},
		ContextAssembler: fakeAssembler{inputs: map[string]string{}},
		Moderator:        moderation.AllowAll{},
		DifyClient:       &fakeDify{err: &dify.MessageReplaceError{Fallback: "replacement"}},
	})
	send, sent := captureSend(t)

	h.HandleChat(context.Background(), authedSession("player-1"), &gatewaypb.ChatRequest{
		RequestId: "req-1",
		NpcId:     "npc",
		Query:     "hello",
	}, send)

	want := []sentEnvelope{{kind: "blocked", id: "req-1", text: "replacement"}}
	if !reflect.DeepEqual(*sent, want) {
		t.Fatalf("sent = %#v, want %#v", *sent, want)
	}
	if !reflect.DeepEqual(lim.recordCalls, []int{0}) {
		t.Fatalf("limiter records = %#v, want release with 0 tokens", lim.recordCalls)
	}
}

func TestHandleChatMapsUpstreamErrorAndRecordsFailure(t *testing.T) {
	lim := &fakeLimiter{}
	h := New(Config{
		Limiter:          lim,
		Store:            &fakeStore{conversation: "conv-existing"},
		ContextAssembler: fakeAssembler{inputs: map[string]string{}},
		Moderator:        moderation.AllowAll{},
		DifyClient:       &fakeDify{err: errors.New("upstream failed")},
	})
	send, sent := captureSend(t)

	h.HandleChat(context.Background(), authedSession("player-1"), &gatewaypb.ChatRequest{
		RequestId: "req-1",
		NpcId:     "npc",
		Query:     "hello",
	}, send)

	if len(*sent) != 1 || (*sent)[0].kind != "error" || (*sent)[0].code != "UPSTREAM_ERROR" {
		t.Fatalf("sent = %#v, want UPSTREAM_ERROR", *sent)
	}
	if lim.failureCalls != 1 {
		t.Fatalf("failureCalls = %d, want 1", lim.failureCalls)
	}
}

func TestHandleChatBlocksUnsafeStreamingOutput(t *testing.T) {
	lim := &fakeLimiter{}
	h := New(Config{
		Limiter:          lim,
		Store:            &fakeStore{conversation: "conv-existing"},
		ContextAssembler: fakeAssembler{inputs: map[string]string{}},
		Moderator:        moderation.NewPolicyModerator(moderation.NewKeywordPolicy([]string{"unsafe"})).WithFallbacks("input fallback", "output fallback"),
		DifyClient: &fakeDify{
			deltas: []string{"this is un", "safe."},
			result: dify.ChatResult{ConversationID: "conv-existing", TotalTokens: 12},
		},
	})
	send, sent := captureSend(t)

	h.HandleChat(context.Background(), authedSession("player-1"), &gatewaypb.ChatRequest{
		RequestId: "req-1",
		NpcId:     "npc",
		Query:     "hello",
	}, send)

	want := []sentEnvelope{{kind: "blocked", id: "req-1", text: "output fallback"}}
	if !reflect.DeepEqual(*sent, want) {
		t.Fatalf("sent = %#v, want %#v", *sent, want)
	}
	if !reflect.DeepEqual(lim.recordCalls, []int{12}) {
		t.Fatalf("limiter records = %#v, want record final usage", lim.recordCalls)
	}
}

func TestHandleChatExtendsCreationLockToRequestDeadline(t *testing.T) {
	store := &fakeStore{} // empty mapping -> new conversation path takes the lock
	h := New(Config{
		Limiter:          &fakeLimiter{},
		Store:            store,
		ContextAssembler: fakeAssembler{inputs: map[string]string{}},
		Moderator:        moderation.AllowAll{},
		DifyClient:       &fakeDify{result: dify.ChatResult{ConversationID: "conv-new"}},
	})
	send, _ := captureSend(t)

	// A deadline well beyond the default 15s lock TTL must widen the lease so
	// the lock outlives a long stream.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	h.HandleChat(ctx, authedSession("player-1"), &gatewaypb.ChatRequest{
		RequestId: "req-1",
		NpcId:     "npc",
		Query:     "hello",
	}, send)

	if store.lockCalls != 1 {
		t.Fatalf("lockCalls = %d, want 1", store.lockCalls)
	}
	if store.lockTTL <= defaultConversationLockTTL {
		t.Fatalf("lockTTL = %s, want > default %s (extended to deadline)", store.lockTTL, defaultConversationLockTTL)
	}
}

func TestHandleChatUsesDefaultLockTTLWithoutDeadline(t *testing.T) {
	store := &fakeStore{}
	h := New(Config{
		Limiter:          &fakeLimiter{},
		Store:            store,
		ContextAssembler: fakeAssembler{inputs: map[string]string{}},
		Moderator:        moderation.AllowAll{},
		DifyClient:       &fakeDify{result: dify.ChatResult{ConversationID: "conv-new"}},
	})
	send, _ := captureSend(t)

	h.HandleChat(context.Background(), authedSession("player-1"), &gatewaypb.ChatRequest{
		RequestId: "req-1",
		NpcId:     "npc",
		Query:     "hello",
	}, send)

	if store.lockTTL != defaultConversationLockTTL {
		t.Fatalf("lockTTL = %s, want default %s", store.lockTTL, defaultConversationLockTTL)
	}
}

func TestHandleChatStopAbortsUpstreamAndIsNotAFailure(t *testing.T) {
	lim := &fakeLimiter{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	difyClient := &fakeDify{
		deltas:       []string{"Hel"},
		result:       dify.ChatResult{ConversationID: "conv-x", TotalTokens: 5},
		beforeReturn: cancel, // a StopRequest cancels the request context mid-stream
	}
	store := &fakeStore{} // new conversation; stop should persist the created id
	h := New(Config{
		Limiter:          lim,
		Store:            store,
		ContextAssembler: fakeAssembler{inputs: map[string]string{}},
		Moderator:        moderation.AllowAll{},
		DifyClient:       difyClient,
	})
	send, sent := captureSend(t)

	h.HandleChat(ctx, authedSession("player-1"), &gatewaypb.ChatRequest{
		RequestId: "req-1",
		NpcId:     "npc",
		Query:     "hello",
	}, send)

	// The cached task_id (from onEvent) is used to stop the upstream generation
	// with the same scoped user.
	if difyClient.stopCalls != 1 || difyClient.stopTaskID != "task-1" || difyClient.stopUser != "player-1:npc" {
		t.Fatalf("stop = (%d, %q, %q), want one stop for task-1 / player-1:npc",
			difyClient.stopCalls, difyClient.stopTaskID, difyClient.stopUser)
	}
	// A cancel is neither success nor failure: the slot is released neutrally,
	// with no circuit success (Record) or failure (RecordFailure).
	if lim.releaseCalls != 1 || len(lim.recordCalls) != 0 || lim.failureCalls != 0 {
		t.Fatalf("limiter = (release %d, record %#v, failure %d), want one neutral release only",
			lim.releaseCalls, lim.recordCalls, lim.failureCalls)
	}
	// The conversation Dify created before the stop is persisted, not orphaned.
	if !reflect.DeepEqual(store.sets, []string{"conv-x"}) {
		t.Fatalf("store.sets = %#v, want [conv-x] persisted on stop", store.sets)
	}
	// No error frame; a clean done closes the request.
	for _, e := range *sent {
		if e.kind == "error" {
			t.Fatalf("unexpected error frame on stop: %#v", *sent)
		}
	}
	if n := len(*sent); n == 0 || (*sent)[n-1].kind != "done" {
		t.Fatalf("sent = %#v, want trailing done", *sent)
	}
}

func TestHandleChatDeadlineIsAFailureNotAStop(t *testing.T) {
	lim := &fakeLimiter{}
	// A context that is already past its deadline: ChatStream returns the ctx
	// error and ctx.Err() == DeadlineExceeded, which must NOT be treated as a
	// client stop.
	ctx, cancel := context.WithTimeout(context.Background(), -time.Second)
	defer cancel()
	difyClient := &fakeDify{deltas: []string{"Hi"}}
	h := New(Config{
		Limiter:          lim,
		Store:            &fakeStore{conversation: "conv-x"},
		ContextAssembler: fakeAssembler{inputs: map[string]string{}},
		Moderator:        moderation.AllowAll{},
		DifyClient:       difyClient,
	})
	send, sent := captureSend(t)

	h.HandleChat(ctx, authedSession("player-1"), &gatewaypb.ChatRequest{
		RequestId: "req-1",
		NpcId:     "npc",
		Query:     "hello",
	}, send)

	if difyClient.stopCalls != 0 {
		t.Fatalf("stopCalls = %d, want 0 (a timeout is not a client stop)", difyClient.stopCalls)
	}
	if lim.failureCalls != 1 || lim.releaseCalls != 0 {
		t.Fatalf("limiter = (failure %d, release %d), want one circuit failure", lim.failureCalls, lim.releaseCalls)
	}
	if n := len(*sent); n != 1 || (*sent)[0].kind != "error" || (*sent)[0].code != "UPSTREAM_TIMEOUT" {
		t.Fatalf("sent = %#v, want UPSTREAM_TIMEOUT error", *sent)
	}
}

func TestHandleResetClearsMappingSoNextChatIsNewConversation(t *testing.T) {
	store := &fakeStore{conversation: "conv-existing"}
	h := New(Config{
		Limiter:          &fakeLimiter{},
		Store:            store,
		ContextAssembler: fakeAssembler{inputs: map[string]string{}},
		Moderator:        moderation.AllowAll{},
		DifyClient:       &fakeDify{result: dify.ChatResult{ConversationID: "conv-new"}},
	})
	send, sent := captureSend(t)

	h.HandleReset(context.Background(), authedSession("player-1"), &gatewaypb.ResetRequest{
		RequestId: "req-1",
		NpcId:     "npc",
	}, send)

	if store.deletes != 1 {
		t.Fatalf("deletes = %d, want 1", store.deletes)
	}
	want := []sentEnvelope{{kind: "done", id: "req-1"}}
	if !reflect.DeepEqual(*sent, want) {
		t.Fatalf("reset ack = %#v, want %#v", *sent, want)
	}

	// The next chat must take the creation lock (mapping was cleared).
	send2, _ := captureSend(t)
	h.HandleChat(context.Background(), authedSession("player-1"), &gatewaypb.ChatRequest{
		RequestId: "req-2",
		NpcId:     "npc",
		Query:     "hello again",
	}, send2)
	if store.lockCalls != 1 {
		t.Fatalf("lockCalls = %d after reset, want 1 (new conversation)", store.lockCalls)
	}
}

func TestHandleResetRequiresNpcID(t *testing.T) {
	store := &fakeStore{}
	h := New(Config{Limiter: &fakeLimiter{}, Store: store, DifyClient: &fakeDify{}})
	send, sent := captureSend(t)

	h.HandleReset(context.Background(), authedSession("player-1"), &gatewaypb.ResetRequest{RequestId: "req-1"}, send)

	if store.deletes != 0 {
		t.Fatalf("deletes = %d, want 0 when npc_id missing", store.deletes)
	}
	if len(*sent) != 1 || (*sent)[0].kind != "error" || (*sent)[0].code != "BAD_REQUEST" {
		t.Fatalf("sent = %#v, want BAD_REQUEST error", *sent)
	}
}

func TestClientErrorCodeMapsKnownFailures(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "player rate limit", err: limiter.ErrRateLimited, want: "RATE_LIMITED"},
		{name: "daily budget", err: limiter.ErrBudgetExceeded, want: "RATE_LIMITED"},
		{name: "inflight full", err: limiter.ErrInflightFull, want: "RATE_LIMITED"},
		{name: "circuit open", err: limiter.ErrCircuitOpen, want: "RATE_LIMITED"},
		{name: "deadline", err: context.DeadlineExceeded, want: "UPSTREAM_TIMEOUT"},
		{name: "upstream 429", err: &dify.UpstreamError{StatusCode: 429}, want: "RATE_LIMITED"},
		{name: "upstream 504", err: &dify.UpstreamError{StatusCode: 504}, want: "UPSTREAM_TIMEOUT"},
		{name: "stream rate limit", err: &dify.StreamError{Code: "rate_limited"}, want: "RATE_LIMITED"},
		{name: "stream incomplete", err: dify.ErrStreamIncomplete, want: "UPSTREAM_ERROR"},
		{name: "generic upstream", err: errors.New("boom"), want: "UPSTREAM_ERROR"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := clientErrorCode(tt.err); got != tt.want {
				t.Fatalf("clientErrorCode(%T) = %q, want %q", tt.err, got, tt.want)
			}
		})
	}
}
