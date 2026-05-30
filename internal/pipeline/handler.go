// Package pipeline wires the completed gateway modules into the M4-T1 chat
// request path. Transport stays in listener; this package owns business
// orchestration and protocol-level error mapping.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	gatewaypb "dify_gateway/api/proto"

	"dify_gateway/internal/auth"
	contextasm "dify_gateway/internal/context"
	"dify_gateway/internal/dify"
	"dify_gateway/internal/limiter"
	"dify_gateway/internal/moderation"
	"dify_gateway/internal/session"
	"dify_gateway/internal/telemetry"
)

const (
	defaultConversationTTL     = 24 * time.Hour
	defaultConversationLockTTL = 15 * time.Second
	maxQueryRunes              = 4096
	// accountingTimeout bounds the post-stream limiter bookkeeping. It runs on
	// a context detached from the request so a client disconnect cannot abort
	// the in-flight slot release, while still capping how long a slow Redis can
	// hold the handler.
	accountingTimeout = 5 * time.Second
)

type DifyClient interface {
	ChatStream(ctx context.Context, req dify.ChatReq, onEvent func(taskID, convID string), onDelta func(delta string)) (dify.ChatResult, error)
	// Stop aborts an in-flight streaming generation upstream so Dify stops
	// producing (and billing) tokens; dropping the local connection alone does
	// not guarantee that. taskID comes from the ChatStream onEvent callback.
	Stop(ctx context.Context, taskID, user string) error
}

type SessionStore interface {
	GetConversation(ctx context.Context, playerID, npcID string) (string, error)
	SetConversation(ctx context.Context, playerID, npcID, convID string, ttl time.Duration) error
	DeleteConversation(ctx context.Context, playerID, npcID string) error
	AcquireConversationLock(ctx context.Context, playerID, npcID string, ttl time.Duration) (func(), error)
}

type Config struct {
	Authenticator       auth.Authenticator
	Limiter             limiter.Limiter
	Store               SessionStore
	ContextAssembler    contextasm.ContextAssembler
	Moderator           moderation.Moderator
	DifyClient          DifyClient
	ConversationTTL     time.Duration
	ConversationLockTTL time.Duration
	Logger              *slog.Logger
}

type Handler struct {
	auth                auth.Authenticator
	limiter             limiter.Limiter
	store               SessionStore
	contextAssembler    contextasm.ContextAssembler
	moderator           moderation.Moderator
	dify                DifyClient
	conversationTTL     time.Duration
	conversationLockTTL time.Duration
	logger              *slog.Logger
}

func New(cfg Config) *Handler {
	if cfg.ConversationTTL == 0 {
		cfg.ConversationTTL = defaultConversationTTL
	}
	if cfg.ConversationLockTTL == 0 {
		cfg.ConversationLockTTL = defaultConversationLockTTL
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Moderator == nil {
		cfg.Moderator = moderation.AllowAll{}
	}
	return &Handler{
		auth:                cfg.Authenticator,
		limiter:             cfg.Limiter,
		store:               cfg.Store,
		contextAssembler:    cfg.ContextAssembler,
		moderator:           cfg.Moderator,
		dify:                cfg.DifyClient,
		conversationTTL:     cfg.ConversationTTL,
		conversationLockTTL: cfg.ConversationLockTTL,
		logger:              cfg.Logger,
	}
}

func (h *Handler) Authenticate(ctx context.Context, sess *session.Session, req *gatewaypb.AuthRequest) (bool, string) {
	if h.auth == nil {
		return false, "authenticator unavailable"
	}
	return auth.BindSession(ctx, h.auth, sess, req.GetSessionToken())
}

func (h *Handler) HandleChat(ctx context.Context, sess *session.Session, req *gatewaypb.ChatRequest, send func(*gatewaypb.ServerEnvelope) error) {
	reqID := req.GetRequestId()
	playerID := sess.PlayerID()
	npcID := strings.TrimSpace(req.GetNpcId())
	query := strings.TrimSpace(req.GetQuery())

	if npcID == "" {
		h.send(send, errorMsg(reqID, "BAD_REQUEST", "npc_id is required"))
		telemetry.RequestsTotal.WithLabelValues("bad_request").Inc()
		return
	}
	if query == "" {
		h.send(send, errorMsg(reqID, "BAD_REQUEST", "query is required"))
		telemetry.RequestsTotal.WithLabelValues("bad_request").Inc()
		return
	}
	if queryTooLong(query) {
		h.send(send, errorMsg(reqID, "BAD_REQUEST", "query is too long"))
		telemetry.RequestsTotal.WithLabelValues("bad_request").Inc()
		return
	}
	if err := h.ready(); err != nil {
		h.logger.Error("pipeline not ready", "err", err.Error())
		h.send(send, errorMsg(reqID, "INTERNAL", "gateway is not ready"))
		telemetry.RequestsTotal.WithLabelValues("error").Inc()
		return
	}

	if ok, err := h.limiter.Allow(ctx, playerID, 0); err != nil || !ok {
		if err == nil {
			err = limiter.ErrRateLimited
		}
		h.send(send, errorMsg(reqID, clientErrorCode(err), clientErrorMessage(err)))
		telemetry.RequestsTotal.WithLabelValues("rate_limited").Inc()
		return
	}

	conversationID, unlock, err := h.resolveConversation(ctx, playerID, npcID, req.GetConversationId())
	if unlock != nil {
		defer unlock()
	}
	if err != nil {
		_ = h.recordSuccess(ctx, playerID, 0)
		h.send(send, errorMsg(reqID, "INTERNAL", "conversation state unavailable"))
		telemetry.RequestsTotal.WithLabelValues("error").Inc()
		return
	}

	inputs, err := h.contextAssembler.Build(ctx, playerID, npcID)
	if err != nil {
		_ = h.recordSuccess(ctx, playerID, 0)
		h.send(send, errorMsg(reqID, "INTERNAL", "context unavailable"))
		telemetry.RequestsTotal.WithLabelValues("error").Inc()
		return
	}

	if ok, fallback := h.moderator.CheckInput(ctx, query); !ok {
		_ = h.recordSuccess(ctx, playerID, 0)
		h.send(send, blocked(reqID, fallback))
		telemetry.RequestsTotal.WithLabelValues("blocked").Inc()
		return
	}

	start := time.Now()
	firstDelta := true
	filter := moderation.NewOutputFilter(h.moderator)
	outputBlocked := false
	user := userID(playerID, npcID)
	// taskID is captured from the first Dify message event so a StopRequest can
	// abort the generation upstream while the stream is still active. onEvent and
	// onDelta run synchronously on this goroutine, so a plain assignment is safe.
	var taskID string
	onEvent := func(tid, _ string) { taskID = tid }
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	onDelta := func(delta string) {
		if outputBlocked {
			return
		}
		if firstDelta {
			firstDelta = false
			telemetry.FirstTokenLatencySeconds.Observe(time.Since(start).Seconds())
		}
		emit, blocked, fallback := filter.Push(ctx, delta)
		if blocked {
			outputBlocked = true
			h.send(send, blockedEnvelope(reqID, fallback))
			telemetry.RequestsTotal.WithLabelValues("blocked").Inc()
			cancelStream()
			return
		}
		if emit != "" {
			h.send(send, chunk(reqID, emit))
		}
	}

	result, err := h.dify.ChatStream(streamCtx, dify.ChatReq{
		Query:          query,
		Inputs:         inputs,
		User:           user,
		ConversationID: conversationID,
	}, onEvent, onDelta)
	telemetry.UpstreamLatencySeconds.Observe(time.Since(start).Seconds())
	if err != nil {
		if replace, ok := messageReplace(err); ok {
			_ = h.recordSuccess(ctx, playerID, result.TotalTokens)
			h.send(send, blocked(reqID, replace))
			telemetry.RequestsTotal.WithLabelValues("blocked").Inc()
			return
		}
		if outputBlocked {
			_ = h.recordSuccess(ctx, playerID, result.TotalTokens)
			return
		}
		if errors.Is(ctx.Err(), context.Canceled) {
			// The request context was cancelled by a client StopRequest (or a
			// disconnect), which aborted the local stream — distinct from a
			// deadline (context.DeadlineExceeded), which is a real upstream
			// timeout and falls through to the error path below. Ask Dify to
			// stop generating upstream, persist any conversation it created so
			// the next message can continue it, and release the slot WITHOUT a
			// circuit success/failure: a cancel is neither, so it must not skew
			// the breaker or surface an error frame.
			h.stopUpstream(taskID, user)
			h.releaseSlot(ctx)
			h.persistConversation(ctx, playerID, npcID, result.ConversationID)
			h.send(send, done(reqID, result.ConversationID, result.TotalTokens))
			telemetry.RequestsTotal.WithLabelValues("stopped").Inc()
			return
		}
		h.recordFailure(ctx)
		h.send(send, errorMsg(reqID, clientErrorCode(err), clientErrorMessage(err)))
		telemetry.RequestsTotal.WithLabelValues("error").Inc()
		return
	}

	if err := h.recordSuccess(ctx, playerID, result.TotalTokens); err != nil {
		h.send(send, errorMsg(reqID, "INTERNAL", "usage accounting failed"))
		telemetry.RequestsTotal.WithLabelValues("error").Inc()
		return
	}
	if result.TotalTokens > 0 {
		telemetry.TokensTotal.Add(float64(result.TotalTokens))
	}
	if result.ConversationID != "" {
		if err := h.store.SetConversation(ctx, playerID, npcID, result.ConversationID, h.conversationTTL); err != nil {
			h.send(send, errorMsg(reqID, "INTERNAL", "conversation state unavailable"))
			telemetry.RequestsTotal.WithLabelValues("error").Inc()
			return
		}
	}
	if outputBlocked {
		return
	}
	if emit, blocked, fallback := filter.Flush(ctx); blocked {
		h.send(send, blockedEnvelope(reqID, fallback))
		telemetry.RequestsTotal.WithLabelValues("blocked").Inc()
		return
	} else if emit != "" {
		h.send(send, chunk(reqID, emit))
	}

	h.send(send, done(reqID, result.ConversationID, result.TotalTokens))
	telemetry.RequestsTotal.WithLabelValues("ok").Inc()
}

func (h *Handler) ready() error {
	switch {
	case h.limiter == nil:
		return fmt.Errorf("limiter is nil")
	case h.store == nil:
		return fmt.Errorf("store is nil")
	case h.contextAssembler == nil:
		return fmt.Errorf("context assembler is nil")
	case h.dify == nil:
		return fmt.Errorf("dify client is nil")
	default:
		return nil
	}
}

func queryTooLong(query string) bool {
	count := 0
	for range query {
		count++
		if count > maxQueryRunes {
			return true
		}
	}
	return false
}

func (h *Handler) resolveConversation(ctx context.Context, playerID, npcID, clientConvID string) (conversationID string, unlock func(), err error) {
	conversationID, err = h.store.GetConversation(ctx, playerID, npcID)
	if err != nil {
		return "", nil, err
	}
	if conversationID != "" {
		return conversationID, nil, nil
	}
	if trimmed := strings.TrimSpace(clientConvID); trimmed != "" {
		// The client-supplied conversation_id is only a fallback when the
		// server has no stored mapping. It is NOT trusted for ownership: every
		// upstream call is scoped to userID(playerID, npcID) (see HandleChat),
		// so Dify rejects a conversation that belongs to a different user. A
		// guessed/foreign id therefore cannot leak another player's context.
		return trimmed, nil, nil
	}

	// Hold the creation lock until the new conversation id is persisted. The
	// lock must outlive the streaming call, so extend its TTL to the request
	// deadline (the stream cannot exceed it); otherwise a long stream could
	// outlast the lock and let a concurrent first message create a duplicate
	// conversation.
	lockTTL := h.conversationLockTTL
	if deadline, ok := ctx.Deadline(); ok {
		if untilDeadline := time.Until(deadline) + time.Second; untilDeadline > lockTTL {
			lockTTL = untilDeadline
		}
	}
	unlock, err = h.store.AcquireConversationLock(ctx, playerID, npcID, lockTTL)
	if err != nil {
		return "", nil, err
	}
	conversationID, err = h.store.GetConversation(ctx, playerID, npcID)
	if err != nil {
		unlock()
		return "", nil, err
	}
	if conversationID != "" {
		unlock()
		return conversationID, nil, nil
	}
	return conversationID, unlock, nil
}

func (h *Handler) recordSuccess(ctx context.Context, playerID string, tokens int) error {
	actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), accountingTimeout)
	defer cancel()
	return h.limiter.Record(actx, playerID, tokens)
}

func (h *Handler) recordFailure(ctx context.Context) {
	actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), accountingTimeout)
	defer cancel()
	if err := h.limiter.RecordFailure(actx); err != nil {
		h.logger.Warn("record upstream failure", "err", err.Error())
	}
}

// releaseSlot frees the in-flight slot for a cancelled request without recording
// a circuit success or failure. Runs detached since the request ctx is cancelled.
func (h *Handler) releaseSlot(ctx context.Context) {
	actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), accountingTimeout)
	defer cancel()
	if err := h.limiter.Release(actx); err != nil {
		h.logger.Warn("release inflight slot", "err", err.Error())
	}
}

// persistConversation stores the conversation mapping on a detached context. It
// is used on the stop path (the request ctx is cancelled) so a conversation Dify
// created before the stop is not orphaned and the next message can continue it.
// Note: this shares the inherent reset/chat race — a concurrent ResetRequest can
// be undone by a slightly-later persist, same as the normal success path.
func (h *Handler) persistConversation(ctx context.Context, playerID, npcID, convID string) {
	if convID == "" {
		return
	}
	actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), accountingTimeout)
	defer cancel()
	if err := h.store.SetConversation(actx, playerID, npcID, convID, h.conversationTTL); err != nil {
		h.logger.Warn("persist conversation after stop", "err", err.Error())
	}
}

// stopUpstream best-effort aborts the Dify generation for taskID. The request
// context is already cancelled here, so it runs on a fresh bounded context.
// A stop that arrives before Dify's first message event has no taskID yet
// (PDR M4-T2 only requires stop to work after the first delta); such an early
// stop aborts the local stream but cannot call the upstream stop API.
func (h *Handler) stopUpstream(taskID, user string) {
	if taskID == "" {
		return // stream never produced a message event; nothing to stop upstream
	}
	ctx, cancel := context.WithTimeout(context.Background(), accountingTimeout)
	defer cancel()
	if err := h.dify.Stop(ctx, taskID, user); err != nil {
		h.logger.Warn("stop upstream generation", "task_id", taskID, "err", err.Error())
	}
}

// HandleReset clears the gateway-managed conversation mapping for (player, npc)
// so the next chat starts a fresh Dify conversation (PDR §M4-T2 reset). It acks
// with a ChatDone (conversation_id empty, total_tokens 0) on success.
func (h *Handler) HandleReset(ctx context.Context, sess *session.Session, req *gatewaypb.ResetRequest, send func(*gatewaypb.ServerEnvelope) error) {
	reqID := req.GetRequestId()
	npcID := strings.TrimSpace(req.GetNpcId())
	if npcID == "" {
		h.send(send, errorMsg(reqID, "BAD_REQUEST", "npc_id is required"))
		telemetry.RequestsTotal.WithLabelValues("bad_request").Inc()
		return
	}
	if h.store == nil {
		h.send(send, errorMsg(reqID, "INTERNAL", "gateway is not ready"))
		telemetry.RequestsTotal.WithLabelValues("error").Inc()
		return
	}
	if err := h.store.DeleteConversation(ctx, sess.PlayerID(), npcID); err != nil {
		h.logger.Warn("reset conversation", "err", err.Error())
		h.send(send, errorMsg(reqID, "INTERNAL", "conversation reset failed"))
		telemetry.RequestsTotal.WithLabelValues("error").Inc()
		return
	}
	h.send(send, done(reqID, "", 0))
	telemetry.RequestsTotal.WithLabelValues("ok").Inc()
}

func (h *Handler) send(send func(*gatewaypb.ServerEnvelope) error, env *gatewaypb.ServerEnvelope) {
	if send == nil {
		return
	}
	if err := send(env); err != nil {
		h.logger.Warn("send envelope failed", "err", err.Error())
	}
}

func clientErrorCode(err error) string {
	switch {
	case errors.Is(err, limiter.ErrRateLimited),
		errors.Is(err, limiter.ErrBudgetExceeded),
		errors.Is(err, limiter.ErrInflightFull),
		errors.Is(err, limiter.ErrCircuitOpen):
		return "RATE_LIMITED"
	case errors.Is(err, context.DeadlineExceeded):
		return "UPSTREAM_TIMEOUT"
	}

	var upstreamErr *dify.UpstreamError
	if errors.As(err, &upstreamErr) {
		if upstreamErr.StatusCode == http.StatusTooManyRequests {
			return "RATE_LIMITED"
		}
		if upstreamErr.StatusCode == http.StatusGatewayTimeout || upstreamErr.StatusCode == http.StatusRequestTimeout {
			return "UPSTREAM_TIMEOUT"
		}
		return "UPSTREAM_ERROR"
	}
	var streamErr *dify.StreamError
	if errors.As(err, &streamErr) && strings.EqualFold(streamErr.Code, "rate_limited") {
		return "RATE_LIMITED"
	}
	return "UPSTREAM_ERROR"
}

func clientErrorMessage(err error) string {
	switch clientErrorCode(err) {
	case "RATE_LIMITED":
		return "rate limited"
	case "UPSTREAM_TIMEOUT":
		return "upstream timeout"
	case "BAD_REQUEST":
		return "bad request"
	default:
		return "upstream error"
	}
}

func messageReplace(err error) (string, bool) {
	var replaceErr *dify.MessageReplaceError
	if errors.As(err, &replaceErr) {
		return replaceErr.Fallback, true
	}
	return "", false
}

func userID(playerID, npcID string) string {
	return playerID + ":" + npcID
}

func chunk(requestID, delta string) *gatewaypb.ServerEnvelope {
	return &gatewaypb.ServerEnvelope{Body: &gatewaypb.ServerEnvelope_Chunk{
		Chunk: &gatewaypb.ChatChunk{RequestId: requestID, Delta: delta},
	}}
}

func done(requestID, conversationID string, totalTokens int) *gatewaypb.ServerEnvelope {
	if totalTokens < 0 {
		totalTokens = 0
	}
	return &gatewaypb.ServerEnvelope{Body: &gatewaypb.ServerEnvelope_Done{
		Done: &gatewaypb.ChatDone{RequestId: requestID, ConversationId: conversationID, TotalTokens: uint32(totalTokens)},
	}}
}

func errorMsg(requestID, code, message string) *gatewaypb.ServerEnvelope {
	return &gatewaypb.ServerEnvelope{Body: &gatewaypb.ServerEnvelope_Error{
		Error: &gatewaypb.ErrorMsg{RequestId: requestID, Code: code, Message: message},
	}}
}

func blocked(requestID, fallback string) *gatewaypb.ServerEnvelope {
	return blockedEnvelope(requestID, fallback)
}

func blockedEnvelope(requestID, fallback string) *gatewaypb.ServerEnvelope {
	return &gatewaypb.ServerEnvelope{Body: &gatewaypb.ServerEnvelope_Blocked{
		Blocked: &gatewaypb.ChatBlocked{RequestId: requestID, Fallback: fallback},
	}}
}
