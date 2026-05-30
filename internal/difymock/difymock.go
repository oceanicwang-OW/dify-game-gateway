// Package difymock provides a scriptable, in-process mock of the Dify HTTP API
// for integration tests (PDR M5-T1). It serves POST /chat-messages with a
// programmable SSE event sequence, the POST /chat-messages/{task_id}/stop
// endpoint, and can inject HTTP error statuses (429/5xx), upstream stream
// events (error/message_replace), and timeouts (hang until the client cancels).
package difymock

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Event is one SSE event the mock streams for a chat request.
type Event struct {
	// Event is the Dify event name: "message", "message_end", "message_replace",
	// "error", "ping", etc.
	Event string
	// Delay, if set, is slept (respecting the request context) before this event
	// is written — used to interleave a client Stop/timeout mid-stream.
	Delay time.Duration

	Answer         string
	TaskID         string
	MessageID      string
	ConversationID string

	// Code/Message populate an "error" event.
	Code    string
	Message string

	// TotalTokens populates metadata.usage.total_tokens on a "message_end".
	TotalTokens int
}

// ChatBehavior scripts how the mock answers POST /chat-messages. Its zero value
// streams Events with no error injection, so error injection is always opt-in.
type ChatBehavior struct {
	// Status is the HTTP status (>= 400) to inject, with Body and optional
	// RetryAfter. It is applied per the FailTimes / FailForever knobs below; on
	// its own (both zero/false) no error is injected and Events are streamed.
	Status     int
	Body       string
	RetryAfter string

	// FailTimes injects Status on the first N attempts, then streams Events
	// (models retry-then-recover). FailForever injects Status on every attempt
	// (models retry exhaustion) and takes precedence over FailTimes.
	FailTimes   int
	FailForever bool

	// Hang blocks the handler until the request context is cancelled, simulating
	// an upstream that never responds (client-side timeout / Stop).
	Hang bool

	// Events is the SSE sequence streamed on a successful response.
	Events []Event
}

// ChatRequest is a captured POST /chat-messages call.
type ChatRequest struct {
	Authorization  string
	Accept         string
	Query          string            `json:"query"`
	User           string            `json:"user"`
	ConversationID string            `json:"conversation_id"`
	ResponseMode   string            `json:"response_mode"`
	Inputs         map[string]string `json:"inputs"`
}

// StopCall is a captured POST /chat-messages/{task_id}/stop call.
type StopCall struct {
	TaskID string
	User   string `json:"user"`
}

// Server is a running mock Dify. Close it when the test finishes.
type Server struct {
	*httptest.Server

	apiKey    string
	mu        sync.Mutex
	chat      ChatBehavior
	chatN     int
	chatReqs  []ChatRequest
	stopCalls []StopCall
}

// New starts a mock Dify server. If apiKey is non-empty, requests must carry
// "Authorization: Bearer <apiKey>" or the mock responds 401 (like real Dify).
// The default chat behavior streams nothing and must be configured with SetChat.
func New(apiKey string) *Server {
	s := &Server{apiKey: apiKey}
	mux := http.NewServeMux()
	mux.HandleFunc("/chat-messages", s.handleChat)
	mux.HandleFunc("/chat-messages/", s.handleStop) // {task_id}/stop
	s.Server = httptest.NewServer(mux)
	return s
}

// authorized reports whether the request carries the expected bearer token.
// When the server was created without an API key, all requests are authorized.
func (s *Server) authorized(r *http.Request) bool {
	if s.apiKey == "" {
		return true
	}
	return r.Header.Get("Authorization") == "Bearer "+s.apiKey
}

// SetChat installs the behavior for subsequent chat requests.
func (s *Server) SetChat(b ChatBehavior) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.chat = b
	s.chatN = 0
}

// ChatRequests returns a copy of the captured chat requests.
func (s *Server) ChatRequests() []ChatRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ChatRequest(nil), s.chatReqs...)
}

// StopCalls returns a copy of the captured stop calls.
func (s *Server) StopCalls() []StopCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]StopCall(nil), s.stopCalls...)
}

func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var captured ChatRequest
	_ = json.NewDecoder(r.Body).Decode(&captured)
	captured.Authorization = r.Header.Get("Authorization")
	captured.Accept = r.Header.Get("Accept")

	s.mu.Lock()
	s.chatReqs = append(s.chatReqs, captured)
	s.chatN++
	attempt := s.chatN
	b := s.chat
	s.mu.Unlock()

	if !s.authorized(r) {
		http.Error(w, `{"code":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	if captured.Query == "" || captured.User == "" {
		http.Error(w, `{"code":"invalid_param"}`, http.StatusBadRequest)
		return
	}

	// Error injection: every attempt (FailForever) or the first FailTimes attempts.
	if b.Status >= 400 && (b.FailForever || attempt <= b.FailTimes) {
		if b.RetryAfter != "" {
			w.Header().Set("Retry-After", b.RetryAfter)
		}
		http.Error(w, b.Body, b.Status)
		return
	}

	if b.Hang {
		<-r.Context().Done() // unblocks when the client cancels/times out
		return
	}

	s.streamSSE(w, r, b.Events)
}

func (s *Server) streamSSE(w http.ResponseWriter, r *http.Request, events []Event) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	// Real Dify stamps task_id on every streaming event; propagate the first
	// task_id seen onto later events that don't set their own.
	streamTaskID := ""
	for _, ev := range events {
		if ev.TaskID != "" {
			streamTaskID = ev.TaskID
		} else {
			ev.TaskID = streamTaskID
		}
		if ev.Delay > 0 {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(ev.Delay):
			}
		}
		if _, err := fmt.Fprintf(w, "data: %s\n\n", encodeEvent(ev)); err != nil {
			return
		}
		flusher.Flush()
	}
}

// encodeEvent renders one event as the Dify SSE JSON payload.
func encodeEvent(ev Event) []byte {
	m := map[string]any{"event": ev.Event}
	if ev.TaskID != "" {
		m["task_id"] = ev.TaskID
	}
	if ev.MessageID != "" {
		m["message_id"] = ev.MessageID
	}
	if ev.ConversationID != "" {
		m["conversation_id"] = ev.ConversationID
	}
	if ev.Answer != "" {
		m["answer"] = ev.Answer
	}
	if ev.Code != "" {
		m["code"] = ev.Code
	}
	if ev.Message != "" {
		m["message"] = ev.Message
	}
	if ev.Event == "message_end" {
		m["metadata"] = map[string]any{
			"usage": map[string]any{"total_tokens": ev.TotalTokens},
		}
	}
	out, _ := json.Marshal(m)
	return out
}

func (s *Server) handleStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.authorized(r) {
		http.Error(w, `{"code":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	// Path is /chat-messages/{task_id}/stop. Parse the escaped path and unescape
	// the segment so task_ids containing '/' (percent-encoded by the client) are
	// recovered intact rather than splitting the route.
	rest := strings.TrimPrefix(r.URL.EscapedPath(), "/chat-messages/")
	enc := strings.TrimSuffix(rest, "/stop")
	if enc == rest || enc == "" {
		http.NotFound(w, r)
		return
	}
	taskID, err := url.PathUnescape(enc)
	if err != nil {
		http.Error(w, `{"code":"invalid_param"}`, http.StatusBadRequest)
		return
	}

	var call StopCall
	_ = json.NewDecoder(r.Body).Decode(&call)
	call.TaskID = taskID

	s.mu.Lock()
	s.stopCalls = append(s.stopCalls, call)
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"result":"success"}`))
}
