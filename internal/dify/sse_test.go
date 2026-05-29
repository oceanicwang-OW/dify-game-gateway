package dify

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestClientChatStreamDispatchesDeltasAndUsage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat-messages" {
			t.Fatalf("path = %s, want /chat-messages", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer app-test-key" {
			t.Fatalf("Authorization = %q", got)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"event\":\"message\",\"task_id\":\"task-1\",\"message_id\":\"msg-1\",\"conversation_id\":\"conv-1\",\"answer\":\"hello\"}\n\n"))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		_, _ = w.Write([]byte("data: {\"event\":\"ping\"}\n\n"))
		_, _ = w.Write([]byte("data: {\"event\":\"message\",\"task_id\":\"task-1\",\"conversation_id\":\"conv-1\",\"answer\":\" world\"}\n\n"))
		_, _ = w.Write([]byte("data: {\"event\":\"message_end\",\"message_id\":\"msg-1\",\"conversation_id\":\"conv-1\",\"metadata\":{\"usage\":{\"total_tokens\":9}}}\n\n"))
	}))
	defer server.Close()

	client := NewClient(server.URL, "app-test-key", server.Client())
	var events []string
	var deltas []string
	result, err := client.ChatStream(
		context.Background(),
		ChatReq{Query: "hello", Inputs: map[string]string{}, User: "player-1"},
		func(taskID, convID string) { events = append(events, taskID+":"+convID) },
		func(delta string) { deltas = append(deltas, delta) },
	)
	if err != nil {
		t.Fatalf("ChatStream() error = %v", err)
	}

	if !reflect.DeepEqual(events, []string{"task-1:conv-1"}) {
		t.Fatalf("events = %#v", events)
	}
	if !reflect.DeepEqual(deltas, []string{"hello", " world"}) {
		t.Fatalf("deltas = %#v", deltas)
	}
	if result.ConversationID != "conv-1" || result.MessageID != "msg-1" || result.TotalTokens != 9 {
		t.Fatalf("result = %#v", result)
	}
}

func TestClientChatStreamRetriesInitialServerErrors(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			http.Error(w, "temporary", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"event\":\"message\",\"task_id\":\"task-1\",\"message_id\":\"msg-1\",\"conversation_id\":\"conv-1\",\"answer\":\"hello\"}\n\n"))
		_, _ = w.Write([]byte("data: {\"event\":\"message_end\",\"message_id\":\"msg-1\",\"conversation_id\":\"conv-1\",\"metadata\":{\"usage\":{\"total_tokens\":4}}}\n\n"))
	}))
	defer server.Close()

	client := NewClient(server.URL, "app-test-key", server.Client())
	var deltas []string
	result, err := client.ChatStream(
		context.Background(),
		ChatReq{Query: "hello", Inputs: map[string]string{}, User: "player-1"},
		nil,
		func(delta string) { deltas = append(deltas, delta) },
	)
	if err != nil {
		t.Fatalf("ChatStream() error = %v", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
	if !reflect.DeepEqual(deltas, []string{"hello"}) {
		t.Fatalf("deltas = %#v", deltas)
	}
	if result.TotalTokens != 4 {
		t.Fatalf("TotalTokens = %d", result.TotalTokens)
	}
}

func TestClientChatStreamDoesNotRetryAfterDelta(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"event\":\"message\",\"task_id\":\"task-1\",\"message_id\":\"msg-1\",\"conversation_id\":\"conv-1\",\"answer\":\"partial\"}\n\n"))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}))
	defer server.Close()

	client := NewClient(server.URL, "app-test-key", server.Client())
	var deltas []string
	_, err := client.ChatStream(
		context.Background(),
		ChatReq{Query: "hello", Inputs: map[string]string{}, User: "player-1"},
		nil,
		func(delta string) { deltas = append(deltas, delta) },
	)
	if err == nil {
		t.Fatal("ChatStream() error = nil, want incomplete stream error")
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want no retry after delta", attempts)
	}
	if !reflect.DeepEqual(deltas, []string{"partial"}) {
		t.Fatalf("deltas = %#v", deltas)
	}
}

func TestClientChatStreamReturnsStreamErrorEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"event\":\"error\",\"code\":\"rate_limited\",\"message\":\"slow down\"}\n\n"))
	}))
	defer server.Close()

	client := NewClient(server.URL, "app-test-key", server.Client())
	_, err := client.ChatStream(context.Background(), ChatReq{Query: "hello", Inputs: map[string]string{}, User: "player-1"}, nil, nil)
	if err == nil {
		t.Fatal("ChatStream() error = nil, want stream error")
	}
	var streamErr *StreamError
	if !errors.As(err, &streamErr) {
		t.Fatalf("error type = %T, want *StreamError", err)
	}
	if streamErr.Code != "rate_limited" || streamErr.Message != "slow down" {
		t.Fatalf("streamErr = %#v", streamErr)
	}
}

func TestClientChatStreamReturnsErrorWhenStreamEndsBeforeTerminalEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"event\":\"message\",\"task_id\":\"task-1\",\"message_id\":\"msg-1\",\"conversation_id\":\"conv-1\",\"answer\":\"partial\"}\n\n"))
	}))
	defer server.Close()

	client := NewClient(server.URL, "app-test-key", server.Client())
	_, err := client.ChatStream(context.Background(), ChatReq{Query: "hello", Inputs: map[string]string{}, User: "player-1"}, nil, nil)
	if err == nil {
		t.Fatal("ChatStream() error = nil, want incomplete stream error")
	}
	if !strings.Contains(err.Error(), "ended before terminal event") {
		t.Fatalf("ChatStream() error = %v, want incomplete stream error", err)
	}
}

func TestClientChatStreamReturnsMessageReplaceFallback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"event\":\"message_replace\",\"answer\":\"fallback text\"}\n\n"))
	}))
	defer server.Close()

	client := NewClient(server.URL, "app-test-key", server.Client())
	_, err := client.ChatStream(context.Background(), ChatReq{Query: "hello", Inputs: map[string]string{}, User: "player-1"}, nil, nil)
	if err == nil {
		t.Fatal("ChatStream() error = nil, want message replace error")
	}
	var replaceErr *MessageReplaceError
	if !errors.As(err, &replaceErr) {
		t.Fatalf("error type = %T, want *MessageReplaceError", err)
	}
	if replaceErr.Fallback != "fallback text" {
		t.Fatalf("Fallback = %q", replaceErr.Fallback)
	}
}
