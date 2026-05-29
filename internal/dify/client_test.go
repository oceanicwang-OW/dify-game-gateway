package dify

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// TestMain shrinks the retry backoff so retry-path tests don't sleep for the
// production default (500ms+ per attempt).
func TestMain(m *testing.M) {
	upstreamRetryBaseBackoff = time.Millisecond
	upstreamRetryMaxBackoff = 5 * time.Millisecond
	os.Exit(m.Run())
}

func TestClientChatSendsBlockingRequestAndParsesResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/chat-messages" {
			t.Fatalf("path = %s, want /chat-messages", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer app-test-key" {
			t.Fatalf("Authorization = %q", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Fatalf("Content-Type = %q", got)
		}

		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request body: %v", err)
		}

		if body["query"] != "What gear do you sell?" {
			t.Fatalf("query = %#v", body["query"])
		}
		if body["user"] != "player-10086:npc-blacksmith" {
			t.Fatalf("user = %#v", body["user"])
		}
		if body["response_mode"] != "blocking" {
			t.Fatalf("response_mode = %#v", body["response_mode"])
		}
		if body["conversation_id"] != "conv-existing" {
			t.Fatalf("conversation_id = %#v", body["conversation_id"])
		}
		inputs, ok := body["inputs"].(map[string]any)
		if !ok {
			t.Fatalf("inputs = %#v, want object", body["inputs"])
		}
		if inputs["player_level"] != "12" || inputs["affinity"] != "friendly" {
			t.Fatalf("inputs = %#v", inputs)
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"event":"message",
			"message_id":"msg-1",
			"conversation_id":"conv-2",
			"answer":"Here is a steel sword.",
			"metadata":{"usage":{"total_tokens":37}}
		}`))
	}))
	defer server.Close()

	client := NewClient(server.URL, "app-test-key", server.Client())
	result, err := client.Chat(context.Background(), ChatReq{
		Query:          "What gear do you sell?",
		Inputs:         map[string]string{"player_level": "12", "affinity": "friendly"},
		User:           "player-10086:npc-blacksmith",
		ConversationID: "conv-existing",
	})
	if err != nil {
		t.Fatalf("Chat() error = %v", err)
	}

	if result.Answer != "Here is a steel sword." {
		t.Fatalf("Answer = %q", result.Answer)
	}
	if result.ConversationID != "conv-2" {
		t.Fatalf("ConversationID = %q", result.ConversationID)
	}
	if result.MessageID != "msg-1" {
		t.Fatalf("MessageID = %q", result.MessageID)
	}
	if result.TotalTokens != 37 {
		t.Fatalf("TotalTokens = %d", result.TotalTokens)
	}
}

func TestClientChatRejectsMissingRequiredFields(t *testing.T) {
	client := NewClient("http://example.invalid", "app-test-key", nil)

	_, err := client.Chat(context.Background(), ChatReq{Query: "hello", User: "player-1"})
	if err == nil {
		t.Fatal("Chat() error = nil, want missing inputs error")
	}
}

func TestClientChatRetriesRateLimitAndServerErrors(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			http.Error(w, "temporary", http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"message_id":"msg-1",
			"conversation_id":"conv-1",
			"answer":"ok",
			"metadata":{"usage":{"total_tokens":3}}
		}`))
	}))
	defer server.Close()

	client := NewClient(server.URL, "app-test-key", server.Client())
	result, err := client.Chat(context.Background(), ChatReq{Query: "hello", Inputs: map[string]string{}, User: "player-1"})
	if err != nil {
		t.Fatalf("Chat() error = %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
	if result.Answer != "ok" {
		t.Fatalf("Answer = %q", result.Answer)
	}
}

func TestClientChatDoesNotRetryClientErrors(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer server.Close()

	client := NewClient(server.URL, "app-test-key", server.Client())
	_, err := client.Chat(context.Background(), ChatReq{Query: "hello", Inputs: map[string]string{}, User: "player-1"})
	if err == nil {
		t.Fatal("Chat() error = nil, want upstream error")
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
	var upstreamErr *UpstreamError
	if !errors.As(err, &upstreamErr) {
		t.Fatalf("error type = %T, want *UpstreamError", err)
	}
	if upstreamErr.StatusCode != http.StatusBadRequest || !strings.Contains(upstreamErr.Body, "bad request") {
		t.Fatalf("upstreamErr = %#v", upstreamErr)
	}
}
