package telemetry

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRedactSecretHidesSensitiveValues(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{name: "api key", input: "app-1234567890abcdef"},
		{name: "player text", input: "player asked for a legendary sword"},
		{name: "jwt", input: "eyJhbGciOiJIUzI1NiJ9.payload.signature"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Redact(tc.input)
			if got == "" || got == tc.input {
				t.Fatalf("Redact(%q) = %q, want masked value", tc.input, got)
			}
			if strings.Contains(got, tc.input) {
				t.Fatalf("Redact(%q) = %q still contains original value", tc.input, got)
			}
		})
	}
}

func TestNewJSONLoggerWritesStructuredJSON(t *testing.T) {
	var buf bytes.Buffer
	logger := NewJSONLogger(&buf)

	logger.Info("request completed", "request_id", "req-1", "player_id", Redact("player-10086"))

	line := strings.TrimSpace(buf.String())
	if line == "" {
		t.Fatal("logger wrote no output")
	}

	var entry map[string]any
	if err := json.Unmarshal([]byte(line), &entry); err != nil {
		t.Fatalf("log line is not JSON: %v; line=%q", err, line)
	}
	if entry["msg"] != "request completed" {
		t.Fatalf("msg = %#v, want request completed", entry["msg"])
	}
	if entry["request_id"] != "req-1" {
		t.Fatalf("request_id = %#v, want req-1", entry["request_id"])
	}
	if entry["player_id"] == "player-10086" {
		t.Fatal("player_id was not redacted")
	}
}

func TestInitRegistersMetricsEndpoint(t *testing.T) {
	mux := http.NewServeMux()
	if err := Init(mux, NewJSONLogger(&bytes.Buffer{})); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	mux.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("/metrics status = %d, want 200", recorder.Code)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "gateway_requests_total") {
		t.Fatalf("/metrics body does not contain gateway_requests_total: %s", body)
	}
}
