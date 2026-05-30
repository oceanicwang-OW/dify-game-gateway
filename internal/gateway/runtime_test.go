package gateway

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"

	"dify_gateway/internal/config"
	"dify_gateway/internal/dify"
	"dify_gateway/internal/telemetry"
)

func testPublicKeyPEM(t *testing.T) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rsa key: %v", err)
	}
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatalf("marshal public key: %v", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
}

func TestBuildRuntimeWiresAdminHealth(t *testing.T) {
	redisSrv := miniredis.RunT(t)
	cfg := config.Config{
		GatewayAddr:         "127.0.0.1:0",
		GatewayAdminAddr:    "127.0.0.1:0",
		DifyBaseURL:         "http://dify.test/v1",
		DifyAppKeys:         map[string]string{"default": "app-default"},
		RedisAddr:           redisSrv.Addr(),
		AuthJWTPubKey:       testPublicKeyPEM(t),
		UpstreamTimeoutSec:  30,
		RatePerPlayer:       "10r/1s",
		TokenBudgetDaily:    1000,
		MaxInflightUpstream: 10,
		ModerationEnabled:   true,
	}

	rt, err := Build(cfg, telemetry.NewJSONLogger(&strings.Builder{}))
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	defer rt.Close()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rt.AdminMux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("/healthz status = %d, body=%q; want 200", rec.Code, rec.Body.String())
	}
	if strings.TrimSpace(rec.Body.String()) != "ok" {
		t.Fatalf("/healthz body = %q, want ok", rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rt.AdminMux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/readyz status = %d, body=%q; want 200", rec.Code, rec.Body.String())
	}
}

func TestDifyRouterSelectsNPCSpecificKeyAndDefaultFallback(t *testing.T) {
	var authHeaders []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeaders = append(authHeaders, r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"event\":\"message_end\",\"conversation_id\":\"conv\",\"metadata\":{\"usage\":{\"total_tokens\":1}}}\n\n"))
	}))
	defer upstream.Close()

	router, err := NewDifyRouter(upstream.URL, map[string]string{
		"default":        "app-default",
		"npc-blacksmith": "app-smith",
		"npc-apothecary": "app-apothecary",
	}, upstream.Client())
	if err != nil {
		t.Fatalf("NewDifyRouter() error = %v", err)
	}

	for _, user := range []string{"player-1:npc-blacksmith", "player-1:npc-unknown"} {
		_, err := router.ChatStream(context.Background(), dify.ChatReq{
			Query:  "hello",
			Inputs: map[string]string{},
			User:   user,
		}, nil, nil)
		if err != nil {
			t.Fatalf("ChatStream(%q) error = %v", user, err)
		}
	}

	want := []string{"Bearer app-smith", "Bearer app-default"}
	if len(authHeaders) != len(want) {
		t.Fatalf("auth headers = %#v, want %#v", authHeaders, want)
	}
	for i := range want {
		if authHeaders[i] != want[i] {
			t.Fatalf("authHeaders[%d] = %q, want %q", i, authHeaders[i], want[i])
		}
	}
}

func TestDifyRouterRequiresMatchingKeyWhenNoDefaultExists(t *testing.T) {
	router, err := NewDifyRouter("http://dify.test/v1", map[string]string{"npc-a": "app-a"}, nil)
	if err != nil {
		t.Fatalf("NewDifyRouter() error = %v", err)
	}

	_, err = router.ChatStream(context.Background(), dify.ChatReq{
		Query:  "hello",
		Inputs: map[string]string{},
		User:   "player-1:npc-missing",
	}, nil, nil)
	if err == nil {
		t.Fatal("ChatStream() error = nil, want missing app key error")
	}
	if strings.Contains(err.Error(), "app-a") {
		t.Fatalf("error leaked configured app key: %q", err.Error())
	}
}
