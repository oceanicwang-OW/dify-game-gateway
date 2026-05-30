package config

import (
	"strings"
	"testing"
)

func TestLoadReportsAllMissingRequiredVariables(t *testing.T) {
	t.Setenv("GATEWAY_ADDR", "")
	t.Setenv("DIFY_BASE_URL", "")
	t.Setenv("DIFY_APP_KEYS", "")
	t.Setenv("REDIS_ADDR", "")
	t.Setenv("AUTH_JWT_PUBKEY", "")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want missing required variables")
	}

	message := err.Error()
	for _, name := range []string{"DIFY_BASE_URL", "DIFY_APP_KEYS", "REDIS_ADDR", "AUTH_JWT_PUBKEY"} {
		if !strings.Contains(message, name) {
			t.Fatalf("Load() error %q does not mention missing %s", message, name)
		}
	}
}

func TestLoadAppliesDefaultsAndParsesValues(t *testing.T) {
	t.Setenv("DIFY_BASE_URL", "http://dify-api/v1")
	t.Setenv("DIFY_APP_KEYS", "default=app-default;npc-blacksmith=app-smith")
	t.Setenv("REDIS_ADDR", "redis:6379")
	t.Setenv("AUTH_JWT_PUBKEY", "pubkey")
	t.Setenv("GATEWAY_ADDR", "")
	t.Setenv("GATEWAY_ADMIN_ADDR", "")
	t.Setenv("UPSTREAM_TIMEOUT_SEC", "")
	t.Setenv("RATE_PER_PLAYER", "")
	t.Setenv("TOKEN_BUDGET_DAILY", "")
	t.Setenv("MAX_INFLIGHT_UPSTREAM", "")
	t.Setenv("MODERATION_ENABLED", "")
	t.Setenv("GATEWAY_SERVICE_NAME", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v, want nil", err)
	}

	if cfg.GatewayAddr != ":9000" {
		t.Fatalf("GatewayAddr = %q, want :9000", cfg.GatewayAddr)
	}
	if cfg.GatewayAdminAddr != ":9001" {
		t.Fatalf("GatewayAdminAddr = %q, want :9001", cfg.GatewayAdminAddr)
	}
	if cfg.DifyBaseURL != "http://dify-api/v1" {
		t.Fatalf("DifyBaseURL = %q", cfg.DifyBaseURL)
	}
	if cfg.DifyAppKeys["default"] != "app-default" || cfg.DifyAppKeys["npc-blacksmith"] != "app-smith" {
		t.Fatalf("DifyAppKeys = %#v", cfg.DifyAppKeys)
	}
	if cfg.RedisAddr != "redis:6379" {
		t.Fatalf("RedisAddr = %q", cfg.RedisAddr)
	}
	if cfg.AuthJWTPubKey != "pubkey" {
		t.Fatalf("AuthJWTPubKey = %q", cfg.AuthJWTPubKey)
	}
	if cfg.UpstreamTimeoutSec != 60 {
		t.Fatalf("UpstreamTimeoutSec = %d, want 60", cfg.UpstreamTimeoutSec)
	}
	if cfg.RatePerPlayer != "1r/2s" {
		t.Fatalf("RatePerPlayer = %q", cfg.RatePerPlayer)
	}
	if cfg.TokenBudgetDaily != 100000 {
		t.Fatalf("TokenBudgetDaily = %d, want 100000", cfg.TokenBudgetDaily)
	}
	if cfg.MaxInflightUpstream != 200 {
		t.Fatalf("MaxInflightUpstream = %d, want 200", cfg.MaxInflightUpstream)
	}
	if !cfg.ModerationEnabled {
		t.Fatal("ModerationEnabled = false, want true")
	}
	if cfg.GatewayServiceName != "game-ai-gateway" {
		t.Fatalf("GatewayServiceName = %q, want game-ai-gateway", cfg.GatewayServiceName)
	}
	if cfg.OTELExporterOTLPEndpoint != "" {
		t.Fatalf("OTELExporterOTLPEndpoint = %q, want empty", cfg.OTELExporterOTLPEndpoint)
	}
}

func TestLoadNormalizesEscapedPEMNewlines(t *testing.T) {
	t.Setenv("DIFY_BASE_URL", "http://dify-api/v1")
	t.Setenv("DIFY_APP_KEYS", "default=app-default")
	t.Setenv("REDIS_ADDR", "redis:6379")
	t.Setenv("AUTH_JWT_PUBKEY", "-----BEGIN PUBLIC KEY-----\\nabc\\n-----END PUBLIC KEY-----")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v, want nil", err)
	}
	if !strings.Contains(cfg.AuthJWTPubKey, "\nabc\n") {
		t.Fatalf("AuthJWTPubKey was not newline-normalized: %q", cfg.AuthJWTPubKey)
	}
}

func TestLoadRejectsInvalidNumbersAndBooleans(t *testing.T) {
	t.Setenv("DIFY_BASE_URL", "http://dify-api/v1")
	t.Setenv("DIFY_APP_KEYS", "default=app-default")
	t.Setenv("REDIS_ADDR", "redis:6379")
	t.Setenv("AUTH_JWT_PUBKEY", "pubkey")
	t.Setenv("UPSTREAM_TIMEOUT_SEC", "not-a-number")
	t.Setenv("TOKEN_BUDGET_DAILY", "-1")
	t.Setenv("MAX_INFLIGHT_UPSTREAM", "0")
	t.Setenv("MODERATION_ENABLED", "maybe")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want parse validation errors")
	}

	message := err.Error()
	for _, name := range []string{"UPSTREAM_TIMEOUT_SEC", "TOKEN_BUDGET_DAILY", "MAX_INFLIGHT_UPSTREAM", "MODERATION_ENABLED"} {
		if !strings.Contains(message, name) {
			t.Fatalf("Load() error %q does not mention invalid %s", message, name)
		}
	}
}

func TestLoadRejectsInvalidDifyAppKeyMapping(t *testing.T) {
	t.Setenv("DIFY_BASE_URL", "http://dify-api/v1")
	t.Setenv("DIFY_APP_KEYS", "default=app-default;broken")
	t.Setenv("REDIS_ADDR", "redis:6379")
	t.Setenv("AUTH_JWT_PUBKEY", "pubkey")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want invalid DIFY_APP_KEYS error")
	}
	if !strings.Contains(err.Error(), "DIFY_APP_KEYS") {
		t.Fatalf("Load() error %q does not mention DIFY_APP_KEYS", err.Error())
	}
}

func TestLoadDoesNotLeakDifyAppKeyInParseErrors(t *testing.T) {
	t.Setenv("DIFY_BASE_URL", "http://dify-api/v1")
	t.Setenv("DIFY_APP_KEYS", "default=app-default;=app-secret-value")
	t.Setenv("REDIS_ADDR", "redis:6379")
	t.Setenv("AUTH_JWT_PUBKEY", "pubkey")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want invalid DIFY_APP_KEYS error")
	}
	if strings.Contains(err.Error(), "app-secret-value") {
		t.Fatalf("Load() error leaked app key: %q", err.Error())
	}
	if !strings.Contains(err.Error(), "DIFY_APP_KEYS") {
		t.Fatalf("Load() error %q does not mention DIFY_APP_KEYS", err.Error())
	}
}
