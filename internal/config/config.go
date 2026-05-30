package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	defaultGatewayAddr        = ":9000"
	defaultGatewayAdminAddr   = ":9001"
	defaultUpstreamTimeoutSec = 60
	defaultRatePerPlayer      = "1r/2s"
	defaultTokenBudgetDaily   = 100000
	defaultMaxInflight        = 200
	defaultModerationEnabled  = true
	defaultGatewayServiceName = "game-ai-gateway"
)

// Config contains startup configuration loaded from environment variables.
type Config struct {
	GatewayAddr              string
	GatewayAdminAddr         string
	DifyBaseURL              string
	DifyAppKeys              map[string]string
	RedisAddr                string
	AuthJWTPubKey            string
	UpstreamTimeoutSec       int
	RatePerPlayer            string
	TokenBudgetDaily         int
	MaxInflightUpstream      int
	ModerationEnabled        bool
	GatewayServiceName       string
	OTELExporterOTLPEndpoint string
}

// Load reads all gateway configuration from process environment variables.
func Load() (Config, error) {
	var errs []error

	cfg := Config{
		GatewayAddr:              envString("GATEWAY_ADDR", defaultGatewayAddr),
		GatewayAdminAddr:         envString("GATEWAY_ADMIN_ADDR", defaultGatewayAdminAddr),
		DifyBaseURL:              strings.TrimSpace(os.Getenv("DIFY_BASE_URL")),
		RedisAddr:                strings.TrimSpace(os.Getenv("REDIS_ADDR")),
		AuthJWTPubKey:            normalizePEMEnv(os.Getenv("AUTH_JWT_PUBKEY")),
		RatePerPlayer:            envString("RATE_PER_PLAYER", defaultRatePerPlayer),
		UpstreamTimeoutSec:       defaultUpstreamTimeoutSec,
		TokenBudgetDaily:         defaultTokenBudgetDaily,
		MaxInflightUpstream:      defaultMaxInflight,
		ModerationEnabled:        defaultModerationEnabled,
		GatewayServiceName:       envString("GATEWAY_SERVICE_NAME", defaultGatewayServiceName),
		OTELExporterOTLPEndpoint: strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")),
	}

	if cfg.DifyBaseURL == "" {
		errs = append(errs, missing("DIFY_BASE_URL"))
	}
	if cfg.RedisAddr == "" {
		errs = append(errs, missing("REDIS_ADDR"))
	}
	if cfg.AuthJWTPubKey == "" {
		errs = append(errs, missing("AUTH_JWT_PUBKEY"))
	}

	appKeys, err := parseAppKeys(os.Getenv("DIFY_APP_KEYS"))
	if err != nil {
		errs = append(errs, err)
	}
	cfg.DifyAppKeys = appKeys

	if value := strings.TrimSpace(os.Getenv("UPSTREAM_TIMEOUT_SEC")); value != "" {
		cfg.UpstreamTimeoutSec, err = positiveInt("UPSTREAM_TIMEOUT_SEC", value)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if value := strings.TrimSpace(os.Getenv("TOKEN_BUDGET_DAILY")); value != "" {
		cfg.TokenBudgetDaily, err = positiveInt("TOKEN_BUDGET_DAILY", value)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if value := strings.TrimSpace(os.Getenv("MAX_INFLIGHT_UPSTREAM")); value != "" {
		cfg.MaxInflightUpstream, err = positiveInt("MAX_INFLIGHT_UPSTREAM", value)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if value := strings.TrimSpace(os.Getenv("MODERATION_ENABLED")); value != "" {
		cfg.ModerationEnabled, err = strconv.ParseBool(value)
		if err != nil {
			errs = append(errs, fmt.Errorf("MODERATION_ENABLED must be a boolean: %w", err))
		}
	}

	if len(errs) > 0 {
		return Config{}, errors.Join(errs...)
	}
	return cfg, nil
}

func envString(name, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	return value
}

func normalizePEMEnv(raw string) string {
	return strings.ReplaceAll(strings.TrimSpace(raw), `\n`, "\n")
}

func parseAppKeys(raw string) (map[string]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, missing("DIFY_APP_KEYS")
	}

	keys := make(map[string]string)
	for i, pair := range strings.Split(raw, ";") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		name, key, ok := strings.Cut(pair, "=")
		name = strings.TrimSpace(name)
		key = strings.TrimSpace(key)
		if !ok || name == "" || key == "" {
			return nil, fmt.Errorf("DIFY_APP_KEYS contains invalid mapping at position %d", i+1)
		}
		keys[name] = key
	}
	if len(keys) == 0 {
		return nil, missing("DIFY_APP_KEYS")
	}
	return keys, nil
}

func positiveInt(name, raw string) (int, error) {
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		if err != nil {
			return 0, fmt.Errorf("%s must be a positive integer: %w", name, err)
		}
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return value, nil
}

func missing(name string) error {
	return fmt.Errorf("missing required environment variable %s", name)
}
