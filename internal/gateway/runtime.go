package gateway

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"

	"dify_gateway/internal/auth"
	"dify_gateway/internal/config"
	contextasm "dify_gateway/internal/context"
	"dify_gateway/internal/limiter"
	"dify_gateway/internal/listener"
	"dify_gateway/internal/moderation"
	"dify_gateway/internal/pipeline"
	"dify_gateway/internal/store"
	"dify_gateway/internal/telemetry"
)

type Runtime struct {
	Config      config.Config
	Logger      *slog.Logger
	Redis       *redis.Client
	AdminMux    *http.ServeMux
	Handler     *pipeline.Handler
	Listener    *listener.Server
	AdminServer *http.Server
}

func Build(cfg config.Config, logger *slog.Logger) (*Runtime, error) {
	if logger == nil {
		logger = slog.Default()
	}

	authn, err := auth.New(cfg.AuthJWTPubKey)
	if err != nil {
		return nil, err
	}

	redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	redisStore := store.New(redisClient)
	redisLimiter, err := limiter.New(redisClient, limiter.Config{
		RatePerPlayer:       cfg.RatePerPlayer,
		TokenBudgetDaily:    cfg.TokenBudgetDaily,
		MaxInflightUpstream: cfg.MaxInflightUpstream,
	})
	if err != nil {
		_ = redisClient.Close()
		return nil, err
	}

	httpClient := &http.Client{Timeout: time.Duration(cfg.UpstreamTimeoutSec) * time.Second}
	difyClient, err := NewDifyRouter(cfg.DifyBaseURL, cfg.DifyAppKeys, httpClient)
	if err != nil {
		_ = redisClient.Close()
		return nil, err
	}

	mod := moderation.Moderator(moderation.NewPolicyModerator(moderation.NewKeywordPolicy(nil)))
	if !cfg.ModerationEnabled {
		mod = moderation.AllowAll{}
	}

	adminMux := http.NewServeMux()
	if err := telemetry.Init(adminMux, logger); err != nil {
		_ = redisClient.Close()
		return nil, err
	}
	addHealthHandlers(adminMux, redisClient)

	h := pipeline.New(pipeline.Config{
		Authenticator:    authn,
		Limiter:          redisLimiter,
		Store:            redisStore,
		ContextAssembler: contextasm.New(contextasm.DefaultContract(), logger),
		Moderator:        mod,
		DifyClient:       difyClient,
		Logger:           logger,
	})

	return &Runtime{
		Config:      cfg,
		Logger:      logger,
		Redis:       redisClient,
		AdminMux:    adminMux,
		Handler:     h,
		Listener:    listener.New(h, logger),
		AdminServer: &http.Server{Addr: cfg.GatewayAdminAddr, Handler: adminMux},
	}, nil
}

func (r *Runtime) Run(ctx context.Context) error {
	if r == nil {
		return fmt.Errorf("gateway: runtime is nil")
	}
	errCh := make(chan error, 2)

	go func() {
		r.Logger.Info("admin server starting", "addr", r.Config.GatewayAdminAddr)
		if err := r.AdminServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("admin server: %w", err)
			return
		}
		errCh <- nil
	}()

	go func() {
		r.Logger.Info("gateway listener starting", "addr", r.Config.GatewayAddr)
		if err := r.Listener.ListenAndServe(ctx, r.Config.GatewayAddr); err != nil {
			errCh <- fmt.Errorf("gateway listener: %w", err)
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.AdminServer.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		if err != nil {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = r.AdminServer.Shutdown(shutdownCtx)
			return err
		}
		return nil
	}
}

func (r *Runtime) Close() error {
	if r == nil || r.Redis == nil {
		return nil
	}
	return r.Redis.Close()
}

func addHealthHandlers(mux *http.ServeMux, redisClient *redis.Client) {
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
		defer cancel()
		if err := redisClient.Ping(ctx).Err(); err != nil {
			http.Error(w, "redis unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
}

func Run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	rt, err := Build(cfg, logger)
	if err != nil {
		return err
	}
	defer rt.Close()
	return rt.Run(ctx)
}
