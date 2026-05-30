package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"dify_gateway/internal/config"
	"dify_gateway/internal/gateway"
	"dify_gateway/internal/telemetry"
)

func main() {
	logger := telemetry.NewJSONLogger(os.Stdout)
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := gateway.Run(ctx, cfg, logger); err != nil {
		log.Fatalf("run gateway: %v", err)
	}
}
