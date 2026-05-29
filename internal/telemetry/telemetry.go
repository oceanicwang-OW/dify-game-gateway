package telemetry

import (
	"io"
	"log/slog"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	RequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_requests_total",
			Help: "Total gateway requests by status.",
		},
		[]string{"status"},
	)
	UpstreamLatencySeconds = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "gateway_upstream_latency_seconds",
			Help:    "Dify upstream request latency in seconds.",
			Buckets: prometheus.DefBuckets,
		},
	)
	FirstTokenLatencySeconds = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "gateway_first_token_latency_seconds",
			Help:    "Latency to first streamed token in seconds.",
			Buckets: prometheus.DefBuckets,
		},
	)
	TokensTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "gateway_tokens_total",
			Help: "Total upstream tokens consumed.",
		},
	)
	CostUSDTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "gateway_cost_usd_total",
			Help: "Total estimated upstream cost in USD.",
		},
	)
	ActiveConnections = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "gateway_active_connections",
			Help: "Currently active client connections.",
		},
	)
	InflightUpstream = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "gateway_inflight_upstream",
			Help: "Currently inflight upstream requests.",
		},
	)
	RateLimitedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "gateway_ratelimited_total",
			Help: "Total requests rejected by rate limiting.",
		},
	)
	CircuitState = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "gateway_circuit_state",
			Help: "Circuit breaker state: 0 closed, 1 half-open, 2 open.",
		},
	)
)

// Init registers telemetry endpoints on mux. Passing nil uses the default logger.
func Init(mux *http.ServeMux, logger *slog.Logger) error {
	if mux == nil {
		mux = http.DefaultServeMux
	}
	if logger == nil {
		logger = slog.Default()
	}

	registry := prometheus.NewRegistry()
	for _, collector := range []prometheus.Collector{
		RequestsTotal,
		UpstreamLatencySeconds,
		FirstTokenLatencySeconds,
		TokensTotal,
		CostUSDTotal,
		ActiveConnections,
		InflightUpstream,
		RateLimitedTotal,
		CircuitState,
	} {
		if err := registry.Register(collector); err != nil {
			return err
		}
	}

	RequestsTotal.WithLabelValues("init").Add(0)
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	logger.Info("telemetry initialized")
	return nil
}

func NewJSONLogger(w io.Writer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{}))
}

func Redact(value string) string {
	if value == "" {
		return ""
	}
	return "[REDACTED]"
}
