# Deployment Guide

This directory contains the M5-T4 deployment workspace for the Dify Game AI Gateway.

## Build

```bash
docker build -t dify-game-ai-gateway:local .
```

The container exposes:

- `9000`: TCP game-client gateway.
- `9001`: admin HTTP server with `/healthz`, `/readyz`, and `/metrics`.

## Required Secrets

Inject these through Kubernetes Secret, a secret manager, or equivalent platform primitives:

- `DIFY_APP_KEYS`: semicolon-separated mapping, for example `default=app-xxx;npc-blacksmith=app-yyy`.
- `AUTH_JWT_PUBKEY`: login-server JWT public key PEM. Escaped `\n` and multiline PEM are both supported.

Do not put real app keys or private keys in Git.

## Kubernetes

Edit `deploy/k8s/gateway.yaml` before applying:

- Replace `image: ghcr.io/your-org/dify-game-ai-gateway:latest`.
- Replace `DIFY_BASE_URL`, `REDIS_ADDR`, `DIFY_APP_KEYS`, and `AUTH_JWT_PUBKEY`.
- Set replicas and resource limits for the target environment.

Apply:

```bash
kubectl apply -f deploy/k8s/gateway.yaml
```

Check rollout:

```bash
kubectl rollout status deploy/dify-game-ai-gateway
kubectl get pods -l app.kubernetes.io/name=dify-game-ai-gateway
```

## Probes And Metrics

- Liveness: `GET /healthz` returns `200 ok` if the process is alive.
- Readiness: `GET /readyz` returns `200 ok` only when Redis responds to `PING`.
- Metrics: `GET /metrics` exposes Prometheus metrics.

Prometheus should scrape the admin port (`9001`). Keep the admin port internal to the cluster unless the deployment platform has separate authentication and network controls.

## Post-Deploy Validation

After M5-T4 deployment, run an external validation against the running service:

1. Authenticate with a valid game session token.
2. Send a streaming chat request and verify chunks plus final `ChatDone`.
3. Send `StopRequest` after the first chunk and verify no further chunks arrive.
4. Trigger rate limiting and verify `RATE_LIMITED`.
5. Check `/metrics` for request, token, inflight, and circuit metrics.
6. Run an external load test against real TCP, Redis, and Dify to replace the in-process M5-T2 baseline with production-environment numbers.
