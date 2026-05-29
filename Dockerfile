# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . ./
RUN go build -o /out/gateway ./cmd/gateway

FROM alpine:3.20
RUN adduser -D -H -u 10001 gateway
USER gateway
COPY --from=builder /out/gateway /gateway
EXPOSE 9000 9001
ENTRYPOINT ["/gateway"]
