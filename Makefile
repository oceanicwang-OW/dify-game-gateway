.PHONY: build test lint proto

build:
	go build ./...

test:
	go test ./...

lint:
	go fmt ./...
	go vet ./...

proto:
	@echo "proto generation is defined in M2-T1; install protoc and protoc-gen-go before enabling this target"
	@exit 1
