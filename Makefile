.PHONY: build test lint proto

build:
	go build ./...

test:
	go test ./...

lint:
	go fmt ./...
	go vet ./...

proto:
	@command -v protoc >/dev/null 2>&1 || { echo "protoc not found: install the Protocol Buffers compiler (e.g. 'choco install protoc')"; exit 1; }
	@command -v protoc-gen-go >/dev/null 2>&1 || { echo "protoc-gen-go not found: run 'go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.8' and ensure \$$(go env GOPATH)/bin is on PATH"; exit 1; }
	protoc --go_out=api/proto --go_opt=paths=source_relative -I api/proto api/proto/gateway.proto
