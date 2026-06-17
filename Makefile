.PHONY: build test fmt

build:
	go build -o kubectl-shrink_pvc ./cmd/kubectl-shrink_pvc

test:
	go test ./...

fmt:
	gofmt -w cmd internal
