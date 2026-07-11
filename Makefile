PLUGIN := kubectl-shrink_pvc

.PHONY: build test vet fmt lint snapshot clean

build:
	go build -o $(PLUGIN) ./cmd/kubectl-shrink_pvc

test:
	go test ./...

vet:
	go vet ./...

fmt:
	gofmt -w cmd internal

lint:
	golangci-lint run

snapshot:
	goreleaser release --snapshot --clean

clean:
	rm -rf dist $(PLUGIN)
