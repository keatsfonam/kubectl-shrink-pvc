PLUGIN := kubectl-shrink_pvc

.PHONY: build test vet fmt lint e2e snapshot clean

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

e2e:
	hack/e2e.sh

snapshot:
	goreleaser release --snapshot --clean

clean:
	rm -rf dist $(PLUGIN)
