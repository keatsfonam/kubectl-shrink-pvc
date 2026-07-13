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
	shellcheck hack/*.sh

e2e:
	hack/e2e.sh

snapshot:
	goreleaser check
	goreleaser release --snapshot --clean --skip=sign

clean:
	rm -rf dist $(PLUGIN)
