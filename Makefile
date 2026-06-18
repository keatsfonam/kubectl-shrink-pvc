PLUGIN := kubectl-shrink_pvc
VERSION ?= $(shell git describe --tags --always --dirty)
PLATFORMS := linux/amd64 linux/arm64 darwin/amd64 darwin/arm64

.PHONY: build test vet fmt release-archives clean

build:
	go build -o $(PLUGIN) ./cmd/kubectl-shrink_pvc

test:
	go test ./...

vet:
	go vet ./...

fmt:
	gofmt -w cmd internal

release-archives:
	rm -rf dist
	mkdir -p dist
	for platform in $(PLATFORMS); do \
		os=$${platform%/*}; \
		arch=$${platform#*/}; \
		GOOS=$$os GOARCH=$$arch CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o dist/$(PLUGIN) ./cmd/kubectl-shrink_pvc; \
		cp README.md LICENSE dist/; \
		tar -C dist -czf dist/kubectl-shrink-pvc_$(VERSION)_$${os}_$${arch}.tar.gz $(PLUGIN) README.md LICENSE; \
		rm -f dist/$(PLUGIN) dist/README.md dist/LICENSE; \
	done
	cd dist && shasum -a 256 *.tar.gz > checksums.txt

clean:
	rm -rf dist $(PLUGIN)
