# Makefile for OutpostNet v2

# github.com/outpostos/OutpostRange (observation contract, used by
# internal/observation) is a private module: fetch it directly over SSH
# (git config url.git@github.com:.insteadOf https://github.com/), without the
# public proxy or checksum database.
GOPRIVATE ?= github.com/outpostos
GONOSUMDB ?= github.com/outpostos
export GOPRIVATE GONOSUMDB

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build -buildvcs=false
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod

# Binary names
SERVER_BINARY=outpostnet-server
SIDECAR_BINARY=outpostnet-sidecar
CONTROLLER_BINARY=outpostnet-controller
AGENT_BINARY=outpostnet-agent
SERVICE_BINARY=outpostnet-service
STATE_RESET_BINARY=outpostnet-state-reset

# Build directory
BUILD_DIR=build

# Version information
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT = $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME = $(shell date +%Y-%m-%dT%H:%M:%S%z)

# Version package for ldflags injection (K8s style)
VERSION_PKG = github.com/outpostos/OutpostNet/pkg/version

# Go build flags
LDFLAGS = -ldflags "-X $(VERSION_PKG).gitVersion=$(VERSION) -X $(VERSION_PKG).gitCommit=$(COMMIT) -X $(VERSION_PKG).buildDate=$(BUILD_TIME)"

# Release platforms
RELEASE_PLATFORMS = linux/amd64 linux/arm64 darwin/amd64 darwin/arm64
RELEASE_COMPONENTS = outpostnet-server outpostnet-sidecar outpostnet-agent outpostnet-controller outpostnet-service outpostnet-state-reset
RELEASE_LDFLAGS = -ldflags "-s -w -X $(VERSION_PKG).gitVersion=$(VERSION) -X $(VERSION_PKG).gitCommit=$(COMMIT) -X $(VERSION_PKG).buildDate=$(BUILD_TIME)"

# Release artifacts
RELEASE_DIR = $(BUILD_DIR)/release
K8S_MANIFEST = outpostnet-k8s.yaml
# Images rewritten by kustomize's `images:` transformer. The sidecar is not
# listed here: it is never referenced via an `image:` field, only as the
# controller's --sidecar-image argument, which the transformer cannot rewrite.
# release-manifest pins that argument separately (see SIDECAR_IMAGE_ARG).
K8S_IMAGES = outpostnet-controller outpostnet-server
SIDECAR_IMAGE_ARG = --sidecar-image=outpostnet-sidecar
KUSTOMIZE ?= kubectl kustomize

.PHONY: all build clean test test-verbose test-cover test-timesync-integration lint fmt vet tidy help release
.PHONY: release-archives release-manifest release-checksums release-verify

# `release` runs its prerequisites in order: fmt, vet, archives, manifest, checksums.
.NOTPARALLEL: release

# Default target
all: clean fmt vet test build

# Build targets
build: build-server build-sidecar build-controller build-agent build-service build-state-reset

build-server:
	@echo "Building $(SERVER_BINARY)..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(SERVER_BINARY) ./cmd/outpostnet-server

build-sidecar:
	@echo "Building $(SIDECAR_BINARY)..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(SIDECAR_BINARY) ./cmd/outpostnet-sidecar

build-controller:
	@echo "Building $(CONTROLLER_BINARY)..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(CONTROLLER_BINARY) ./cmd/outpostnet-controller

build-agent:
	@echo "Building $(AGENT_BINARY)..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(AGENT_BINARY) ./cmd/outpostnet-agent

build-service:
	@echo "Building $(SERVICE_BINARY)..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(SERVICE_BINARY) ./cmd/outpostnet-service

build-state-reset:
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(STATE_RESET_BINARY) ./cmd/outpostnet-state-reset

# Install builds binaries to project root for easy testing
install: build
	@echo "Installing binaries to project root..."
	@cp $(BUILD_DIR)/$(SERVER_BINARY) ./
	@cp $(BUILD_DIR)/$(SIDECAR_BINARY) ./

# Test targets
test:
	@echo "Running tests..."
	$(GOTEST) -race -count=1 ./...

test-verbose:
	@echo "Running tests with verbose output..."
	$(GOTEST) -v -race ./...

test-cover:
	@echo "Running tests with coverage..."
	$(GOTEST) -v -race -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# NTS time-sync end-to-end test: two chronyd processes in network namespaces.
# Needs Linux, root, chrony >= 4.3 and iproute2. Skips cleanly when
# prerequisites are absent; set OUTPOSTNET_NTS_REQUIRE=1 to fail on skip (CI).
test-timesync-integration:
	@echo "Running NTS time-sync integration test..."
	$(GOTEST) -tags integration -count=1 -timeout 5m -run '^TestNTSSyncIntegration$$' -v ./pkg/chronyconf/

# Code quality targets
fmt:
	@echo "Formatting code..."
	$(GOCMD) fmt ./...

vet:
	@echo "Running go vet..."
	$(GOCMD) vet ./...

lint:
	@echo "Running golint..."
	@if command -v golint >/dev/null 2>&1; then \
		golint ./...; \
	else \
		echo "golint not installed, skipping..."; \
	fi

# Dependency management
tidy:
	@echo "Tidying modules..."
	$(GOMOD) tidy

deps:
	@echo "Downloading dependencies..."
	$(GOMOD) download

# Clean targets
clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	@rm -f $(SERVER_BINARY) $(SIDECAR_BINARY) $(CONTROLLER_BINARY) $(AGENT_BINARY) $(SERVICE_BINARY) $(STATE_RESET_BINARY)
	@rm -rf $(BUILD_DIR)
	@rm -f coverage.out coverage.html

clean-all: clean
	@echo "Cleaning all generated files..."
	@rm -f /tmp/outpostnet-*.pid /tmp/outpostnet-*.log

# K8s E2E test
e2e-k8s:
	@echo "Running K8s E2E test..."
	./hack/e2e-k8s.sh

e2e-k8s-cleanup:
	./hack/e2e-k8s.sh cleanup

e2e-controller:
	@echo "Running Controller E2E test..."
	./hack/e2e-controller.sh

e2e-controller-cleanup:
	./hack/e2e-controller.sh cleanup

e2e-cross-region:
	@echo "Running Cross-Region E2E test..."
	./hack/e2e-cross-region.sh

e2e-cross-region-cleanup:
	./hack/e2e-cross-region.sh cleanup

e2e-relay:
	@echo "Running Relay E2E test..."
	./hack/e2e-relay.sh

e2e-relay-cleanup:
	./hack/e2e-relay.sh cleanup

# Tunnel benchmarks
bench-tunnel:
	@echo "Running QUIC tunnel benchmarks..."
	$(GOTEST) -bench=. -benchtime=3s -run=^$$ ./pkg/tunnel/

# Release targets
release: fmt vet release-archives release-manifest release-checksums release-verify
	@echo "Release artifacts ready in $(RELEASE_DIR)/"
	@ls -lh $(RELEASE_DIR)/*.tar.gz $(RELEASE_DIR)/$(K8S_MANIFEST) $(RELEASE_DIR)/SHA256SUMS

release-archives:
	@echo "Building release binaries for all platforms..."
	@mkdir -p $(RELEASE_DIR)
	@rm -f $(RELEASE_DIR)/*.tar.gz
	@for component in $(RELEASE_COMPONENTS); do \
		for platform in $(RELEASE_PLATFORMS); do \
			os=$$(echo $$platform | cut -d/ -f1); \
			arch=$$(echo $$platform | cut -d/ -f2); \
			binary=$$component-$(VERSION)-$$os-$$arch; \
			echo "  $$os/$$arch $$component"; \
			CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch $(GOBUILD) $(RELEASE_LDFLAGS) \
				-o $(RELEASE_DIR)/$$binary ./cmd/$$component || exit 1; \
			tar -czf $(RELEASE_DIR)/$$component-$(VERSION)-$$os-$$arch.tar.gz \
				-C $(RELEASE_DIR) $$binary; \
			rm -f $(RELEASE_DIR)/$$binary; \
		done \
	done
	@echo "Release archives built in $(RELEASE_DIR)/"

release-manifest:
	@command -v $(firstword $(KUSTOMIZE)) >/dev/null 2>&1 || { \
		echo "error: $(firstword $(KUSTOMIZE)) is required to generate $(K8S_MANIFEST); install kubectl or set KUSTOMIZE" >&2; \
		exit 1; \
	}
	@echo "Generating Kubernetes manifest ($(K8S_MANIFEST)) for $(VERSION)..."
	@mkdir -p $(RELEASE_DIR)/kustomize
	@{ \
		echo 'apiVersion: kustomize.config.k8s.io/v1beta1'; \
		echo 'kind: Kustomization'; \
		echo 'resources:'; \
		echo '  - ../../../deploy/kustomize/base'; \
		echo 'images:'; \
		for image in $(K8S_IMAGES); do \
			echo "  - name: $$image"; \
			echo "    newTag: $(VERSION)"; \
		done; \
	} > $(RELEASE_DIR)/kustomize/kustomization.yaml
	@$(KUSTOMIZE) $(RELEASE_DIR)/kustomize > $(RELEASE_DIR)/$(K8S_MANIFEST).tmp
	@mv $(RELEASE_DIR)/$(K8S_MANIFEST).tmp $(RELEASE_DIR)/$(K8S_MANIFEST)
	@rm -rf $(RELEASE_DIR)/kustomize
	@echo "Pinning injected sidecar image ($(SIDECAR_IMAGE_ARG):$(VERSION))..."
	@sed -E 's#($(SIDECAR_IMAGE_ARG):)[^[:space:]"]+#\1$(VERSION)#' \
		$(RELEASE_DIR)/$(K8S_MANIFEST) > $(RELEASE_DIR)/$(K8S_MANIFEST).tmp
	@mv $(RELEASE_DIR)/$(K8S_MANIFEST).tmp $(RELEASE_DIR)/$(K8S_MANIFEST)
	@echo "Kubernetes manifest written to $(RELEASE_DIR)/$(K8S_MANIFEST)"

release-checksums:
	@echo "Generating SHA256SUMS..."
	@stale=$$(cd $(RELEASE_DIR) && ls *.tar.gz 2>/dev/null | grep -vF -- "-$(VERSION)-" || true); \
	if [ -n "$$stale" ]; then \
		echo "error: $(RELEASE_DIR) contains archives for another version:" >&2; \
		echo "$$stale" >&2; \
		echo "run 'make release' (which removes stale archives) or delete them first" >&2; \
		exit 1; \
	fi
	@cd $(RELEASE_DIR) && sha256sum *.tar.gz $(K8S_MANIFEST) > SHA256SUMS.tmp
	@cd $(RELEASE_DIR) && mv SHA256SUMS.tmp SHA256SUMS
	@echo "Checksums written to $(RELEASE_DIR)/SHA256SUMS"

release-verify:
	@cd $(RELEASE_DIR) && sha256sum -c SHA256SUMS
	@echo "Verifying $(K8S_MANIFEST) pins every outpostnet image ref to $(VERSION)..."
	@manifest=$(RELEASE_DIR)/$(K8S_MANIFEST); \
	if grep -nE 'outpostnet-[a-z]+:latest' "$$manifest"; then \
		echo "error: $(K8S_MANIFEST) still has unpinned outpostnet image refs (^) instead of $(VERSION)" >&2; \
		exit 1; \
	fi; \
	grep -qE -- "$(SIDECAR_IMAGE_ARG):$(VERSION)$$" "$$manifest" || { \
		echo "error: $(K8S_MANIFEST) does not pin $(SIDECAR_IMAGE_ARG) to $(VERSION)" >&2; \
		exit 1; \
	}; \
	refs=$$(grep -oE '(image: *|--sidecar-image=)(outpostnet-[a-z]+:[^[:space:]"]+)' "$$manifest" | sed -E 's/^(image: *|--sidecar-image=)//'); \
	if [ -z "$$refs" ]; then \
		echo "error: no outpostnet image refs found in $(K8S_MANIFEST)" >&2; \
		exit 1; \
	fi; \
	bad=$$(printf '%s\n' "$$refs" | grep -vE -- ":$(VERSION)$$" || true); \
	if [ -n "$$bad" ]; then \
		echo "error: $(K8S_MANIFEST) has outpostnet image refs not pinned to $(VERSION):" >&2; \
		printf '  %s\n' $$bad >&2; \
		exit 1; \
	fi; \
	echo "All outpostnet image refs pinned to $(VERSION):"; \
	printf '  %s\n' "$$refs"

# Help
help:
	@echo "OutpostNet v2 Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  build           - Build all binaries"
	@echo "  build-server    - Build server binary only"
	@echo "  build-sidecar   - Build sidecar binary only"
	@echo "  build-controller - Build controller binary only"
	@echo "  build-agent     - Build agent binary only"
	@echo "  build-service   - Build service (OutpostService entry) binary only"
	@echo "  build-state-reset - Build offline state clock recovery tool"
	@echo "  install         - Build and install binaries to project root"
	@echo ""
	@echo "  test            - Run all tests"
	@echo "  test-verbose    - Run tests with verbose output"
	@echo "  test-cover      - Run tests with coverage report"
	@echo "  test-timesync-integration - Run the NTS multi-process sync test (Linux, root, chrony >= 4.3)"
	@echo ""
	@echo "  fmt             - Format code"
	@echo "  vet             - Run go vet"
	@echo "  lint            - Run golint"
	@echo "  tidy            - Tidy modules"
	@echo "  deps            - Download dependencies"
	@echo ""
	@echo "  clean           - Clean build artifacts"
	@echo "  clean-all       - Clean all generated files"
	@echo ""
	@echo "  bench-tunnel         - Run QUIC tunnel benchmarks"
	@echo "  release              - Build archives (6 components × 4 OS/arch), k8s manifest, and SHA256SUMS"
	@echo "  release-verify       - Verify release artifacts against SHA256SUMS"
	@echo "  e2e-k8s / e2e-controller / e2e-cross-region / e2e-relay - Run hack/ E2E scripts (*-cleanup to tear down)"
	@echo "  help                 - Show this help"
