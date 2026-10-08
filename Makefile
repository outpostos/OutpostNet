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
RELEASE_COMPONENTS = outpostnet-server outpostnet-sidecar outpostnet-agent outpostnet-controller outpostnet-service
RELEASE_LDFLAGS = -ldflags "-s -w -X $(VERSION_PKG).gitVersion=$(VERSION) -X $(VERSION_PKG).gitCommit=$(COMMIT) -X $(VERSION_PKG).buildDate=$(BUILD_TIME)"

.PHONY: all build clean test test-verbose test-cover lint fmt vet tidy help release

# Default target
all: clean fmt vet test build

# Build targets
build: build-server build-sidecar build-controller build-agent build-service

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
	@rm -f $(SERVER_BINARY) $(SIDECAR_BINARY) $(CONTROLLER_BINARY) $(AGENT_BINARY) $(SERVICE_BINARY)
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
release: fmt vet
	@echo "Building release binaries for all platforms..."
	@mkdir -p $(BUILD_DIR)/release
	@for component in $(RELEASE_COMPONENTS); do \
		for platform in $(RELEASE_PLATFORMS); do \
			os=$$(echo $$platform | cut -d/ -f1); \
			arch=$$(echo $$platform | cut -d/ -f2); \
			binary=$$component-$(VERSION)-$$os-$$arch; \
			echo "  $$os/$$arch $$component"; \
			CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch $(GOBUILD) $(RELEASE_LDFLAGS) \
				-o $(BUILD_DIR)/release/$$binary ./cmd/$$component || exit 1; \
			tar -czf $(BUILD_DIR)/release/$$component-$(VERSION)-$$os-$$arch.tar.gz \
				-C $(BUILD_DIR)/release $$binary; \
			rm -f $(BUILD_DIR)/release/$$binary; \
		done \
	done
	@echo "Release archives built in $(BUILD_DIR)/release/"
	@ls -lh $(BUILD_DIR)/release/

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
	@echo "  install         - Build and install binaries to project root"
	@echo ""
	@echo "  test            - Run all tests"
	@echo "  test-verbose    - Run tests with verbose output"
	@echo "  test-cover      - Run tests with coverage report"
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
	@echo "  release              - Build release archives for all platforms (5 components × 4 OS/arch)"
	@echo "  e2e-k8s / e2e-controller / e2e-cross-region / e2e-relay - Run hack/ E2E scripts (*-cleanup to tear down)"
	@echo "  help                 - Show this help"
