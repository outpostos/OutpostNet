# Makefile for OutpostNet v2

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

.PHONY: all build clean test test-verbose test-cover lint fmt vet tidy help

# Default target
all: clean fmt vet test build

# Build targets
build: build-server build-sidecar

build-server:
	@echo "Building $(SERVER_BINARY)..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(SERVER_BINARY) ./cmd/outpostnet-server

build-sidecar:
	@echo "Building $(SIDECAR_BINARY)..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(SIDECAR_BINARY) ./cmd/outpostnet-sidecar

# Install builds binaries to project root for easy testing
install: build
	@echo "Installing binaries to project root..."
	@cp $(BUILD_DIR)/$(SERVER_BINARY) ./
	@cp $(BUILD_DIR)/$(SIDECAR_BINARY) ./

# Test targets
test:
	@echo "Running tests..."
	$(GOTEST) -v ./...

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
	@rm -f $(SERVER_BINARY) $(SIDECAR_BINARY)
	@rm -rf $(BUILD_DIR)
	@rm -f coverage.out coverage.html

clean-all: clean
	@echo "Cleaning all generated files..."
	@rm -f /tmp/outpostnet-*.pid /tmp/outpostnet-*.log

# Docker targets
docker-build:
	@echo "Building Docker images..."
	docker build -t outpostnet-server:$(VERSION) -t outpostnet-server:latest \
		--build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) --build-arg BUILD_TIME=$(BUILD_TIME) \
		-f build/docker/Dockerfile.server .
	docker build -t outpostnet-sidecar:$(VERSION) -t outpostnet-sidecar:latest \
		--build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) --build-arg BUILD_TIME=$(BUILD_TIME) \
		-f build/docker/Dockerfile.sidecar .

# K8s E2E test
e2e-k8s:
	@echo "Running K8s E2E test..."
	./hack/e2e-k8s.sh

e2e-k8s-cleanup:
	./hack/e2e-k8s.sh cleanup

# Release targets
release: clean fmt vet test
	@echo "Building release binaries..."
	@mkdir -p $(BUILD_DIR)/release
	@GOOS=linux GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/release/$(SERVER_BINARY)-linux-amd64 ./cmd/outpostnet-server
	@GOOS=linux GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/release/$(SIDECAR_BINARY)-linux-amd64 ./cmd/outpostnet-sidecar
	@GOOS=darwin GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/release/$(SERVER_BINARY)-darwin-amd64 ./cmd/outpostnet-server
	@GOOS=darwin GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/release/$(SIDECAR_BINARY)-darwin-amd64 ./cmd/outpostnet-sidecar
	@GOOS=darwin GOARCH=arm64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/release/$(SERVER_BINARY)-darwin-arm64 ./cmd/outpostnet-server
	@GOOS=darwin GOARCH=arm64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/release/$(SIDECAR_BINARY)-darwin-arm64 ./cmd/outpostnet-sidecar
	@GOOS=windows GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/release/$(SERVER_BINARY)-windows-amd64.exe ./cmd/outpostnet-server
	@GOOS=windows GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/release/$(SIDECAR_BINARY)-windows-amd64.exe ./cmd/outpostnet-sidecar
	@echo "Release binaries built in $(BUILD_DIR)/release/"

# Help
help:
	@echo "OutpostNet v2 Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  build           - Build server and sidecar binaries"
	@echo "  build-server    - Build server binary only"
	@echo "  build-sidecar   - Build sidecar binary only"
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
	@echo "  release         - Build release binaries for all platforms"
	@echo "  docker-build    - Build Docker images"
	@echo "  help            - Show this help"
