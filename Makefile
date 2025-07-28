# Makefile for OutpostNet

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod

# Binary names
SERVER_BINARY=outpostnet-server
CLIENT_BINARY=outpostnet-client

# Build directory
BUILD_DIR=build

# Version information
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT = $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME = $(shell date +%Y-%m-%dT%H:%M:%S%z)

# Go build flags
LDFLAGS = -ldflags "-X main.Version=$(VERSION) -X main.Commit=$(COMMIT) -X main.BuildTime=$(BUILD_TIME)"

.PHONY: all build clean test test-verbose test-cover lint fmt vet tidy demo examples help

# Default target
all: clean fmt vet test build

# Build targets
build: build-server build-client

build-server:
	@echo "Building $(SERVER_BINARY)..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(SERVER_BINARY) ./cmd/outpostnet-server

build-client:
	@echo "Building $(CLIENT_BINARY)..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(CLIENT_BINARY) ./cmd/outpostnet-client


# Install builds binaries to project root for easy testing
install: build
	@echo "Installing binaries to project root..."
	@cp $(BUILD_DIR)/$(SERVER_BINARY) ./
	@cp $(BUILD_DIR)/$(CLIENT_BINARY) ./

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

test-integration:
	@echo "Running integration tests..."
	$(GOTEST) -v -run TestServerStartup
	$(GOTEST) -v -run TestPhase1Complete

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

# Development targets
demo: install
	@echo "Starting outpostnet demo environment..."
	@./scripts/quick-start.sh start

demo-stop:
	@echo "Stopping outpostnet demo environment..."
	@./scripts/quick-start.sh stop

demo-test:
	@echo "Testing outpostnet demo environment..."
	@./scripts/quick-start.sh test

demo-status:
	@echo "Checking outpostnet demo environment status..."
	@./scripts/quick-start.sh status

# Configuration examples (see examples/ directory)
examples:
	@echo "Configuration examples are available in the examples/ directory:"
	@echo "  examples/configs/           - Server and client configuration files"
	@echo "  examples/quick-start/       - Quick start demo"
	@echo "  examples/kubectl/           - Kubernetes integration examples"
	@echo "  See examples/README.md for detailed usage instructions"

# Clean targets
clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	@rm -f $(SERVER_BINARY) $(CLIENT_BINARY)
	@rm -rf $(BUILD_DIR)
	@rm -f coverage.out coverage.html

clean-all: clean
	@echo "Cleaning all generated files..."
	@rm -f /tmp/outpostnet-*.pid /tmp/outpostnet-*.log
	@rm -f /tmp/proxy-config.yaml /tmp/index.html

# Docker targets (for future use)
docker-build:
	@echo "Building Docker images..."
	@docker build -t outpostnet-server:$(VERSION) -f docker/Dockerfile.server .
	@docker build -t outpostnet-client:$(VERSION) -f docker/Dockerfile.client .

# Release targets
release: clean fmt vet test
	@echo "Building release binaries..."
	@mkdir -p $(BUILD_DIR)/release
	
	# Linux amd64
	@GOOS=linux GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/release/$(SERVER_BINARY)-linux-amd64 ./cmd/outpostnet-server
	@GOOS=linux GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/release/$(CLIENT_BINARY)-linux-amd64 ./cmd/outpostnet-client
	
	# Darwin amd64 (Intel Mac)
	@GOOS=darwin GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/release/$(SERVER_BINARY)-darwin-amd64 ./cmd/outpostnet-server
	@GOOS=darwin GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/release/$(CLIENT_BINARY)-darwin-amd64 ./cmd/outpostnet-client
	
	# Darwin arm64 (Apple Silicon)
	@GOOS=darwin GOARCH=arm64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/release/$(SERVER_BINARY)-darwin-arm64 ./cmd/outpostnet-server
	@GOOS=darwin GOARCH=arm64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/release/$(CLIENT_BINARY)-darwin-arm64 ./cmd/outpostnet-client
	
	# Windows amd64
	@GOOS=windows GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/release/$(SERVER_BINARY)-windows-amd64.exe ./cmd/outpostnet-server
	@GOOS=windows GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/release/$(CLIENT_BINARY)-windows-amd64.exe ./cmd/outpostnet-client
	
	@echo "Release binaries built in $(BUILD_DIR)/release/"

# Help
help:
	@echo "outpostnet Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  build           - Build server and client binaries"
	@echo "  build-server    - Build server binary only"
	@echo "  build-client    - Build client binary only"
	@echo "  install         - Build and install binaries to project root"
	@echo ""
	@echo "  test            - Run all tests"
	@echo "  test-verbose    - Run tests with verbose output"
	@echo "  test-cover      - Run tests with coverage report"
	@echo "  test-integration- Run integration tests"
	@echo ""
	@echo "  fmt             - Format code"
	@echo "  vet             - Run go vet"
	@echo "  lint            - Run golint"
	@echo "  tidy            - Tidy modules"
	@echo "  deps            - Download dependencies"
	@echo ""
	@echo "  demo            - Start demo environment"
	@echo "  demo-stop       - Stop demo environment"
	@echo "  demo-test       - Test demo environment"
	@echo "  demo-status     - Check demo environment status"
	@echo ""
	@echo "  examples        - Show configuration examples information"
	@echo ""
	@echo "  clean           - Clean build artifacts"
	@echo "  clean-all       - Clean all generated files"
	@echo ""
	@echo "  release         - Build release binaries for all platforms"
	@echo "  docker-build    - Build Docker images"
	@echo "  help            - Show this help"
	@echo ""
	@echo "Environment variables:"
	@echo "  VERSION         - Version string (default: git describe)"