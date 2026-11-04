.PHONY: all build test lint fmt tidy vet cover clean help

GO ?= go
GOLANGCI_LINT ?= golangci-lint
PKG := ./...

.DEFAULT_GOAL := help

all: fmt lint test ## Format, lint, and test

build: ## Build all packages
	$(GO) build ./...

test: ## Run tests
	$(GO) test -race -count=1 $(PKG)

vet: ## Run go vet
	$(GO) vet $(PKG)

lint: ## Run golangci-lint
	$(GOLANGCI_LINT) run ./...

fmt: ## Format code
	gofmt -w .
	goimports -w . 2>/dev/null || true

tidy: ## Tidy modules
	$(GO) mod tidy

cover: ## Run tests with coverage
	$(GO) test -race -coverprofile=coverage.out -covermode=atomic $(PKG)
	$(GO) tool cover -html=coverage.out -o coverage.html

clean: ## Clean build artifacts
	rm -f coverage.out coverage.html

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'
