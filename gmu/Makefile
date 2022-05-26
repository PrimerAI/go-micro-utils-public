help:  ## Display this help text
	@grep -E '^[a-zA-Z_%\-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ========
# = Lint =
# ========

fmt:  ## Run go fmt against code
	gofmt -s -w .
	gofumpt -w .

vet:  ## Run go vet against code
	go vet ./...

lint-go:
	golangci-lint run

lint: lint-go  ## Run linting on go files

tidy-go:
	golangci-lint run --fix -E gofmt
	golangci-lint run --fix -E gofumpt
	golangci-lint run --fix -E goimports

tidy-go-mod:
	go mod tidy

tidy: tidy-go tidy-go-mod  ## Fix linting on go files

# =========
# = Tests =
# =========

test: fmt vet  ## Run tests
	go test ./... $(TEST_FLAGS) -coverprofile $(or $(COVERAGE_FILE),cover.out)
	go tool cover -func $(or $(COVERAGE_FILE),cover.out)

# =========
# = Build =
# =========

build: fmt lint  ## Run build
	go build ./...

build-mocks:  ## Build mocks
	rm -rf mocks
	mockery --all --keeptree --with-expecter

install-mockery:  ## Install mockery
	brew install mockery
	brew upgrade mockery

