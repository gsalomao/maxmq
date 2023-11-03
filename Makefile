# Copyright 2023 The MaxMQ Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Project parameters
NAME            = maxmq
BUILD_DIR       = build
COVERAGE_DIR    = coverage
MAIN_FILE       = cmd/maxmq/main.go

# Colors
GREEN       := $(shell tput -Txterm setaf 2)
YELLOW      := $(shell tput -Txterm setaf 3)
WHITE       := $(shell tput -Txterm setaf 7)
CYAN        := $(shell tput -Txterm setaf 6)
RESET       := \033[0m
BOLD        := \033[0;1m

# Build information
VERSION         = $(shell git describe --tags --always --dirty | sed -e 's/^v//')
REVISION        = $(shell git rev-parse HEAD)
BUILD_TIME      = $(shell date -u '+%Y-%m-%d %H:%M:%S')
DISTRIBUTION    = "OSS"

LDFLAGS ="\
	-X 'github.com/gsalomao/maxmq/internal/info.version=${VERSION}' \
	-X 'github.com/gsalomao/maxmq/internal/info.revision=${REVISION}' \
	-X 'github.com/gsalomao/maxmq/internal/info.buildTime=${BUILD_TIME}' \
	-X 'github.com/gsalomao/maxmq/internal/info.distribution=${DISTRIBUTION}' \
	"

.PHONY: all
all: help

## Project
.PHONY: init
init: ## Initialize project
	$(call print_task,"Installing Git hooks")
	@cp scripts/githooks/* .git/hooks
	@chmod +x .git/hooks/*

.PHONY: fmt
fmt: ## Format source code
	$(call print_task,"Formatting source code")
	@go fmt ./...

.PHONY: generate
generate: ## Generate files
	$(call print_task,"Generating mock files")
	@mockery

## Build
.PHONY: build
build: ## Build server
	$(call print_task,"Building server")
	@mkdir -p ${BUILD_DIR}
	@go build -o ${BUILD_DIR}/$(NAME) -ldflags ${LDFLAGS} $(MAIN_FILE)

.PHONY: image
image: ## Build Docker image
	$(call print_task,"Building Docker image")
	@docker build . -t maxmq:${VERSION}
	@docker tag maxmq:${VERSION} maxmq:latest

.PHONE: update
update: ## Update dependencies
	$(call print_task,"Updating dependencies")
	@go get -u ./...
	@go mod tidy

.PHONY: clean
clean: ## Clean build folder
	$(call print_task,"Cleaning build folder")
	@go clean
	@rm -rf ${BUILD_DIR}

## Run
.PHONY: start
start: build ## Start server
	$(call print_task,"Starting server")
	@$(BUILD_DIR)/$(NAME) start

.PHONY: start-dev
start-dev: ## Start server in development mode
	$(call print_task,"Starting server in development mode")
	@reflex -s -d none -r "\.go" -- sh -c "go run $(MAIN_FILE) start"

## Test
.PHONY: test
test: ## Run unit tests
	$(call print_task,"Running unit tests")
	@gotestsum --format pkgname --packages ./cmd/... ./internal/... -- -timeout 10s -race

.PHONY: test-dev
test-dev: ## Run unit tests in development mode
	$(call print_task,"Running unit tests in development mode")
	@gotestsum --format testname --packages ./cmd/... --packages ./internal/... --watch -- -timeout 10s -race

.PHONY: coverage
coverage: ## Run unit tests with coverage report
	$(call print_task,"Running unit tests")
	@rm -rf ${COVERAGE_DIR}
	@mkdir -p ${COVERAGE_DIR}
	@go test -timeout 10s -cover -covermode=atomic -race \
		-coverprofile=$(COVERAGE_DIR)/coverage.out ./cmd/... ./internal/...

	$(call print_task,"Generating coverage report")
	@go tool cover -func $(COVERAGE_DIR)/coverage.out

.PHONY: coverage-html
coverage-html: coverage ## Open the coverage report in the browser
	$(call print_task,"Opening coverage report")
	@go tool cover -html coverage/coverage.out

## Analyze
.PHONY: inspect
inspect: vet lint complexity security ## Inspect source code

.PHONY: vet
vet: ## Run vet command
	$(call print_task,"Running vet command")
	@go vet ./...

.PHONY: lint
lint: ## Lint source code
	$(call print_task,"Linting source code")
	@golint  -set_exit_status $(go list ./...)
	@golangci-lint run $(go list ./...)

.PHONY: complexity
complexity: ## Calculates cyclomatic complexity
	$(call print_task,"Calculating cyclomatic complexity")
	@gocyclo -top 10 .
	@gocyclo -over 15 -avg .

.PHONY: security
security: ## Run security checks
	$(call print_task,"Running govulncheck")
	@govulncheck ./... || true

	$(call print_task,"Running gosec")
	@gosec -quiet -exclude-dir testdata ./...

## Help
.PHONY: help
help: ## Show this help
	@echo 'Usage:'
	@echo '  ${YELLOW}make${RESET} ${GREEN}<target>${RESET}'
	@echo ''
	@echo 'Targets'
	@awk 'BEGIN {FS = ":.*?## "} { \
		if (/^[a-zA-Z0-9\/_-]+:.*?##.*$$/) { \
			printf "    ${YELLOW}%-20s${GREEN}%s${RESET}\n", $$1, $$2} \
			else if (/^## .*$$/) { \
				printf "  ${CYAN}%s:${RESET}\n", substr($$1,4)\
			} \
		}' $(MAKEFILE_LIST)

define print_task
	@printf "${CYAN}==>${BOLD} %s...${RESET}\n" $(1)
endef
