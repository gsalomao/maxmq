# Copyright 2022 The MaxMQ Authors
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
BUILD_DIR       = bin
COVERAGE_DIR    = coverage
MAIN_FILE       = cmd/maxmq/main.go

# Colors
GREEN       := $(shell tput -Txterm setaf 2)
YELLOW      := $(shell tput -Txterm setaf 3)
WHITE       := $(shell tput -Txterm setaf 7)
CYAN        := $(shell tput -Txterm setaf 6)
RESET       := $(shell tput -Txterm sgr0)
BOLD        = \033[0;1m

# Build parameters
VERSION = $(shell git describe --tags --always --dirty | sed -e 's/^v//')

.PHONY: all build coverage e2e

all: help

LDFLAGS ="-X 'github.com/gsalomao/maxmq/cli.version=${VERSION}'"

## Build
build: ## Build application
	$(call print_task,"Building application")
	@mkdir -p ${BUILD_DIR}
	@go build -o ${BUILD_DIR}/$(NAME) -ldflags ${LDFLAGS} $(MAIN_FILE)
	$(call print_task_result,"Building application","done")

image: ## Build Docker image
	$(call print_task,"Building Docker image")
	@docker build . -t maxmq:${VERSION}
	@docker tag maxmq:${VERSION} maxmq:latest
	$(call print_task_result,"Building Docker image","done")

clean: ## Clean build folder
	$(call print_task,"Cleaning build folder")
	@go clean
	@rm -rf ${BUILD_DIR}
	$(call print_task_result,"Cleaning build folder","done")

## Run
start: build ## Start broker
	$(call print_task,"Starting broker")
	@$(BUILD_DIR)/$(NAME) start

start-dev: ## Start broker in development mode
	$(call print_task,"Starting broker in development mode")
	@reflex -s -d none -r "\.go" -- sh -c "go run $(MAIN_FILE) start"

profile: ## Start broker with CPU/Memory profiler
	$(call print_task,"Starting broker in profiling mode")
	@go build -o ${BUILD_DIR}/$(NAME) -ldflags \
		"-X 'github.com/gsalomao/maxmq/cli.profile=true'" $(MAIN_FILE)
	@$(BUILD_DIR)/$(NAME) start

## Test
test: ## Run unit tests
	$(call print_task,"Running unit tests")
	@gotestsum --format pkgname --packages ./... -- -timeout 3s
	$(call print_task_result,"Running unit tests","done")

test-dev: ## Run unit tests in development mode
	$(call print_task,"Running unit tests in development mode")
	@gotestsum --format testname --packages ./... --watch -- -timeout 3s

coverage: ## Run unit tests with coverage report
	$(call print_task,"Running unit tests")
	@rm -rf ${COVERAGE_DIR}
	@mkdir -p ${COVERAGE_DIR}
	@go test -cover -covermode=atomic -race \
		-coverprofile=$(COVERAGE_DIR)/coverage.out ./...
	$(call print_task_result,"Running unit tests","done")

	$(call print_task,"Generating coverage report")
	@go tool cover -func $(COVERAGE_DIR)/coverage.out
	$(call print_task_result,"Generating coverage report","done")

coverage-html: coverage ## Open the coverage report in the browser
	$(call print_task,"Opening coverage report")
	@go tool cover -html coverage/coverage.out

e2e: ## Run end-to-end (E2E) tests
	$(call print_task,"Running E2E tests")
	@gotestsum --format testname --packages ./e2e -- -tags=e2e -timeout 30s
	$(call print_task_result,"Running E2E tests","done")

## Analyze
vet: ## Examine source code
	$(call print_task,"Examining source code")
	@go vet ./...
	$(call print_task_result,"Examining source code","done")

fmt: ## Format source code
	$(call print_task,"Formatting source code")
	@go fmt ./...
	$(call print_task_result,"Formatting source code","done")

lint: ## Lint source code
	$(call print_task,"Linting source code")
	@golint  -set_exit_status $(go list ./...)
	@golangci-lint run $(go list ./...)
	$(call print_task_result,"Linting source code","done")

complexity: ## Calculates cyclomatic complexities
	$(call print_task,"Calculating cyclomatic complexities")
	@gocyclo -over 10 -avg .
	$(call print_task_result,"Calculating cyclomatic complexities","done")

check: vet lint complexity ## Check source code

## Help
help: ## Show this help
	@echo 'Usage:'
	@echo '  ${YELLOW}make${RESET} ${GREEN}<target>${RESET}'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} { \
		if (/^[a-zA-Z_-]+:.*?##.*$$/) { \
			printf "    ${YELLOW}%-20s${GREEN}%s${RESET}\n", $$1, $$2} \
			else if (/^## .*$$/) { \
				printf "  ${CYAN}%s:${RESET}\n", substr($$1,4)\
			} \
		}' $(MAKEFILE_LIST)

define print_task
	@printf "${CYAN}==>${BOLD} %s...${RESET}\n" $(1)
endef

define print_task_result
	@printf "${CYAN}==> %s... %s${RESET}\n" $(1) $(2)
endef
