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
NAME			= maxmq
BUILD_DIR		= bin
COVERAGE_DIR	= coverage
MAIN_FILE		= cmd/maxmq/main.go

# Colors
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
CYAN   := $(shell tput -Txterm setaf 6)
RESET  := $(shell tput -Txterm sgr0)
BOLD	= \033[0;1m
CYAN	= \033[0;36m
NO_COLOR	= \033[0m

# Build parameters
VERSION = $(shell git describe --tags --always --dirty)

.PHONY: all build coverage

all: help

LDFLAGS ="\
	-X 'github.com/gsalomao/maxmq/pkg/cli.version=${VERSION}' \
"

## Build
build: ## Build application
	$(call print_task,"Building application")
	@mkdir -p ${BUILD_DIR}
	@go build -o ${BUILD_DIR}/$(NAME) -ldflags ${LDFLAGS} $(MAIN_FILE)
	$(call print_task_result,"Building application","done")

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

## Test
test: ## Run tests
	$(call print_task,"Running tests")
	@gotestsum --format pkgname --packages ./pkg/... -- -timeout 3s
	$(call print_task_result,"Running tests","done")

test-dev: ## Run tests in development mode
	$(call print_task,"Running tests in development mode")
	@gotestsum --format testname --packages ./pkg/... --watch -- -timeout 3s

coverage: ## Run tests with coverage report
	$(call print_task,"Running tests")
	@rm -rf ${COVERAGE_DIR}
	@mkdir -p ${COVERAGE_DIR}
	@go test -cover -covermode=atomic -race \
		-coverprofile=$(COVERAGE_DIR)/coverage.out ./...
	$(call print_task_result,"Running tests","done")

	$(call print_task,"Generating coverage report")
	@go tool cover -func $(COVERAGE_DIR)/coverage.out
	$(call print_task_result,"Generating coverage report","done")

coverage-html: coverage ## Open the coverage report in the browser
	$(call print_task,"Opening coverage report")
	@go tool cover -html coverage/coverage.out

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
	@golangci-lint run ./...
	$(call print_task_result,"Linting source code","done")

imports: ## Update Go import lines
	$(call print_task,"Updating Go imports")
	@goimports -l -w .
	$(call print_task_result,"Updating Go imports","done")

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
	@echo "${CYAN}==>${BOLD} $(1)...${NO_COLOR}"
endef

define print_task_result
	@echo "${CYAN}==>${NOCOLOR} $(1)... $(2)"
endef
