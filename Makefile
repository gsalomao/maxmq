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
BUILD_PATH		= bin
COVERAGE_PATH	= coverage

# Go commands
GOCMD		= go
GOBUILD		= $(GOCMD) build
GOCLEAN		= $(GOCMD) clean
GOTEST		= $(GOCMD) test
GOVET		= $(GOCMD) vet
GOFMT		= $(GOCMD) fmt
GOLINT		= golangci-lint run
GOIMPORTS	= goimports

# Colors
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
CYAN   := $(shell tput -Txterm setaf 6)
RESET  := $(shell tput -Txterm sgr0)
BOLD	= \033[0;1m
CYAN	= \033[0;36m
NOCOLOR	= \033[0m

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
	@mkdir -p ${BUILD_PATH}
	@$(GOBUILD) -o ${BUILD_PATH}/$(NAME) -ldflags ${LDFLAGS} main.go
	$(call print_task_result,"Building application","done")

clean: ## Clean build folder
	$(call print_task,"Cleaning build folder")
	@$(GOCLEAN)
	@rm -rf ${BUILD_PATH}
	$(call print_task_result,"Cleaning build folder","done")

## Test
unit: ## Run unit tests
	$(call print_task,"Running unit tests")
	@$(GOTEST) -v ./...
	$(call print_task_result,"Running unit tests","done")

coverage: ## Run unit tests with coverage report
	$(call print_task,"Running unit tests")
	@rm -rf ${COVERAGE_PATH}
	@mkdir -p ${COVERAGE_PATH}
	@$(GOTEST) -cover -covermode=count \
		-coverprofile=$(COVERAGE_PATH)/profile.cov ./...
	$(call print_task_result,"Running unit tests","done")

	$(call print_task,"Generating coverage report")
	@$(GOCMD) tool cover -func $(COVERAGE_PATH)/profile.cov
	$(call print_task_result,"Generating coverage report","done")

## Analyze
vet: ## Examine source code
	$(call print_task,"Examining source code")
	@$(GOVET) ./...
	$(call print_task_result,"Examining source code","done")

fmt: ## Format source code
	$(call print_task,"Formatting source code")
	@$(GOFMT) ./...
	$(call print_task_result,"Formatting source code","done")

lint: ## Lint source code
	$(call print_task,"Linting source code")
	@$(GOLINT) ./...
	$(call print_task_result,"Linting source code","done")

imports: ## Update Go import lines
	$(call print_task,"Updating Go imports")
	@$(GOIMPORTS) -l -w .
	$(call print_task_result,"Updating Go imports","done")

check: vet lint ## Check source code

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
	@echo "${CYAN}==>${BOLD} $(1)...${NOCOLOR}"
endef

define print_task_result
	@echo "${CYAN}==>${NOCOLOR} $(1)... $(2)"
endef
