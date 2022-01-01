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
NAME=maxmq
BUILD_PATH=bin
COVERAGE_PATH=coverage

# Go commands
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOVET=$(GOCMD) vet
GOFMT=$(GOCMD) fmt
GOLINT=golangci-lint run
GOIMPORTS=goimports

GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
CYAN   := $(shell tput -Txterm setaf 6)
RESET  := $(shell tput -Txterm sgr0)

.PHONY: all build

all: help

## Build
build: ## Build application
	@echo "--> Building application..."
	@mkdir -p ${BUILD_PATH}
	@$(GOBUILD) -o ${BUILD_PATH}/$(NAME) main.go

clean: ## Clean build folder
	@echo "--> Cleaning build folder..."
	@$(GOCLEAN)
	@rm -rf ${BUILD_PATH}

## Test
unit: ## Run unit tests
	@echo "--> Running unit tests..."
	@$(GOTEST) -v ./...

coverage: ## Run unit tests with coverage report
	@echo "--> Running unit tests..."
	$(GOTEST) -cover -covermode=count \
		-coverprofile=$(COVERAGE_PATH)/profile.cov ./...

	@echo "--> Generating coverage report..."
	$(GOCMD) tool cover -func $(COVERAGE_PATH)/profile.cov

## Analyze
vet: ## Examine source code
	@echo "--> Examining source code..."
	@$(GOVET) ./...

fmt: ## Format source code
	@echo "--> Formatting source code..."
	@$(GOFMT) ./...

lint: ## Lint source code
	@echo "--> Linting source code..."
	@$(GOLINT) ./...

imports: ## Update Go import lines
	@echo "--> Updating Go imports..."
	@$(GOIMPORTS) -l -w .

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
