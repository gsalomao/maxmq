# 2. Use Go language

Date: 2022-02-06

## Status

Accepted

## Context

A message broker is a software responsible to handle thousands, or even
millions, of simultaneous connections and handle a high number of messages per
seconds. Due to this, the MaxMQ must use a little resource as possible, in order
to  achieve a high number of concurrent connections and high throughput, in a
cost-effective manner.

We expect, and hope, that the MaxMQ grows, and for this, we need a programming
language that is not only fast and lightweight, but a language that is not
complex to learn and has a lot of adoption in the community.

## Decision

We chose the Go language as the programming language to write the MaxMQ, as Go
provides a performance close to high-performance languages, such as C/C++, but
without all complexities involved with memory management that those low-level
languages have.

The project must adopt the following best practice in order to have and keep a
high-quality code:

* Unit tests with coverage report
* Static code analysis using Go `vet` and `golangci-lint`
* Cyclomatic complexity analysis using `gocyclo`

## Consequences

In order to achieve a high-performance application, and leverage multi-cores
environments, the code must use goroutines extensively. The code must follow
Go standards and all dependencies must be chosen carefully.

The test coverage must not be lower than 80%, but it's expected to be at least
90%. It must not fail during the static code analysis, and it shall not have any
warning.

No code shall have the cyclomatic complexity over 10, and must not have over 15,
as code with cyclomatic complexity above these numbers has higher risk.
