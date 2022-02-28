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

# Build stage
FROM golang:1.17-alpine3.15 AS build

RUN apk add --no-cache git make ncurses

# Set terminal colors
ENV TERM=xterm-256color

# Set the Working Directory inside the container
WORKDIR /tmp/maxmq

# Populate the module cache based on the go.{mod,sum} files.
COPY go.mod .
COPY go.sum .
RUN go mod download

# Build application
COPY . .
RUN make build

# Start fresh from a smaller image
FROM alpine:3.15
LABEL maintainer="Gustavo Salomao <gsalomao.eng@gmail.com>"
LABEL description="MaxMQ is a cloud-native and high-performance message broker"

# Create user and group
RUN addgroup -S maxmq && adduser -S maxmq -G maxmq

# Add application from build stage
COPY --from=build /tmp/maxmq/bin/maxmq /usr/bin/maxmq
COPY --from=build /tmp/maxmq/config/maxmq.conf /etc/maxmq.conf

# MaxMQ uses the following ports:
# - 1883 for MQTT
# - 8888 for Prometheus
EXPOSE 1883 8888

# Run application
CMD ["/usr/bin/maxmq", "start"]
