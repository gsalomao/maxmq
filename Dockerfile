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

# Argument for Go version
ARG GO_VERSION=1.18

# Build stage
FROM golang:${GO_VERSION} AS build

# Set the Working Directory inside the container
WORKDIR /tmp/maxmq

# Populate the module cache based on the go.{mod,sum} files.
COPY go.mod go.sum ./
RUN go mod download

# Build application
COPY . .
RUN CGO_ENABLED=0 make build

# Start fresh from a smaller image
FROM gcr.io/distroless/static
LABEL maintainer="Gustavo Salomao <gsalomao.eng@gmail.com>"
LABEL description="Cloud-Native and High-Performance MQTT Broker for IoT"

# Add application from build stage
COPY --from=build /tmp/maxmq/bin/maxmq /usr/bin/maxmq
COPY --from=build /tmp/maxmq/config/maxmq.conf /etc/maxmq.conf

# MaxMQ uses the following ports:
# - 1883 for MQTT
# - 8888 for Prometheus
EXPOSE 1883 8888

# Run application
CMD ["/usr/bin/maxmq", "start"]
