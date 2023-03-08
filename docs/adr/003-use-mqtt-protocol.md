# 3. Use MQTT protocol

Date: 2022-02-06

## Status

Accepted

## Context

The MaxMQ is message broker designed for IoT (Internet of Things) applications.
This means that the MaxMQ receives messages from IoT devices and distribute them
to other software interested in those messages.

IoT devices sometime are constrained devices, this means that they can have low
memory footprint, low power (in battery-powered devices), low connectivity, etc.
Those constraints are challenges when they have to communicate with the internet
and, due to this, the protocol used to communicate through the internet must be
chosen carefully.

## Decision

Today, there are several protocol in the IoT market, such as MQTT, CoAP,
LoRaWAN, and so on. Choose one protocol for the MaxMQ is not a simple task, and
we chose for the MaxMQ the MQTT as the main protocol.

This does not mean that the MaxMQ will not implement other protocols than the
MQTT, but it means that the first protocol, and the main protocol, will be the
MQTT protocol.

## Consequences

Choosing the MQTT as the main protocol means that the MaxMQ must be compliant
with the MQTT standards 3.1, 3.1.1 and 5.0.

It must allow clients connect with the server using standard TCP connections or
through WebSocket. The MaxMQ must also allow clients connect using encrypted
connection (SSL/TLS), and must allow clients to authenticate using, at least, a
simple username/password.

The MaxMQ must allow the administrator to set MQTT parameters, to change the
server's behavior, when starting the server and, if possible, to change them at
runtime.
