# MaxMQ

[![build](https://github.com/gsalomao/maxmq/actions/workflows/build.yml/badge.svg)](https://github.com/gsalomao/maxmq/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/gsalomao/maxmq/branch/master/graph/badge.svg?token=FUXEU188HA)](https://codecov.io/gh/gsalomao/maxmq)
[![Go Report Card](https://goreportcard.com/badge/github.com/gsalomao/maxmq)](https://goreportcard.com/report/github.com/gsalomao/maxmq)
[![license](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

> **_NOTE:_**  This project is under development, DO NOT use it in production.

MaxMQ is an open-source, cloud-native, and high-performance message broker for IoT, compliant with the MQTT 3.1, 3.1.1
and 5.0 specification.

MaxMQ is an [Apache 2.0](./LICENSE) licensed MQTT broker developed in [Go](https://go.dev/), and designed with the
following characteristics:

- **High Availability:** The system must have low latency and maintain highly available for any operations even if one
or more nodes are in a failure state, or if there's a network failure.
- **High Scalability:** The system must scale both vertically (make use of modern multicore, multi-CPU architectures,
and high-capacity storage devices), as well as horizontally (adding more nodes).
- **High Performance:** The system must run as close to the hardware as possible to deliver low and consistent latency
as well as very high throughput.
- **High Maintainability:** The system must be easy to operate with easy-to-use features that require minimal initial
configuration.

#### What is MQTT?

MQTT stands for MQ Telemetry Transport. It is a publish-subscribe, extremely simple and lightweight messaging protocol,
designed for constrained devices and low-bandwidth, high-latency or unreliable networks.
[Learn more](https://mqtt.org/faq)

### Features

- [ ] MQTT 3.1, 3.1.1 and 5.0 protocol *(Coming soon)*
- [ ] Metrics support ([Prometheus](https://prometheus.io/)) *(Coming soon)*

### Roadmap

- [ ] MQTT over WebSocket
- [ ] SSL / TLS
- [ ] `$SYS` support
- [ ] Username and password authentication
- [ ] Access control (ACL) based on client ID, username, or client certificate
- [ ] Connection rate limit
- [ ] Message rate limit
- [ ] High-Availability (Cluster)
- [ ] Rule Engine
- [ ] Extensible through plugins
- [ ] WebUI

## Design

For an in-depth understanding of the MaxMQ architecture, see the [System Design](./docs/system-design.md).

## Contributing

Please follow the
[MaxMQ Contributing Guide](https://github.com/gsalomao/maxmq/blob/master/CONTRIBUTING.md)

## License

This project is released under
[Apache 2.0 License](https://github.com/gsalomao/maxmq/blob/master/LICENSE).
