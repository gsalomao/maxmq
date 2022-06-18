# MaxMQ

[![build](https://github.com/gsalomao/maxmq/actions/workflows/build.yml/badge.svg)](https://github.com/gsalomao/maxmq/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/gsalomao/maxmq/branch/master/graph/badge.svg?token=FUXEU188HA)](https://codecov.io/gh/gsalomao/maxmq)
[![Go Report Card](https://goreportcard.com/badge/github.com/gsalomao/maxmq)](https://goreportcard.com/report/github.com/gsalomao/maxmq)
[![license](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

> **_NOTE:_**  This project is under development, DO NOT use it in production.

MaxMQ is an open-source, cloud-native, and high-performance message broker for
IoT, compliant with the MQTT 3.1, 3.1.1 and 5.0 specification.

MaxMQ is an [Apache 2.0](./LICENSE) licensed MQTT broker developed in
[Go](https://go.dev/), and built for vertical and horizontal scaling.

#### What is MQTT?

MQTT stands for MQ Telemetry Transport. It is a publish-subscribe, extremely
simple and lightweight messaging protocol, designed for constrained devices and
low-bandwidth, high-latency or unreliable networks.
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
- [ ] Multi-server (Cluster)
- [ ] Automatic network partition healing (Cluster)
- [ ] Extensible through plugins
- [ ] WebUI
- [ ] Redis, MySQL, PostgreSQL, MongoDB, HTTP authentication
- [ ] MQTT bridge support
- [ ] MQTT-SN protocol support
- [ ] CoAP protocol support

## Contributing

Please follow the 
[MaxMQ Contributing Guide](https://github.com/gsalomao/maxmq/blob/master/CONTRIBUTING.md)

## License

This project is released under 
[Apache 2.0 License](https://github.com/gsalomao/maxmq/blob/master/LICENSE).
