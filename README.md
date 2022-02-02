# MaxMQ
[![build](https://github.com/gsalomao/maxmq/actions/workflows/build.yml/badge.svg)](https://github.com/gsalomao/maxmq/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/gsalomao/maxmq/branch/master/graph/badge.svg?token=FUXEU188HA)](https://codecov.io/gh/gsalomao/maxmq)
[![license](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

MaxMQ is a cloud-native and highly scalable message broker for IoT, compliant
with the MQTT 3.1, 3.1.1 and 5.0 specification.

#### What is MQTT?
MQTT stands for MQ Telemetry Transport. It is a publish/subscribe, extremely
simple and lightweight messaging protocol, designed for constrained devices and
low-bandwidth, high-latency or unreliable networks.
[Learn more](https://mqtt.org/faq)

### Features
Some of the features you get with MaxMQ are:
- [ ] MQTT 3.1, 3.1.1 and 5.0 protocol specification support
	* QoS0, QoS1, QoS2 message support
	* Persistent and offline message support
	* Retained message support
	* Last Will message support
- [ ] Standard TCP
- [ ] MQTT over WebSocket
- [ ] SSL / TLS
- [ ] `$SYS` support
- [ ] User name and password authentication
- [ ] Access control (ACL) based on client ID or user name
- [ ] Multi-server node (Cluster)
- [ ] Automatic network partition healing
- [ ] Extensible through plugins
- [ ] WebUI

### Roadmap
Some of the features that are in the roadmap:
- [ ] Connection rate limit
- [ ] Message rate limit
- [ ] Redis, MySQL, PostgreSQL, MongoDB, HTTP authentication
- [ ] MQTT bridge support
- [ ] MQTT-SN protocol support
- [ ] CoAP protocol support

## Licenses
This project is released under [Apache 2.0 License](./LICENSE).
