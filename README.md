![MaxMQ logo](docs/assets/logo.png)

# MaxMQ
[![license](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

MaxMQ is an open-source, cloud-native, highly available and highly scalable
message broker for IoT, compliant with the MQTT v3.0, v3.1.1 and v5.0
specification.

#### What is MQTT?
MQTT stands for MQ Telemetry Transport. It is a publish/subscribe, extremely
simple and lightweight messaging protocol, designed for constrained devices and
low-bandwidth, high-latency or unreliable networks.
[Learn more](https://mqtt.org/faq)

### Features
Some of the features you get with MaxMQ are:
- [ ] MQTT V3.1 / V3.1.1 and V5.0 protocol specification support
	* QoS0, QoS1, QoS2 message support
	* Persistent and offline message support
	* Retained message support
	* Last Will message support
- [ ] Standard TCP
- [ ] MQTT over WebSocket
- [ ] SSL / TLS
- [ ] Trie-based Subscription model
- [ ] `$SYS` support
- [ ] User name and password authentication
- [ ] Access control (ACL) based on client ID or user name
- [ ] Multi-server node (Cluster)
- [ ] Automatic network partition healing
- [ ] Extensible through plugins
- [ ] WebUI

### Roadmap
Some of the features that are in the roadmap:
- [ ] LDAP authentication
- [ ] Redis, MySQL, PostgreSQL, MongoDB, HTTP authentication
- [ ] Connection rate limit
- [ ] Message rate limit
- [ ] MQTT bridge support
- [ ] MQTT-SN protocol support
- [ ] CoAP protocol support

## Licenses
This project is released under [Apache 2.0 License](./LICENSE).
