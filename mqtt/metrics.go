/*
 * Copyright 2022 The MaxMQ Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mqtt

import (
	"sync"
	"sync/atomic"

	"github.com/gsalomao/maxmq/logger"
	"github.com/gsalomao/maxmq/mqtt/packet"
	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	packet     packetMetrics
	connection connectionMetrics
	mutex      sync.Mutex
	log        *logger.Logger
}

type packetMetrics struct {
	bytesReceived  packetMetric
	packetReceived packetMetric
	bytesSent      packetMetric
	packetSent     packetMetric
}

type packetMetric struct {
	connect    uint64
	connack    uint64
	pingreq    uint64
	pingresp   uint64
	disconnect uint64
	total      uint64
}

type connectionMetrics struct {
	connectedTotal    uint64
	disconnectedTotal uint64
	activeConnections uint64
}

func newMetrics(log *logger.Logger) *metrics {
	m := &metrics{log: log}

	err := prometheus.Register(m)
	if err != nil {
		log.Warn().Msg("MQTT Failed to register metrics: " + err.Error())
	}

	return m
}

// Describe sends all possible descriptors of metrics collected to the provided
// channel and returns once the last descriptor has been sent.
func (m *metrics) Describe(desc chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(m, desc)
}

// Collect is called by the Prometheus registry when collecting metrics. The
// implementation sends each collected metric via the provided channel and
// returns once the last metric has been sent.
func (m *metrics) Collect(mt chan<- prometheus.Metric) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.log.Trace().Msg("MQTT Collecting metrics")
	m.connection.collect(mt)
	m.packet.collectPackets(mt)
	m.packet.collectBytes(mt)
}

func (m *metrics) packetReceived(pkt packet.Packet) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.packet.received(pkt)
}

func (m *metrics) packetSent(pkt packet.Packet) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.packet.sent(pkt)
}

func (m *metrics) connected() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	atomic.AddUint64(&m.connection.connectedTotal, 1)
	atomic.AddUint64(&m.connection.activeConnections, 1)
}

func (m *metrics) disconnected() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	atomic.AddUint64(&m.connection.disconnectedTotal, 1)
	atomic.AddUint64(&m.connection.activeConnections, ^uint64(0))
}

func (cm connectionMetrics) collect(mt chan<- prometheus.Metric) {
	mt <- prometheus.MustNewConstMetric(
		prometheus.NewDesc("maxmq_mqtt_connected_total",
			"Total of clients connected to the MQTT endpoint", nil, nil),
		prometheus.CounterValue,
		float64(atomic.LoadUint64(&cm.connectedTotal)),
	)

	mt <- prometheus.MustNewConstMetric(
		prometheus.NewDesc("maxmq_mqtt_disconnected_total",
			"Total of clients disconnected with the MQTT endpoint", nil, nil),
		prometheus.CounterValue,
		float64(atomic.LoadUint64(&cm.disconnectedTotal)),
	)

	mt <- prometheus.MustNewConstMetric(
		prometheus.NewDesc("maxmq_mqtt_active_connections",
			"Number of active connection with the MQTT endpoint", nil, nil),
		prometheus.GaugeValue,
		float64(atomic.LoadUint64(&cm.activeConnections)),
	)
}

func (pm packetMetrics) collectPackets(mt chan<- prometheus.Metric) {
	mt <- prometheus.MustNewConstMetric(
		prometheus.NewDesc("maxmq_mqtt_packets_received_total",
			"Number of MQTT packets received", nil, nil),
		prometheus.CounterValue,
		float64(atomic.LoadUint64(&pm.packetReceived.total)),
	)

	mt <- prometheus.MustNewConstMetric(
		prometheus.NewDesc("maxmq_mqtt_packets_sent_total",
			"Number of MQTT packets sent", nil, nil),
		prometheus.CounterValue,
		float64(atomic.LoadUint64(&pm.packetSent.total)),
	)

	mt <- prometheus.MustNewConstMetric(
		prometheus.NewDesc("maxmq_mqtt_packets_received",
			"Number of MQTT packets received", []string{"type"}, nil),
		prometheus.CounterValue,
		float64(atomic.LoadUint64(&pm.packetReceived.connect)),
		"CONNECT",
	)

	mt <- prometheus.MustNewConstMetric(
		prometheus.NewDesc("maxmq_mqtt_packets_sent",
			"Number of MQTT packets sent", []string{"type"}, nil),
		prometheus.CounterValue,
		float64(atomic.LoadUint64(&pm.packetSent.connack)),
		"CONNACK",
	)

	mt <- prometheus.MustNewConstMetric(
		prometheus.NewDesc("maxmq_mqtt_packets_received",
			"Number of MQTT packets received", []string{"type"}, nil),
		prometheus.CounterValue,
		float64(atomic.LoadUint64(&pm.packetReceived.pingreq)),
		"PINGREQ",
	)

	mt <- prometheus.MustNewConstMetric(
		prometheus.NewDesc("maxmq_mqtt_packets_sent",
			"Number of MQTT packets sent", []string{"type"}, nil),
		prometheus.CounterValue,
		float64(atomic.LoadUint64(&pm.packetSent.pingresp)),
		"PINGRESP",
	)

	mt <- prometheus.MustNewConstMetric(
		prometheus.NewDesc("maxmq_mqtt_packets_received",
			"Number of MQTT packets received", []string{"type"}, nil),
		prometheus.CounterValue,
		float64(atomic.LoadUint64(&pm.packetReceived.disconnect)),
		"DISCONNECT",
	)

	mt <- prometheus.MustNewConstMetric(
		prometheus.NewDesc("maxmq_mqtt_packets_sent",
			"Number of MQTT packets sent", []string{"type"}, nil),
		prometheus.CounterValue,
		float64(atomic.LoadUint64(&pm.packetSent.disconnect)),
		"DISCONNECT",
	)
}

func (pm packetMetrics) collectBytes(mt chan<- prometheus.Metric) {
	mt <- prometheus.MustNewConstMetric(
		prometheus.NewDesc("maxmq_mqtt_bytes_received_total",
			"Total number of bytes received through MQTT", nil, nil),
		prometheus.CounterValue,
		float64(atomic.LoadUint64(&pm.bytesReceived.total)),
	)

	mt <- prometheus.MustNewConstMetric(
		prometheus.NewDesc("maxmq_mqtt_bytes_sent_total",
			"Total number of bytes sent through MQTT", nil, nil),
		prometheus.CounterValue,
		float64(atomic.LoadUint64(&pm.bytesSent.total)),
	)

	mt <- prometheus.MustNewConstMetric(
		prometheus.NewDesc("maxmq_mqtt_bytes_received",
			"Number of MQTT bytes received", []string{"type"}, nil),
		prometheus.CounterValue,
		float64(atomic.LoadUint64(&pm.bytesReceived.connect)),
		"CONNECT",
	)

	mt <- prometheus.MustNewConstMetric(
		prometheus.NewDesc("maxmq_mqtt_bytes_sent",
			"Number of MQTT bytes sent", []string{"type"}, nil),
		prometheus.CounterValue,
		float64(atomic.LoadUint64(&pm.bytesSent.connack)),
		"CONNACK",
	)

	mt <- prometheus.MustNewConstMetric(
		prometheus.NewDesc("maxmq_mqtt_bytes_received",
			"Number of MQTT bytes received", []string{"type"}, nil),
		prometheus.CounterValue,
		float64(atomic.LoadUint64(&pm.bytesReceived.pingreq)),
		"PINGREQ",
	)

	mt <- prometheus.MustNewConstMetric(
		prometheus.NewDesc("maxmq_mqtt_bytes_sent",
			"Number of MQTT bytes sent", []string{"type"}, nil),
		prometheus.CounterValue,
		float64(atomic.LoadUint64(&pm.bytesSent.pingresp)),
		"PINGRESP",
	)

	mt <- prometheus.MustNewConstMetric(
		prometheus.NewDesc("maxmq_mqtt_bytes_received",
			"Number of MQTT bytes received", []string{"type"}, nil),
		prometheus.CounterValue,
		float64(atomic.LoadUint64(&pm.bytesReceived.disconnect)),
		"DISCONNECT",
	)

	mt <- prometheus.MustNewConstMetric(
		prometheus.NewDesc("maxmq_mqtt_bytes_sent",
			"Number of MQTT bytes sent", []string{"type"}, nil),
		prometheus.CounterValue,
		float64(atomic.LoadUint64(&pm.bytesSent.disconnect)),
		"DISCONNECT",
	)
}

func (pm *packetMetrics) received(pkt packet.Packet) {
	pktSize := uint64(pkt.Size())

	switch pkt.Type() {
	case packet.CONNECT:
		atomic.AddUint64(&pm.packetReceived.connect, 1)
		atomic.AddUint64(&pm.bytesReceived.connect, pktSize)
	case packet.CONNACK:
		atomic.AddUint64(&pm.packetReceived.connack, 1)
		atomic.AddUint64(&pm.bytesReceived.connack, pktSize)
	case packet.PINGREQ:
		atomic.AddUint64(&pm.packetReceived.pingreq, 1)
		atomic.AddUint64(&pm.bytesReceived.pingreq, pktSize)
	case packet.PINGRESP:
		atomic.AddUint64(&pm.packetReceived.pingresp, 1)
		atomic.AddUint64(&pm.bytesReceived.pingresp, pktSize)
	case packet.DISCONNECT:
		atomic.AddUint64(&pm.packetReceived.disconnect, 1)
		atomic.AddUint64(&pm.bytesReceived.disconnect, pktSize)
	}

	atomic.AddUint64(&pm.packetReceived.total, 1)
	atomic.AddUint64(&pm.bytesReceived.total, pktSize)
}

func (pm *packetMetrics) sent(pkt packet.Packet) {
	switch pkt.Type() {
	case packet.CONNECT:
		atomic.AddUint64(&pm.packetSent.connect, 1)
	case packet.CONNACK:
		atomic.AddUint64(&pm.packetSent.connack, 1)
	case packet.PINGREQ:
		atomic.AddUint64(&pm.packetSent.pingreq, 1)
	case packet.PINGRESP:
		atomic.AddUint64(&pm.packetSent.pingresp, 1)
	case packet.DISCONNECT:
		atomic.AddUint64(&pm.packetSent.disconnect, 1)
	}

	atomic.AddUint64(&pm.packetSent.total, 1)
}
