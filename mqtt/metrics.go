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
	"github.com/gsalomao/maxmq/logger"
	"github.com/gsalomao/maxmq/mqtt/packet"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/multierr"
)

type metrics struct {
	packets     *packetsMetrics
	bytes       *bytesMetrics
	connections *connectionsMetrics
	log         *logger.Logger
}

type packetsMetrics struct {
	received      *prometheus.CounterVec
	sent          *prometheus.CounterVec
	receivedTotal prometheus.Counter
	sentTotal     prometheus.Counter
}

type bytesMetrics struct {
	received      *prometheus.CounterVec
	sent          *prometheus.CounterVec
	receivedTotal prometheus.Counter
	sentTotal     prometheus.Counter
}

type connectionsMetrics struct {
	connectTotal    prometheus.Counter
	disconnectTotal prometheus.Counter
	active          prometheus.Gauge
}

func newMetrics(log *logger.Logger) *metrics {
	mt := &metrics{log: log}

	mt.packets = newPacketsMetrics("maxmq", "mqtt")
	mt.bytes = newBytesMetrics("maxmq", "mqtt")
	mt.connections = newConnectionsMetrics("maxmq", "mqtt")

	if err := mt.registerPacketsMetrics(); err != nil {
		log.Warn().Msg("MQTT Failed to register metrics: " + err.Error())
	}

	err := multierr.Combine(mt.registerPacketsMetrics())
	err = multierr.Combine(err, mt.registerBytesMetrics())
	err = multierr.Combine(err, mt.registerConnectionsMetrics())
	if err != nil {
		log.Warn().Msg("MQTT Failed to register metrics: " + err.Error())
	}

	return mt
}

func newPacketsMetrics(namespace, subsystem string) *packetsMetrics {
	pm := &packetsMetrics{}

	pm.received = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "packets_received",
			Help:      "Number of packets received",
		}, []string{"type"},
	)

	pm.receivedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "packets_received_total",
			Help:      "Total number of packets received",
		},
	)

	pm.sent = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "packets_sent",
			Help:      "Number of packets sent",
		}, []string{"type"},
	)

	pm.sentTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "packets_sent_total",
			Help:      "Total number of packets sent",
		},
	)

	return pm
}

func newBytesMetrics(namespace, subsystem string) *bytesMetrics {
	bm := &bytesMetrics{}

	bm.received = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "bytes_received",
			Help:      "Number of bytes received",
		}, []string{"type"},
	)

	bm.receivedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "bytes_received_total",
			Help:      "Total number of bytes received",
		},
	)

	bm.sent = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "bytes_sent",
			Help:      "Number of bytes sent",
		}, []string{"type"},
	)

	bm.sentTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "bytes_sent_total",
			Help:      "Total number of bytes sent",
		},
	)

	return bm
}

func newConnectionsMetrics(namespace, subsystem string) *connectionsMetrics {
	cm := &connectionsMetrics{}

	cm.connectTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "connected_total",
			Help:      "Total number of connections",
		},
	)

	cm.disconnectTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "disconnected_total",
			Help:      "Total number of disconnections",
		},
	)

	cm.active = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "active_connections",
			Help:      "Number of of active connections",
		},
	)

	return cm
}

func (m *metrics) registerPacketsMetrics() error {
	err := multierr.Combine(prometheus.Register(m.packets.received))
	err = multierr.Combine(err, prometheus.Register(m.packets.receivedTotal))
	err = multierr.Combine(err, prometheus.Register(m.packets.sent))
	err = multierr.Combine(err, prometheus.Register(m.packets.sentTotal))

	return err
}

func (m *metrics) registerBytesMetrics() error {
	err := multierr.Combine(prometheus.Register(m.bytes.received))
	err = multierr.Combine(err, prometheus.Register(m.bytes.receivedTotal))
	err = multierr.Combine(err, prometheus.Register(m.bytes.sent))
	err = multierr.Combine(err, prometheus.Register(m.bytes.sentTotal))

	return err
}

func (m *metrics) registerConnectionsMetrics() error {
	err := multierr.Combine(prometheus.Register(m.connections.connectTotal))
	err = multierr.Combine(err,
		prometheus.Register(m.connections.disconnectTotal))
	err = multierr.Combine(err, prometheus.Register(m.connections.active))

	return err
}

func (m *metrics) packetReceived(pkt packet.Packet) {
	lb := prometheus.Labels{"type": pkt.Type().String()}
	m.packets.received.With(lb).Inc()
	m.packets.receivedTotal.Inc()

	sz := float64(pkt.Size())
	m.bytes.received.With(lb).Add(sz)
	m.bytes.receivedTotal.Add(sz)
}

func (m *metrics) packetSent(pkt packet.Packet) {
	lb := prometheus.Labels{"type": pkt.Type().String()}
	m.packets.sent.With(lb).Inc()
	m.packets.sentTotal.Inc()

	sz := float64(pkt.Size())
	m.bytes.sent.With(lb).Add(sz)
	m.bytes.sentTotal.Add(sz)
}

func (m *metrics) connected() {
	m.connections.connectTotal.Inc()
	m.connections.active.Inc()
}

func (m *metrics) disconnected() {
	m.connections.disconnectTotal.Inc()
	m.connections.active.Dec()
}
