// Copyright 2022 The MaxMQ Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mqtt

import (
	"fmt"
	"sync"
	"time"

	"github.com/gsalomao/maxmq/logger"
	"github.com/gsalomao/maxmq/mqtt/packet"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/multierr"
)

type metrics struct {
	cache       labelCaches
	packets     *packetsMetrics
	connections *connectionsMetrics
	latencies   *latenciesMetrics
	log         *logger.Logger
}

type labelCaches struct {
	mu         sync.RWMutex
	packetType map[packet.Type]prometheus.Labels
	code       map[int]prometheus.Labels
}

type packetsMetrics struct {
	receivedTotal *prometheus.CounterVec
	receivedBytes *prometheus.CounterVec
	sentTotal     *prometheus.CounterVec
	sentBytes     *prometheus.CounterVec
}

type connectionsMetrics struct {
	connectTotal      prometheus.Counter
	disconnectTotal   prometheus.Counter
	activeConnections prometheus.Gauge
}

type latenciesMetrics struct {
	connectSeconds     *prometheus.HistogramVec
	pingSeconds        prometheus.Histogram
	subscribeSeconds   prometheus.Histogram
	unsubscribeSeconds prometheus.Histogram
	disconnectSeconds  prometheus.Histogram
}

func newMetrics(enabled bool, log *logger.Logger) *metrics {
	mt := &metrics{log: log}

	if enabled {
		mt.cache = newCaches()
		mt.packets = newPacketsMetrics()
		mt.connections = newConnectionsMetrics()
		mt.latencies = newLatenciesMetrics()

		err := mt.registerPacketsMetrics()
		err = multierr.Combine(err, mt.registerConnectionsMetrics())
		err = multierr.Combine(err, mt.registerLatenciesMetrics())
		if err != nil {
			log.Error().Msg("MQTT Failed to register metrics: " + err.Error())
		}
	}

	return mt
}

func newCaches() labelCaches {
	return labelCaches{
		packetType: map[packet.Type]prometheus.Labels{},
		code:       map[int]prometheus.Labels{},
	}
}

func newPacketsMetrics() *packetsMetrics {
	pm := &packetsMetrics{}

	pm.receivedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "maxmq",
			Subsystem: "mqtt",
			Name:      "packets_received_total",
			Help:      "Number of packets received",
		}, []string{"type"},
	)

	pm.receivedBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "maxmq",
			Subsystem: "mqtt",
			Name:      "packets_received_bytes",
			Help:      "Number of bytes received",
		}, []string{"type"},
	)

	pm.sentTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "maxmq",
			Subsystem: "mqtt",
			Name:      "packets_sent_total",
			Help:      "Number of packets sent",
		}, []string{"type"},
	)

	pm.sentBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "maxmq",
			Subsystem: "mqtt",
			Name:      "packets_sent_bytes",
			Help:      "Number of bytes sent",
		}, []string{"type"},
	)

	return pm
}

func newConnectionsMetrics() *connectionsMetrics {
	cm := &connectionsMetrics{}

	cm.connectTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "maxmq",
			Subsystem: "mqtt",
			Name:      "connected_total",
			Help:      "Number of connections",
		},
	)

	cm.disconnectTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "maxmq",
			Subsystem: "mqtt",
			Name:      "disconnected_total",
			Help:      "Number of disconnections",
		},
	)

	cm.activeConnections = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "maxmq",
			Subsystem: "mqtt",
			Name:      "active_connections",
			Help:      "Number of active MQTT connections",
		},
	)

	return cm
}

func newLatenciesMetrics() *latenciesMetrics {
	latencies := &latenciesMetrics{}
	buckets := []float64{
		0.00010, 0.00025, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1,
	}

	latencies.connectSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "maxmq",
			Subsystem: "mqtt",
			Name:      "connect_latency_seconds",
			Help: "Duration in seconds from the time the CONNECT packet is " +
				"received until the time the CONNACK packet is sent",
			Buckets: buckets,
		}, []string{"code"},
	)

	latencies.pingSeconds = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "maxmq",
			Subsystem: "mqtt",
			Name:      "ping_latency_seconds",
			Help: "Duration in seconds from the time the PINGREQ packet is " +
				"received until the time the PINGRESP packet is sent",
			Buckets: buckets,
		},
	)

	latencies.subscribeSeconds = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "maxmq",
			Subsystem: "mqtt",
			Name:      "subscribe_latency_seconds",
			Help: "Duration in seconds from the time the SUBSCRIBE packet " +
				"is received until the time the SUBACK packet is sent",
			Buckets: buckets,
		},
	)

	latencies.subscribeSeconds = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "maxmq",
			Subsystem: "mqtt",
			Name:      "subscribe_latency_seconds",
			Help: "Duration in seconds from the time the SUBSCRIBE packet " +
				"is received until the time the SUBACK packet is sent",
			Buckets: buckets,
		},
	)

	latencies.unsubscribeSeconds = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "maxmq",
			Subsystem: "mqtt",
			Name:      "unsubscribe_latency_seconds",
			Help: "Duration in seconds from the time the UNSUBSCRIBE packet " +
				"is received until the time the UNSUBACK packet is sent",
			Buckets: buckets,
		},
	)

	latencies.disconnectSeconds = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "maxmq",
			Subsystem: "mqtt",
			Name:      "disconnect_latency_seconds",
			Help: "Duration in seconds from the time the DISCONNECT packet " +
				"is received until the time the connection is closed",
			Buckets: buckets,
		},
	)

	return latencies
}

func (m *metrics) registerPacketsMetrics() error {
	err := prometheus.Register(m.packets.receivedTotal)
	err = multierr.Combine(err, prometheus.Register(m.packets.receivedBytes))
	err = multierr.Combine(err, prometheus.Register(m.packets.sentTotal))
	err = multierr.Combine(err, prometheus.Register(m.packets.sentBytes))

	return err
}

func (m *metrics) registerConnectionsMetrics() error {
	err := prometheus.Register(m.connections.connectTotal)
	err = multierr.Combine(err,
		prometheus.Register(m.connections.disconnectTotal))
	err = multierr.Combine(err,
		prometheus.Register(m.connections.activeConnections))

	return err
}

func (m *metrics) registerLatenciesMetrics() error {
	err := prometheus.Register(m.latencies.connectSeconds)
	err = multierr.Combine(err, prometheus.Register(m.latencies.pingSeconds))
	err = multierr.Combine(err,
		prometheus.Register(m.latencies.disconnectSeconds))

	return err
}

func (m *metrics) recordPacketReceived(pkt packet.Packet) {
	if m.packets == nil {
		return
	}

	m.cache.mu.RLock()
	lb, ok := m.cache.packetType[pkt.Type()]
	m.cache.mu.RUnlock()

	if !ok {
		lb = prometheus.Labels{"type": pkt.Type().String()}

		m.cache.mu.Lock()
		m.cache.packetType[pkt.Type()] = lb
		m.cache.mu.Unlock()
	}

	sz := float64(pkt.Size())
	m.packets.receivedTotal.With(lb).Inc()
	m.packets.receivedBytes.With(lb).Add(sz)
}

func (m *metrics) recordPacketSent(pkt packet.Packet) {
	if m.packets == nil {
		return
	}

	m.cache.mu.RLock()
	lb, ok := m.cache.packetType[pkt.Type()]
	m.cache.mu.RUnlock()

	if !ok {
		lb = prometheus.Labels{"type": pkt.Type().String()}

		m.cache.mu.Lock()
		m.cache.packetType[pkt.Type()] = lb
		m.cache.mu.Unlock()
	}

	sz := float64(pkt.Size())
	m.packets.sentTotal.With(lb).Inc()
	m.packets.sentBytes.With(lb).Add(sz)
}

func (m *metrics) recordConnection() {
	if m.connections == nil {
		return
	}

	m.connections.connectTotal.Inc()
	m.connections.activeConnections.Inc()
}

func (m *metrics) recordDisconnection() {
	if m.connections == nil {
		return
	}

	m.connections.disconnectTotal.Inc()
	m.connections.activeConnections.Dec()
}

func (m *metrics) recordConnectLatency(d time.Duration, code int) {
	if m.latencies == nil {
		return
	}

	m.cache.mu.RLock()
	lb, ok := m.cache.code[code]
	m.cache.mu.RUnlock()

	if !ok {
		lb = prometheus.Labels{"code": fmt.Sprint(code)}

		m.cache.mu.Lock()
		m.cache.code[code] = lb
		m.cache.mu.Unlock()
	}

	m.latencies.connectSeconds.With(lb).Observe(d.Seconds())
}

func (m *metrics) recordPingLatency(d time.Duration) {
	if m.latencies == nil {
		return
	}

	m.latencies.pingSeconds.Observe(d.Seconds())
}

func (m *metrics) recordSubscribeLatency(d time.Duration) {
	if m.latencies == nil {
		return
	}

	m.latencies.subscribeSeconds.Observe(d.Seconds())
}

func (m *metrics) recordUnsubscribeLatency(d time.Duration) {
	if m.latencies == nil {
		return
	}

	m.latencies.unsubscribeSeconds.Observe(d.Seconds())
}

func (m *metrics) recordDisconnectLatency(d time.Duration) {
	if m.latencies == nil {
		return
	}

	m.latencies.disconnectSeconds.Observe(d.Seconds())
}
