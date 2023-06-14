// Copyright 2023 The MaxMQ Authors
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
	"sync/atomic"

	"github.com/mochi-co/mqtt/v2/system"
	"github.com/prometheus/client_golang/prometheus"
)

type metric struct {
	metricType string
	name       string
	help       string
	value      *int64
}

func registerMetrics(i *system.Info) {
	metrics := []metric{
		{"counter", "bytes_received", "Total number of bytes received", &i.BytesReceived},
		{"counter", "bytes_sent", "Total number of bytes sent", &i.BytesSent},
		{"gauge", "clients_connected", "Number of currently connected clients", &i.ClientsConnected},
		{"gauge", "clients_disconnected", "Total number of persistent clients", &i.ClientsDisconnected},
		{"counter", "clients_maximum", "Maximum number of active clients that have been connected",
			&i.ClientsMaximum},
		{"gauge", "clients_total",
			"Total number of connected and disconnected clients with a persistent session currently connected and registered",
			&i.ClientsTotal},
		{"counter", "messages_received", "Total number of publish messages received",
			&i.MessagesReceived},
		{"counter", "messages_sent", "Total number of publish messages sent", &i.MessagesSent},
		{"counter", "messages_dropped", "Total number of publish messages dropped",
			&i.MessagesDropped},
		{"gauge", "retained", "Total number of retained messages active", &i.Retained},
		{"gauge", "inflight", "Number of messages currently inflight", &i.Inflight},
		{"counter", "inflight_dropped", "Number of inflight messages which were dropped", &i.InflightDropped},
		{"gauge", "subscriptions", "Total number of subscriptions active", &i.Subscriptions},
		{"counter", "packets_received", "Total number of packets received", &i.PacketsReceived},
		{"counter", "packets_sent", "Total number of packets sent", &i.PacketsSent},
	}

	for _, m := range metrics {
		m := m
		fn := func() float64 {
			return float64(atomic.LoadInt64(m.value))
		}

		switch m.metricType {
		case "counter":
			prometheus.MustRegister(
				prometheus.NewCounterFunc(
					prometheus.CounterOpts{
						Namespace: "maxmq",
						Subsystem: "mqtt",
						Name:      m.name,
						Help:      m.help,
					},
					fn,
				),
			)
		case "gauge":
			prometheus.MustRegister(
				prometheus.NewGaugeFunc(
					prometheus.GaugeOpts{
						Namespace: "maxmq",
						Subsystem: "mqtt",
						Name:      m.name,
						Help:      m.help,
					},
					fn,
				),
			)
		}
	}
}
