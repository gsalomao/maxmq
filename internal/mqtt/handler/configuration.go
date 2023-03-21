// Copyright 2022-2023 The MaxMQ Authors
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

package handler

// Configuration holds the MQTT runtime configuration.
type Configuration struct {
	// TCP address (<IP>:<port>) that the MQTT will bind to.
	TCPAddress string

	// The amount of time, in seconds, the MQTT waits for the CONNECT Packet.
	ConnectTimeout int

	// The size, in bytes, of the MQTT receiver and transmitter buffers.
	BufferSize int

	// The default MQTT protocol version (3 -> 3.1; 4 -> 3.1.1; 5 -> 5.0).
	DefaultVersion int

	// The maximum packet size, in bytes, allowed.
	MaxPacketSize int

	// The maximum Keep Alive value, in seconds, allowed by the server.
	MaxKeepAlive int

	// The maximum session Expire Interval, in seconds, allowed by the server.
	MaxSessionExpiryInterval uint32

	// The maximum number of QoS 1 or 2 messages that can be processed simultaneously.
	MaxInflightMessages int

	// The maximum number of retries to send an inflight message to a client.
	MaxInflightRetries int

	// The maximum QoS for PUBLISH Packets accepted by the server.
	MaximumQoS int

	// The maximum number of topic aliases that an MQTT V5 client is allowed to create.
	MaxTopicAlias int

	// Indicate whether the server allows retained messages or not.
	RetainAvailable bool

	// Indicate whether the server allows wildcard subscription or not.
	WildcardSubscriptionAvailable bool

	// Indicate whether the server allows subscription identifier or not.
	SubscriptionIDAvailable bool

	// Indicate whether the server allows shared subscription or not.
	SharedSubscriptionAvailable bool

	// The maximum length, in bytes, for client ID allowed by the server.
	MaxClientIDLen int

	// Indicate whether the server allows zero-length client identifier or not.
	AllowEmptyClientID bool

	// Prefix to be added to automatically generated client IDs.
	ClientIDPrefix []byte

	// This property can be used to provide additional information to the Client including diagnostic information.
	UserProperties map[string]string

	// Indicate whether the server exports metrics or not.
	MetricsEnabled bool
}

func newDefaultConfiguration() Configuration {
	return Configuration{
		ConnectTimeout:                5,
		MaxPacketSize:                 268435456,
		MaxKeepAlive:                  0,
		MaxSessionExpiryInterval:      0,
		MaxInflightMessages:           0,
		MaximumQoS:                    2,
		MaxTopicAlias:                 0,
		RetainAvailable:               true,
		WildcardSubscriptionAvailable: true,
		SubscriptionIDAvailable:       true,
		SharedSubscriptionAvailable:   true,
		MaxClientIDLen:                65535,
		AllowEmptyClientID:            true,
	}
}
