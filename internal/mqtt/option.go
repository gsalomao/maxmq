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

// Options contains the Server options to be used.
type Options struct {
	// Config contains the server configuration.
	Config *Config
}

// Option is the function called by NewServer factory method to set the Options.
type Option func(opts *Options)

// WithConfig sets the configuration into the Options.
func WithConfig(c *Config) Option {
	return func(opts *Options) { opts.Config = c }
}

// Config contains the Server configuration.
type Config struct {
	// The size, in bytes, of the MQTT receiver and transmitter buffers.
	BufferSize int

	// The maximum packet size, in bytes, allowed.
	MaxPacketSize int

	// The maximum session expire interval, in seconds, allowed by the server.
	MaxSessionExpiryInterval int

	// The maximum message expiry interval, in seconds.
	MaxMessageExpiryInterval int

	// SysTopicUpdateInterval specifies, in seconds, the interval between $SYS topic updates.
	SysTopicUpdateInterval int

	// The maximum number of outbound message for a client.
	MaxOutboundMessages int

	// The maximum number of topic aliases that an MQTT V5 client is allowed to create.
	MaxTopicAlias int

	// The maximum number of QoS 1 and QoS 2 publications that it is willing to process concurrently.
	ReceiveMaximum int

	// The maximum QoS for PUBLISH Packets accepted by the server.
	MaximumQoS byte

	// The minimum supported MQTT version.
	MinProtocolVersion byte

	// Indicate whether the server allows retained messages or not.
	RetainAvailable bool

	// Indicate whether the server allows wildcard subscription or not.
	WildcardSubscriptionAvailable bool

	// Indicate whether the server allows subscription identifier or not.
	SubscriptionIDAvailable bool

	// Indicate whether the server allows shared subscription or not.
	SharedSubscriptionAvailable bool

	// Indicate whether the server exports metrics or not.
	MetricsEnabled bool
}

// NewDefaultConfig creates a default Config.
func NewDefaultConfig() *Config {
	return &Config{
		BufferSize:                    2048,
		MaxPacketSize:                 0,
		MaxSessionExpiryInterval:      7200,
		MaxMessageExpiryInterval:      86400,
		SysTopicUpdateInterval:        1,
		MaxOutboundMessages:           8192,
		MaxTopicAlias:                 65535,
		ReceiveMaximum:                1024,
		MaximumQoS:                    2,
		MinProtocolVersion:            3,
		RetainAvailable:               true,
		WildcardSubscriptionAvailable: true,
		SubscriptionIDAvailable:       true,
		SharedSubscriptionAvailable:   true,
		MetricsEnabled:                true,
	}
}
