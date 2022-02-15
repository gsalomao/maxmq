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

// Configuration holds the MQTT runtime configuration.
type Configuration struct {
	// TCP address (<IP>:<port>) that the MQTT will bind to.
	TCPAddress string

	// The amount of time, in seconds, the MQTT waits for the CONNECT Packet.
	ConnectTimeout int

	// The size, in bytes, of the MQTT receiver and transmitter buffers.
	BufferSize int

	// The maximum packet size, in bytes, allowed.
	MaxPacketSize int

	// The maximum Keep Alive value, in seconds, allowed by the broker.
	MaxKeepAlive int

	// The maximum QoS for PUBLISH Packets accepted by the broker.
	MaximumQoS int

	// Indicate whether the broker allows retained messages or not.
	RetainAvailable bool

	// This property can be used to provide additional information to the Client
	// including diagnostic information.
	UserProperties map[string]string
}
