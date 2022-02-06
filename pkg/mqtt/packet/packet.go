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

package packet

import (
	"bytes"
	"errors"
	"io"
)

// FixedHeader represents the MQTT fixed header based on the MQTT
// specifications.
type FixedHeader struct {
	// PacketType represents the packet type (e.g. CONNECT, CONNACK, etc.).
	PacketType PacketType

	// ControlFlags represents the control flags.
	ControlFlags byte

	// RemainingLength is the number of bytes remaining within the current
	// packet.
	RemainingLength int
}

// PacketType represents the packet type (e.g. CONNECT, CONNACK, etc.).
type PacketType byte

// Control packet type based on the MQTT specifications.
const (
	RESERVED PacketType = iota
	CONNECT
	CONNACK
	PUBLISH
	PUBACK
	PUBREC
	PUBREL
	PUBCOMP
	SUBSCRIBE
	SUBACK
	UNSUBSCRIBE
	UNSUBACK
	PINGREQ
	PINGRESP
	DISCONNECT
)

// MQTTVersion represents the MQTT version.
type MQTTVersion byte

// MQTT version.
const (
	MQTT_V3_1 MQTTVersion = iota + 3
	MQTT_V3_1_1
	MQTT_V5_0
)

// Packet represents the MQTT packet.
type Packet interface {
	// Pack encodes the packet into bytes and writes it into the io.Writer.
	Pack(w io.Writer) error

	// Unpack reads the packet bytes from bytes.Buffer and decodes them into the
	// packet.
	Unpack(buf *bytes.Buffer) error

	// Type returns the packet type.
	Type() PacketType
}

var packetTypeToFactory = map[PacketType]func(FixedHeader) (Packet, error){
	CONNECT: newPacketConnect,
}

// NewPacket creates a Packet based on the packet type from the FixedHeader.
// It returns an error if the packet type is invalid.
func NewPacket(fh FixedHeader) (Packet, error) {
	fn, ok := packetTypeToFactory[fh.PacketType]
	if !ok {
		return nil, errors.New("invalid packet type")
	}

	return fn(fh)
}

var packetTypeToString = map[PacketType]string{
	CONNECT:     "CONNECT",
	CONNACK:     "CONNACK",
	PUBLISH:     "PUBLISH",
	PUBACK:      "PUBACK",
	PUBREC:      "PUBREC",
	PUBREL:      "PUBREL",
	PUBCOMP:     "PUBCOMP",
	SUBSCRIBE:   "SUBSCRIBE",
	SUBACK:      "SUBACK",
	UNSUBSCRIBE: "UNSUBSCRIBE",
	UNSUBACK:    "UNSUBACK",
	PINGREQ:     "PINGREQ",
	PINGRESP:    "PINGRESP",
	DISCONNECT:  "DISCONNECT",
}

// String returns the PacketType in string format.
func (pt PacketType) String() string {
	n, ok := packetTypeToString[pt]
	if !ok {
		return ""
	}

	return n
}

var versionToString = map[MQTTVersion]string{
	MQTT_V3_1:   "3.1",
	MQTT_V3_1_1: "3.1.1",
	MQTT_V5_0:   "5.0",
}

// String returns the MQTTVersion in string format.
func (ver MQTTVersion) String() string {
	v := versionToString[ver]
	return v
}
