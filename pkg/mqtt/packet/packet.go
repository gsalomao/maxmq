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

// Type represents the packet type (e.g. CONNECT, CONNACK, etc.).
type Type byte

const (
	packetTypeBit   byte = 4
	controlByteMask byte = 0x0F
)

// Control packet type based on the MQTT specifications.
const (
	RESERVED Type = iota
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
	MQTT31 MQTTVersion = iota + 3
	MQTT311
	MQTT50
)

// Packet represents the MQTT packet.
type Packet interface {
	// Pack encodes the packet into bytes and writes it into the io.Writer.
	Pack(w io.Writer) error

	// Unpack reads the packet bytes from bytes.Buffer and decodes them into the
	// packet.
	Unpack(buf *bytes.Buffer) error

	// Type returns the packet type.
	Type() Type
}

type fixedHeader struct {
	packetType      Type
	controlFlags    byte
	remainingLength int
}

var packetTypeToFactory = map[Type]func(fixedHeader) (Packet, error){
	CONNECT: newPacketConnect,
}

func newPacket(fh fixedHeader) (Packet, error) {
	fn, ok := packetTypeToFactory[fh.packetType]
	if !ok {
		return nil, errors.New("invalid packet type: " + fh.packetType.String())
	}

	return fn(fh)
}

var packetTypeToString = map[Type]string{
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
func (pt Type) String() string {
	n, ok := packetTypeToString[pt]
	if !ok {
		return "UNKNOWN"
	}

	return n
}

var versionToString = map[MQTTVersion]string{
	MQTT31:  "3.1",
	MQTT311: "3.1.1",
	MQTT50:  "5.0",
}

// String returns the MQTTVersion in string format.
func (ver MQTTVersion) String() string {
	v := versionToString[ver]
	return v
}
