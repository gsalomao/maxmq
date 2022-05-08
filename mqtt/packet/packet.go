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

package packet

import (
	"bufio"
	"errors"
	"time"
)

// Type represents the packet type (e.g. CONNECT, CONNACK, etc.).
type Type byte

const (
	packetTypeBit        byte = 4
	controlByteFlagsMask byte = 0x0F
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
	// Pack encodes the packet into bytes and writes it into bufio.Writer.
	Pack(w *bufio.Writer) error

	// Unpack reads the packet bytes from bufio.Reader and decodes them into the
	// packet.
	Unpack(r *bufio.Reader) error

	// Type returns the packet type.
	Type() Type

	// Size returns the packet size in bytes.
	Size() int

	// Timestamp returns the timestamp which the packet was created.
	Timestamp() time.Time
}

type options struct {
	timestamp         time.Time
	fixedHeaderLength int
	remainingLength   int
	packetType        Type
	version           MQTTVersion
	controlFlags      byte
}

var packetTypeToFactory = map[Type]func(options) (Packet, error){
	CONNECT:    newPacketConnect,
	PINGREQ:    newPacketPingReq,
	DISCONNECT: newPacketDisconnect,
}

func newPacket(opts options) (Packet, error) {
	fn, ok := packetTypeToFactory[opts.packetType]
	if !ok {
		return nil, errors.New("invalid packet type: " +
			opts.packetType.String())
	}

	return fn(opts)
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
