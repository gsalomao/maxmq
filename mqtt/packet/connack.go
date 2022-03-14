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
	"bytes"
	"errors"
	"time"
)

// ConnAck represents the CONNACK Packet from MQTT specifications.
type ConnAck struct {
	// Version represents the MQTT version.
	Version MQTTVersion

	// ReasonCode represents the reason code based on the MQTT specifications.
	ReasonCode ReasonCode

	// SessionPresent indicates if there is already a session associated with
	// the Client ID.
	SessionPresent bool

	// Properties represents the CONNACK properties (MQTT V5.0 only).
	Properties *Properties

	size      int
	timestamp time.Time
}

// NewConnAck creates a CONNACK Packet.
func NewConnAck(
	v MQTTVersion, c ReasonCode, sessionPresent bool, p *Properties,
) ConnAck {
	return ConnAck{
		Version:        v,
		ReasonCode:     c,
		SessionPresent: sessionPresent,
		Properties:     p,
		timestamp:      time.Now(),
	}
}

// Pack encodes the packet into bytes and writes it into the io.Writer.
func (pkt *ConnAck) Pack(w *bufio.Writer) error {
	varHeader := &bytes.Buffer{}
	var err error

	// Acknowledge Flags
	if pkt.SessionPresent && pkt.Version != MQTT31 {
		_ = varHeader.WriteByte(1)
	} else {
		_ = varHeader.WriteByte(0)
	}

	_ = varHeader.WriteByte(byte(pkt.ReasonCode))

	if pkt.Version == MQTT50 {
		err = writeProperties(varHeader, pkt.Properties, CONNACK)
		if err != nil {
			return err
		}
	}

	_ = w.WriteByte(byte(CONNACK) << packetTypeBit)
	_ = encodeVarInteger(w, varHeader.Len())
	n, err := varHeader.WriteTo(w)
	pkt.size = 2 + int(n)

	return err
}

// Unpack reads the packet bytes from bytes.Buffer and decodes them into the
// packet.
// It is not supported by the CONNACK Packet in this broker.
func (pkt *ConnAck) Unpack(_ *bufio.Reader) error {
	return errors.New("unsupported (CONNACK)")
}

// Type returns the packet type.
func (pkt *ConnAck) Type() Type {
	return CONNACK
}

// Size returns the packet size in bytes.
func (pkt *ConnAck) Size() int {
	return pkt.size
}

// Timestamp returns the timestamp which the packet was created.
func (pkt *ConnAck) Timestamp() time.Time {
	return pkt.timestamp
}
