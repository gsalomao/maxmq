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
	"fmt"
	"time"

	"go.uber.org/multierr"
)

// ConnAck represents the CONNACK Packet from MQTT specifications.
type ConnAck struct {
	// Properties represents the CONNACK properties (MQTT V5.0 only).
	Properties *Properties

	// timestamp represents the timestamp which the packet was created.
	timestamp time.Time

	// size represents the number of bytes in the packet.
	size int

	// Version represents the MQTT version.
	Version MQTTVersion

	// ReasonCode represents the reason code based on the MQTT specifications.
	ReasonCode ReasonCode

	// SessionPresent indicates if there is already a session associated with
	// the Client ID.
	SessionPresent bool
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
	}
}

// Write encodes the packet into bytes and writes it into the io.Writer.
func (pkt *ConnAck) Write(w *bufio.Writer) error {
	buf := &bytes.Buffer{}

	if pkt.Version == MQTT50 {
		err := writeProperties(buf, pkt.Properties, CONNACK)
		if err != nil {
			return fmt.Errorf("failed to write properties: %w", err)
		}
	}

	pktLen := buf.Len() + 2 // +2 for ack flags and reason code
	var ackFlag byte
	if pkt.SessionPresent && pkt.Version != MQTT31 {
		ackFlag = 1
	}

	err := multierr.Combine(
		w.WriteByte(byte(CONNACK)<<packetTypeBit),
		writeVarInteger(w, pktLen),
		w.WriteByte(ackFlag),
		w.WriteByte(byte(pkt.ReasonCode)),
	)
	_, errBuff := buf.WriteTo(w)
	err = multierr.Combine(err, errBuff)
	if err != nil {
		return fmt.Errorf("failed to send packet: %w", err)
	}

	pkt.timestamp = time.Now()
	pkt.size = pktLen + 2 // +2 for paket type and length

	return err
}

// Read reads the packet bytes from bytes.Buffer and decodes them into the
// packet.
// It is not supported by the CONNACK Packet in this broker.
func (pkt *ConnAck) Read(_ *bufio.Reader) error {
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

// Timestamp returns the timestamp of the moment the packet has been sent.
func (pkt *ConnAck) Timestamp() time.Time {
	return pkt.timestamp
}
