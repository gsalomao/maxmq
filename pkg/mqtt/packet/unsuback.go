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

// UnsubAck represents the UNSUBACK Packet from MQTT specifications.
type UnsubAck struct {
	// Properties represents the UNSUBACK properties (MQTT V5.0 only).
	Properties *Properties

	// ReasonCodes contains the list of Reason Codes for each topic filter in
	// the UNSUBSCRIBE Packet (MQTT V5.0 only).
	ReasonCodes []ReasonCode

	// timestamp represents the timestamp which the packet was created.
	timestamp time.Time

	// size represents the number of bytes in the packet.
	size int

	// PacketID represents the packet identifier.
	PacketID ID

	// Version represents the MQTT version.
	Version MQTTVersion
}

// NewUnsubAck creates a UNSUBACK Packet.
func NewUnsubAck(id ID, v MQTTVersion, c []ReasonCode, p *Properties) UnsubAck {
	return UnsubAck{
		PacketID:    id,
		Version:     v,
		ReasonCodes: c,
		Properties:  p,
	}
}

// Write encodes the packet into bytes and writes it into the io.Writer.
func (pkt *UnsubAck) Write(w *bufio.Writer) error {
	buf := &bytes.Buffer{}

	if pkt.Version == MQTT50 {
		err := writeProperties(buf, pkt.Properties, UNSUBACK)
		if err != nil {
			return fmt.Errorf("failed to write properties: %w", err)
		}
	}

	pktLen := buf.Len() + len(pkt.ReasonCodes) + 2 // +2 for packet ID

	err := multierr.Combine(
		w.WriteByte(byte(UNSUBACK)<<packetTypeBit),
		writeVarInteger(w, pktLen),
		w.WriteByte(byte(pkt.PacketID>>8)),
		w.WriteByte(byte(pkt.PacketID)),
	)
	_, errBuf := buf.WriteTo(w)
	err = multierr.Combine(err, errBuf)
	for _, code := range pkt.ReasonCodes {
		err = multierr.Combine(w.WriteByte(byte(code)))
		if err != nil {
			break
		}
	}
	if err != nil {
		return fmt.Errorf("failed to send packet: %w", err)
	}

	pkt.timestamp = time.Now()
	pkt.size = pktLen + 2 // +2 for packet type and variable length

	return nil
}

// Read reads the packet bytes from bytes.Buffer and decodes them into the
// packet.
// It is not supported by the UNSUBACK Packet in this broker.
func (pkt *UnsubAck) Read(_ *bufio.Reader) error {
	return errors.New("unsupported (UNSUBACK)")
}

// Type returns the packet type.
func (pkt *UnsubAck) Type() Type {
	return UNSUBACK
}

// Size returns the packet size in bytes.
func (pkt *UnsubAck) Size() int {
	return pkt.size
}

// Timestamp returns the timestamp of the moment the packet has been sent.
func (pkt *UnsubAck) Timestamp() time.Time {
	return pkt.timestamp
}
