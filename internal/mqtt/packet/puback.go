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

package packet

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"time"

	"go.uber.org/multierr"
)

// PubAck represents the PUBACK Packet from MQTT specifications.
type PubAck struct {
	// Properties represents the PUBACK properties (MQTT V5.0 only).
	Properties *Properties

	// ReasonCode can contain the PUBACK Reason Code (MQTT V5.0 only).
	ReasonCode ReasonCode

	// PacketID represents the packet identifier.
	PacketID ID

	// Version represents the MQTT version.
	Version Version

	// Unexported fields
	timestamp    time.Time
	size         int
	remainLength int
}

func newPacketPubAck(opts options) (Packet, error) {
	if opts.packetType != PUBACK {
		return nil, errors.New("packet type is not PUBACK")
	}

	if opts.controlFlags != 0 {
		return nil, errors.New("invalid Control Flags (PUBACK)")
	}

	if opts.version < MQTT31 || opts.version > MQTT50 {
		return nil, errors.New("invalid version (PUBACK)")
	}

	if opts.version < MQTT50 && opts.remainingLength != 2 {
		return nil, errors.New("invalid remaining length (PUBACK)")
	} else if opts.version == MQTT50 && opts.remainingLength < 2 {
		return nil, errors.New("invalid remaining length (PUBACK)")
	}

	return &PubAck{
		Version:      opts.version,
		size:         opts.fixedHeaderLength + opts.remainingLength,
		remainLength: opts.remainingLength,
		timestamp:    opts.timestamp,
	}, nil
}

// NewPubAck creates a PUBACK Packet.
func NewPubAck(id ID, v Version, c ReasonCode, p *Properties) PubAck {
	return PubAck{
		PacketID:   id,
		Version:    v,
		ReasonCode: c,
		Properties: p,
	}
}

// Write encodes the packet into bytes and writes it into the io.Writer.
func (pkt *PubAck) Write(w *bufio.Writer) error {
	buf := &bytes.Buffer{}

	if pkt.Version == MQTT50 &&
		(pkt.ReasonCode != ReasonCodeV5Success || pkt.Properties != nil) {

		err := buf.WriteByte(byte(pkt.ReasonCode))
		err = multierr.Combine(err,
			writeProperties(buf, pkt.Properties, PUBACK))
		if err != nil {
			return fmt.Errorf("failed to write properties: %w", err)
		}
	}

	pktLen := buf.Len() + 2 // +2 for packet ID

	err := multierr.Combine(
		w.WriteByte(byte(PUBACK)<<packetTypeBit),
		writeVarInteger(w, pktLen),
		w.WriteByte(byte(pkt.PacketID>>8)),
		w.WriteByte(byte(pkt.PacketID)),
	)
	_, errBuf := buf.WriteTo(w)
	err = multierr.Combine(err, errBuf)
	if err != nil {
		return fmt.Errorf("failed to send packet: %w", err)
	}

	pkt.timestamp = time.Now()
	pkt.size = pktLen + 2 // +2 for packet type and variable length

	return nil
}

// Read reads the packet bytes from bytes.Buffer and decodes them into the
// packet.
func (pkt *PubAck) Read(r *bufio.Reader) error {
	msg := make([]byte, pkt.remainLength)
	if _, err := io.ReadFull(r, msg); err != nil {
		return fmt.Errorf("failed to read remaining bytes: %w",
			ErrV5MalformedPacket)
	}
	buf := bytes.NewBuffer(msg)

	id, _ := readUint[uint16](buf)
	pkt.PacketID = ID(id)

	if pkt.Version == MQTT50 {
		var code byte
		code, _ = buf.ReadByte()
		pkt.ReasonCode = ReasonCode(code)

		if pkt.ReasonCode != ReasonCodeV5Success || buf.Len() > 0 {
			var err error
			pkt.Properties, err = readProperties(buf, PUBACK)
			if err != nil {
				return fmt.Errorf("failed to read properties: %w", err)
			}
		}
	}

	return nil
}

// Type returns the packet type.
func (pkt *PubAck) Type() Type {
	return PUBACK
}

// Size returns the packet size in bytes.
func (pkt *PubAck) Size() int {
	return pkt.size
}

// Timestamp returns the timestamp of the moment the packet has been sent.
func (pkt *PubAck) Timestamp() time.Time {
	return pkt.timestamp
}
