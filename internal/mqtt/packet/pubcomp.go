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
	"io"
	"time"

	"go.uber.org/multierr"
)

// PubComp represents the PUBCOMP Packet from MQTT specifications.
type PubComp struct {
	// Properties represents the PUBCOMP properties (MQTT V5.0 only).
	Properties *Properties

	// PacketID represents the packet identifier.
	PacketID ID

	// ReasonCode can contain the PUBCOMP Reason Code (MQTT V5.0 only).
	ReasonCode ReasonCode

	// Version represents the MQTT version.
	Version MQTTVersion

	// Unexported fields
	timestamp    time.Time
	size         int
	remainLength int
}

func newPacketPubComp(opts options) (Packet, error) {
	if opts.packetType != PUBCOMP {
		return nil, errors.New("packet type is not PUBCOMP")
	}

	if opts.version < MQTT31 || opts.version > MQTT50 {
		return nil, errors.New("invalid version (PUBCOMP)")
	}

	if opts.controlFlags != 0 {
		return nil, errors.New("invalid Control Flags (PUBCOMP)")
	}

	if opts.version < MQTT50 && opts.remainingLength != 2 {
		return nil, errors.New("invalid remaining length (PUBCOMP)")
	} else if opts.version == MQTT50 && opts.remainingLength < 2 {
		return nil, errors.New("invalid remaining length (PUBCOMP)")
	}

	return &PubComp{
		Version:      opts.version,
		size:         opts.fixedHeaderLength + opts.remainingLength,
		remainLength: opts.remainingLength,
		timestamp:    opts.timestamp,
	}, nil
}

// NewPubComp creates a PUBCOMP Packet.
func NewPubComp(id ID, v MQTTVersion, c ReasonCode, p *Properties) PubComp {
	return PubComp{
		PacketID:   id,
		Version:    v,
		ReasonCode: c,
		Properties: p,
	}
}

// Write encodes the packet into bytes and writes it into the io.Writer.
func (pkt *PubComp) Write(w *bufio.Writer) error {
	buf := &bytes.Buffer{}

	if pkt.Version == MQTT50 {
		if !pkt.isValidReasonCode() {
			return fmt.Errorf("invalid reason code: %v", pkt.ReasonCode)
		}

		if pkt.ReasonCode != ReasonCodeV5Success || pkt.Properties != nil {
			err := buf.WriteByte(byte(pkt.ReasonCode))
			err = multierr.Combine(err,
				writeProperties(buf, pkt.Properties, PUBCOMP))
			if err != nil {
				return fmt.Errorf("failed to write properties: %w", err)
			}
		}
	}

	pktLen := buf.Len() + 2 // +2 for packet ID

	err := multierr.Combine(
		w.WriteByte(byte(PUBCOMP)<<packetTypeBit),
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

// Read reads the packet bytes from bufio.Reader and decodes them into the
// packet.
func (pkt *PubComp) Read(r *bufio.Reader) error {
	msg := make([]byte, pkt.remainLength)
	if _, err := io.ReadFull(r, msg); err != nil {
		return fmt.Errorf("failed to read remaining bytes: %w", err)
	}
	buf := bytes.NewBuffer(msg)

	id, _ := readUint[uint16](buf)
	pkt.PacketID = ID(id)

	if pkt.Version == MQTT50 && buf.Len() > 0 {
		var code byte
		code, _ = buf.ReadByte()
		pkt.ReasonCode = ReasonCode(code)

		if !pkt.isValidReasonCode() {
			return fmt.Errorf("invalid reason code: %v", pkt.ReasonCode)
		}

		var err error
		pkt.Properties, err = readProperties(buf, PUBCOMP)
		if err != nil {
			return fmt.Errorf("failed to read properties: %w", err)
		}
	}

	return nil
}

// Type returns the packet type.
func (pkt *PubComp) Type() Type {
	return PUBCOMP
}

// Size returns the packet size in bytes.
func (pkt *PubComp) Size() int {
	return pkt.size
}

// Timestamp returns the timestamp of the moment the packet has been sent.
func (pkt *PubComp) Timestamp() time.Time {
	return pkt.timestamp
}

func (pkt *PubComp) isValidReasonCode() bool {
	validPubRecReasonCodes := []ReasonCode{ReasonCodeV5Success,
		ReasonCodeV5PacketIDNotFound}

	for _, code := range validPubRecReasonCodes {
		if pkt.ReasonCode == code {
			return true
		}
	}
	return false
}
