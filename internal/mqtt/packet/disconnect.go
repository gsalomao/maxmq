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

// Disconnect represents the DISCONNECT Packet from MQTT specifications.
type Disconnect struct {
	// Properties represents the DISCONNECT properties (MQTT V5.0 only).
	Properties *Properties

	// Version represents the MQTT version.
	Version MQTTVersion

	// ReasonCode represents the reason code based on the MQTT specifications.
	ReasonCode ReasonCode

	// Unexported fields
	timestamp    time.Time
	size         int
	remainLength int
}

func newPacketDisconnect(opts options) (Packet, error) {
	if opts.packetType != DISCONNECT {
		return nil, errors.New("packet type is not DISCONNECT")
	}

	if opts.controlFlags != 0 {
		return nil, errors.New("invalid Control Flags (DISCONNECT)")
	}

	ver := MQTT311
	if opts.remainingLength > 0 {
		ver = MQTT50
	}

	return &Disconnect{
		Version:      ver,
		size:         opts.fixedHeaderLength + opts.remainingLength,
		remainLength: opts.remainingLength,
		timestamp:    opts.timestamp,
	}, nil
}

// NewDisconnect creates a DISCONNECT Packet.
func NewDisconnect(v MQTTVersion, c ReasonCode, p *Properties) Disconnect {
	return Disconnect{
		Version:    v,
		ReasonCode: c,
		Properties: p,
	}
}

// Write encodes the packet into bytes and writes it into the io.Writer.
func (pkt *Disconnect) Write(w *bufio.Writer) error {
	buf := &bytes.Buffer{}

	if pkt.Version == MQTT50 {
		_ = buf.WriteByte(byte(pkt.ReasonCode))

		err := writeProperties(buf, pkt.Properties, DISCONNECT)
		if err != nil {
			return fmt.Errorf("failed to write properties: %w", err)
		}
	}

	err := multierr.Combine(
		w.WriteByte(byte(DISCONNECT)<<packetTypeBit),
		writeVarInteger(w, buf.Len()),
	)

	n, errBuf := buf.WriteTo(w)
	err = multierr.Combine(err, errBuf)
	if err != nil {
		return fmt.Errorf("failed to send packet: %w", err)
	}

	pkt.timestamp = time.Now()
	pkt.size = 2 + int(n)

	return nil
}

// Read reads the packet bytes from bufio.Reader and decodes them into the
// packet.
func (pkt *Disconnect) Read(r *bufio.Reader) error {
	if pkt.Version == MQTT50 {
		rc, err := r.ReadByte()
		if err != nil {
			return fmt.Errorf("failed to read Reason Code: %w",
				ErrV5MalformedPacket)
		}
		pkt.ReasonCode = ReasonCode(rc)

		if pkt.remainLength > 2 {
			msg := make([]byte, pkt.remainLength-1)
			if _, err = io.ReadFull(r, msg); err != nil {
				return fmt.Errorf("failed to read remaining bytes: %w",
					ErrV5MalformedPacket)
			}
			buf := bytes.NewBuffer(msg)

			pkt.Properties, err = readProperties(buf, DISCONNECT)
			if err != nil {
				return fmt.Errorf("failed to read properties: %w", err)
			}
		}
	}

	return nil
}

// Type returns the packet type.
func (pkt *Disconnect) Type() Type {
	return DISCONNECT
}

// Size returns the packet size in bytes.
func (pkt *Disconnect) Size() int {
	return pkt.size
}

// Timestamp returns the timestamp of the moment which the packet has been
// received or has been sent.
func (pkt *Disconnect) Timestamp() time.Time {
	return pkt.timestamp
}
