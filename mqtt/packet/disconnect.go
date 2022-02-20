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
	"bufio"
	"bytes"
	"errors"
)

// Disconnect represents the DISCONNECT Packet from MQTT specifications.
type Disconnect struct {
	// Version represents the MQTT version.
	Version MQTTVersion

	// ReasonCode represents the reason code based on the MQTT specifications.
	ReasonCode ReasonCode

	// Properties represents the DISCONNECT properties (MQTT V5.0 only).
	Properties *Properties
}

func newPacketDisconnect(fh fixedHeader) (Packet, error) {
	if fh.packetType != DISCONNECT {
		return nil, errors.New("packet type is not DISCONNECT")
	}

	if fh.controlFlags != 0 {
		return nil, errors.New("invalid Control Flags (DISCONNECT)")
	}

	ver := MQTT311
	if fh.remainingLength > 0 {
		ver = MQTT50
	}

	return &Disconnect{Version: ver}, nil
}

// NewDisconnect creates a DISCONNECT Packet.
func NewDisconnect(v MQTTVersion, c ReasonCode, p *Properties) Disconnect {
	return Disconnect{
		Version:    v,
		ReasonCode: c,
		Properties: p,
	}
}

// Pack encodes the packet into bytes and writes it into the io.Writer.
func (pkt *Disconnect) Pack(w *bufio.Writer) error {
	varHeader := &bytes.Buffer{}

	if pkt.Version == MQTT50 {
		_ = varHeader.WriteByte(byte(pkt.ReasonCode))

		if pkt.Properties == nil {
			pkt.Properties = &Properties{}
		}

		err := pkt.Properties.pack(varHeader, DISCONNECT)
		if err != nil {
			return err
		}
	}

	_ = w.WriteByte(byte(DISCONNECT) << packetTypeBit)
	_ = writeVarInteger(w, varHeader.Len())
	_, err := varHeader.WriteTo(w)
	return err
}

// Unpack reads the packet bytes from bytes.Buffer and decodes them into the
// packet.
func (pkt *Disconnect) Unpack(buf *bytes.Buffer) error {
	if pkt.Version == MQTT50 {
		rc, err := buf.ReadByte()
		if err != nil {
			return errors.New("no Reason Code (DISCONNECT)")
		}

		pkt.ReasonCode = ReasonCode(rc)
		pkt.Properties = &Properties{}

		err = pkt.Properties.unpack(buf, DISCONNECT)
		if err != nil {
			return err
		}
	}

	return nil
}

// Type returns the packet type.
func (pkt *Disconnect) Type() Type {
	return DISCONNECT
}
