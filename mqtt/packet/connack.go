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

// ConnAck represents the CONNACK Packet from MQTT specifications.
type ConnAck struct {
	// Version represents the MQTT version.
	Version MQTTVersion

	// ReturnCode represents the return code based on the MQTT specifications.
	ReturnCode ReturnCode

	// SessionPresent indicates if there is already a session associated with
	// the Client ID.
	SessionPresent bool

	// Properties represents the CONNACK properties (MQTT V5.0 only).
	Properties *Properties
}

// NewConnAck creates a CONNACK Packet.
func NewConnAck(
	v MQTTVersion, c ReturnCode, sessionPresent bool, p *Properties,
) ConnAck {
	return ConnAck{
		Version:        v,
		ReturnCode:     c,
		SessionPresent: sessionPresent,
		Properties:     p,
	}
}

// Pack encodes the packet into bytes and writes it into the io.Writer.
func (p *ConnAck) Pack(w *bufio.Writer) error {
	varHeader := &bytes.Buffer{}
	var err error

	// Acknowledge Flags
	if p.SessionPresent && p.Version != MQTT31 {
		_ = varHeader.WriteByte(1)
	} else {
		_ = varHeader.WriteByte(0)
	}

	// Return Code
	_ = varHeader.WriteByte(byte(p.ReturnCode))

	// Properties
	if p.Version == MQTT50 {
		if p.Properties == nil {
			p.Properties = &Properties{}
		}

		err = p.Properties.pack(varHeader, CONNACK)
		if err != nil {
			return err
		}
	}

	_ = w.WriteByte(byte(CONNACK) << packetTypeBit)
	_ = writeVarInteger(w, varHeader.Len())
	_, err = varHeader.WriteTo(w)
	return err
}

// Unpack reads the packet bytes from bytes.Buffer and decodes them into the
// packet.
// It is not supported by the CONNACK Packet in this broker.
func (p *ConnAck) Unpack(_ *bytes.Buffer) error {
	return errors.New("unsupported (CONNACK)")
}

// Type returns the packet type.
func (p *ConnAck) Type() Type {
	return CONNACK
}
