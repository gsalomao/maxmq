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

// PingReq represents the PINGREQ Packet from MQTT specifications.
type PingReq struct{}

func newPacketPingReq(fh fixedHeader) (Packet, error) {
	if fh.packetType != PINGREQ {
		return nil, errors.New("packet type is not PINGREQ")
	}

	if fh.controlFlags != 0 {
		return nil, errors.New("invalid Control Flags (PINGREQ)")
	}

	if fh.remainingLength != 0 {
		return nil, errors.New("invalid Remain Length (PINGREQ)")
	}

	return &PingReq{}, nil
}

// Pack encodes the packet into bytes and writes it into the io.Writer.
// It is not supported by the PINGREQ Packet in this broker.
func (p *PingReq) Pack(_ *bufio.Writer) error {
	return errors.New("unsupported (PINGREQ)")
}

// Unpack reads the packet bytes from bytes.Buffer and decodes them into the
// packet.
func (p *PingReq) Unpack(_ *bytes.Buffer) error {
	return nil
}

// Type returns the packet type.
func (p *PingReq) Type() Type {
	return PINGREQ
}
