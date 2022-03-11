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

// PingReq represents the PINGREQ Packet from MQTT specifications.
type PingReq struct {
	size      int
	timestamp time.Time
}

func newPacketPingReq(opts options) (Packet, error) {
	if opts.packetType != PINGREQ {
		return nil, errors.New("packet type is not PINGREQ")
	}

	if opts.controlFlags != 0 {
		return nil, errors.New("invalid Control Flags (PINGREQ)")
	}

	if opts.remainingLength != 0 {
		return nil, errors.New("invalid Remain Length (PINGREQ)")
	}

	return &PingReq{size: opts.size, timestamp: opts.timestamp}, nil
}

// Pack encodes the packet into bytes and writes it into the io.Writer.
// It is not supported by the PINGREQ Packet in this broker.
func (pkt *PingReq) Pack(_ *bufio.Writer) error {
	return errors.New("unsupported (PINGREQ)")
}

// Unpack reads the packet bytes from bytes.Buffer and decodes them into the
// packet.
func (pkt *PingReq) Unpack(_ *bytes.Buffer) error {
	return nil
}

// Type returns the packet type.
func (pkt *PingReq) Type() Type {
	return PINGREQ
}

// Size returns the packet size in bytes.
func (pkt *PingReq) Size() int {
	return pkt.size
}

// Timestamp returns the timestamp which the packet was created.
func (pkt *PingReq) Timestamp() time.Time {
	return pkt.timestamp
}
