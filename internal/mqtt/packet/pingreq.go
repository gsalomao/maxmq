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
	"errors"
	"time"
)

// PingReq represents the PINGREQ Packet from MQTT specifications.
type PingReq struct {
	// Version represents the MQTT version.
	Version Version

	// Unexported fields
	timestamp time.Time
	size      int
}

func newPacketPingReq(opts options) (p Packet, err error) {
	if opts.packetType != PINGREQ {
		return nil, errors.New("packet type is not PINGREQ")
	}

	if opts.controlFlags != 0 {
		return nil, errors.New("invalid Control Flags (PINGREQ)")
	}

	if opts.remainingLength != 0 {
		return nil, errors.New("invalid Remain Length (PINGREQ)")
	}

	p = &PingReq{
		Version:   opts.version,
		size:      opts.fixedHeaderLength,
		timestamp: opts.timestamp,
	}
	return p, nil
}

// Write encodes the packet into bytes and writes it into the io.Writer.
// It is not supported by the PINGREQ Packet.
func (pkt *PingReq) Write(_ *bufio.Writer) error {
	return errors.New("unsupported (PINGREQ)")
}

// Read reads the packet bytes from bufio.Reader and decodes them into the packet.
func (pkt *PingReq) Read(_ *bufio.Reader) error {
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

// Timestamp returns the timestamp of the moment which the packet has been received.
func (pkt *PingReq) Timestamp() time.Time {
	return pkt.timestamp
}
