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

// PingResp represents the PINGRESP Packet from MQTT specifications.
type PingResp struct {
	// Unexported fields
	timestamp time.Time
	size      int
}

// NewPingResp creates a PINGRESP Packet.
func NewPingResp() PingResp {
	return PingResp{}
}

// Write encodes the packet into bytes and writes it into the io.Writer.
func (pkt *PingResp) Write(w *bufio.Writer) error {
	_ = w.WriteByte(byte(PINGRESP) << packetTypeBit)
	err := w.WriteByte(0)
	pkt.timestamp = time.Now()
	pkt.size = 2

	return err
}

// Read reads the packet bytes from bufio.Reader and decodes them into the packet.
// It is not supported by the PINGRESP Packet.
func (pkt *PingResp) Read(_ *bufio.Reader) error {
	return errors.New("unsupported (PINGRESP)")
}

// Type returns the packet type.
func (pkt *PingResp) Type() Type {
	return PINGRESP
}

// Size returns the packet size in bytes.
func (pkt *PingResp) Size() int {
	return pkt.size
}

// Timestamp returns the timestamp of the moment the packet has been sent.
func (pkt *PingResp) Timestamp() time.Time {
	return pkt.timestamp
}
