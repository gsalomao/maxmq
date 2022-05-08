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
	"errors"
	"time"
)

// PingResp represents the PINGRESP Packet from MQTT specifications.
type PingResp struct {
	timestamp time.Time
	size      int
}

// NewPingResp creates a PINGRESP Packet.
func NewPingResp() PingResp {
	return PingResp{timestamp: time.Now()}
}

// Pack encodes the packet into bytes and writes it into the io.Writer.
func (pkt *PingResp) Pack(w *bufio.Writer) error {
	_ = w.WriteByte(byte(PINGRESP) << packetTypeBit)
	err := w.WriteByte(0)
	pkt.size = 2

	return err
}

// Unpack reads the packet bytes from bufio.Reader and decodes them into the
// packet.
// It is not supported by the PINGRESP Packet in this broker.
func (pkt *PingResp) Unpack(_ *bufio.Reader) error {
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

// Timestamp returns the timestamp which the packet was created.
func (pkt *PingResp) Timestamp() time.Time {
	return pkt.timestamp
}
