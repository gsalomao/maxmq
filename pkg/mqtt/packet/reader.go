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
	"fmt"
	"io"
	"sync"
	"time"
)

// Reader is responsible for read packets.
type Reader struct {
	readerPool    sync.Pool
	maxPacketSize int
}

// ReaderOptions contains the options for the Reader.
type ReaderOptions struct {
	// BufferSize represents the buffer size.
	BufferSize int

	// MaxPacketSize represents the maximum packet size, in bytes, allowed.
	MaxPacketSize int
}

// NewReader creates a buffered Reader using ReaderOptions.
func NewReader(o ReaderOptions) Reader {
	return Reader{
		readerPool: sync.Pool{
			New: func() interface{} {
				return bufio.NewReaderSize(nil, o.BufferSize)
			},
		},
		maxPacketSize: o.MaxPacketSize,
	}
}

// ReadPacket reads and unpack a packet from the io.Reader.
// It returns an error if it fails to read or unpack the packet.
func (r *Reader) ReadPacket(rd io.Reader, ver MQTTVersion) (Packet, error) {
	ctrlByte := make([]byte, 1)

	_, err := rd.Read(ctrlByte)
	if err != nil {
		return nil, fmt.Errorf("failed to read control byte: %w", err)
	}
	now := time.Now()

	bufRd := r.readerPool.Get().(*bufio.Reader)
	defer r.readerPool.Put(bufRd)
	bufRd.Reset(rd)

	var remainLen int
	n, err := readVarInteger(bufRd, &remainLen)
	if err != nil {
		return nil, fmt.Errorf("failed to read remain length: %w", err)
	}

	if remainLen > r.maxPacketSize {
		return nil, errors.New("max packet size exceeded")
	}

	opts := options{
		packetType:        Type(ctrlByte[0] >> 4),
		controlFlags:      ctrlByte[0] & controlByteFlagsMask,
		fixedHeaderLength: 1 + n,
		remainingLength:   remainLen,
		timestamp:         now,
		version:           ver,
	}

	pkt, err := newPacket(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to read packet: %w", err)
	}

	err = pkt.Read(bufRd)
	if err != nil {
		return nil, fmt.Errorf("failed to read packet %v: %w",
			pkt.Type().String(), err)
	}

	return pkt, nil
}
