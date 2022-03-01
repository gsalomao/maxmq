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
	"io"
	"time"
)

// Reader is responsible for read packets.
type Reader struct {
	bufReader     bufio.Reader
	maxPacketSize int
}

// ReaderOptions contains the options for the Reader.
type ReaderOptions struct {
	// BufferSize represents the buffer size.
	BufferSize int

	// MaxPacketSize represents the maximum packet size, in bytes, allowed.
	MaxPacketSize int
}

// NewReader creates a buffered Reader based on the io.Reader and ReaderOptions.
func NewReader(r io.Reader, o ReaderOptions) Reader {
	return Reader{
		bufReader:     *bufio.NewReaderSize(r, o.BufferSize),
		maxPacketSize: o.MaxPacketSize,
	}
}

// ReadPacket reads and unpack the packet from the buffer.
// It returns an error if it fails to read or unpack the packet.
func (r *Reader) ReadPacket() (Packet, error) {
	ctrlByte, err := r.bufReader.ReadByte()
	if err != nil {
		return nil, err
	}

	now := time.Now()

	var remainLen int
	n, err := readVarInteger(&r.bufReader, &remainLen)
	if err != nil {
		return nil, err
	}

	if remainLen > r.maxPacketSize {
		return nil, errors.New("max packet size exceeded")
	}

	opts := options{
		packetType:      Type(ctrlByte >> 4),
		controlFlags:    ctrlByte & controlByteMask,
		remainingLength: remainLen,
		size:            1 + n,
		timestamp:       now,
	}

	pkt, err := newPacket(opts)
	if err != nil {
		return nil, err
	}

	data := make([]byte, opts.remainingLength)
	if _, err := io.ReadFull(&r.bufReader, data); err != nil {
		return nil, errors.New("missing data")
	}

	buf := bytes.NewBuffer(data)
	err = pkt.Unpack(buf)
	if err != nil {
		return nil, err
	}

	return pkt, nil
}
