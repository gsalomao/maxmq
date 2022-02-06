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
)

// Reader is responsible for read packets.
type Reader struct {
	bufReader bufio.Reader
}

// NewReader creates a buffered Reader based on the io.Reader and the buffer
// size.
func NewReader(r io.Reader, bufSize int) Reader {
	return Reader{
		bufReader: *bufio.NewReaderSize(r, bufSize),
	}
}

// ReadPacket reads and unpack the packet from the buffer.
// It returns an error if it fails to read or unpack the packet.
func (r *Reader) ReadPacket() (Packet, error) {
	ctrlByte, err := r.bufReader.ReadByte()
	if err != nil {
		return nil, err
	}

	len, err := readVariableInteger(&r.bufReader)
	if err != nil {
		return nil, err
	}

	// TODO: Check maximum packet size from configuration

	fh := FixedHeader{
		PacketType:      PacketType(ctrlByte >> 4),
		ControlFlags:    ctrlByte & 0x0F,
		RemainingLength: len,
	}

	pkt, err := NewPacket(fh)
	if err != nil {
		return nil, err
	}

	data := make([]byte, fh.RemainingLength)
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
