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
	"fmt"
	"io"
	"sync"
)

// Writer is responsible for write packets.
type Writer struct {
	writerPool sync.Pool
}

// NewWriter creates a buffered writer.
func NewWriter(bufSize int) Writer {
	return Writer{
		writerPool: sync.Pool{
			New: func() interface{} {
				return bufio.NewWriterSize(nil, bufSize)
			},
		},
	}
}

// WritePacket writes the given Packet into the io.Writer.
// It returns an error if it fails to write the packet.
func (w *Writer) WritePacket(pkt Packet, wr io.Writer) error {
	bufWr := w.writerPool.Get().(*bufio.Writer)
	defer w.writerPool.Put(bufWr)
	bufWr.Reset(wr)

	err := pkt.Pack(bufWr)
	if err != nil {
		return fmt.Errorf("failed to pack packet %v: %w",
			pkt.Type().String(), err)
	}

	return bufWr.Flush()
}
