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
	"io"
)

// Writer is responsible for write packets.
type Writer struct {
	bufWriter *bufio.Writer
}

// NewWriter creates a buffered writer based on the io.Writer and buffer size.
func NewWriter(w io.Writer, bufSize int) Writer {
	return Writer{
		bufWriter: bufio.NewWriterSize(w, bufSize),
	}
}

// WritePacket writes the given Packet into the buffer.
// It returns an error if it fails to write the packet.
func (w *Writer) WritePacket(pkt Packet) error {
	err := pkt.Pack(w.bufWriter)
	if err != nil {
		return err
	}

	return w.bufWriter.Flush()
}
