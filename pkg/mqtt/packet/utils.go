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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
	"unsafe"

	"golang.org/x/exp/constraints"
)

func formatPacketError(pkt Packet, msg string, err error) error {
	return fmt.Errorf("%v (%v): %w", msg, pkt.Type().String(), err)
}

func readVarInteger(r io.ByteReader, val *int) (n int, err error) {
	multiplier := 1
	for {
		var b byte

		b, err = r.ReadByte()
		if err != nil {
			return 0, errors.New("invalid variable integer")
		}

		n++
		*val += int(b&127) * multiplier
		multiplier *= 128

		if multiplier > (128 * 128 * 128) {
			return 0, errors.New("invalid variable integer")
		}

		if b&128 == 0 {
			break
		}
	}

	return
}

func readUint[T constraints.Unsigned](buf *bytes.Buffer,
	version MQTTVersion) (T, error) {

	var val T
	size := int(unsafe.Sizeof(val))

	if buf.Len() < size {
		if version == MQTT50 {
			return 0, ErrV5MalformedPacket
		}

		return 0, fmt.Errorf("no enough bytes to read %v-bytes integer", size)
	}

	switch size {
	case 2:
		val = T(binary.BigEndian.Uint16(buf.Next(2)))
	case 4:
		val = T(binary.BigEndian.Uint32(buf.Next(4)))
	default:
		return 0, errors.New("invalid byte size")
	}

	return val, nil
}

func readString(buf *bytes.Buffer, ver MQTTVersion) ([]byte, error) {
	str, err := readBinary(buf, ver)
	if err != nil {
		return nil, err
	}

	if len(str) > 0 && !isValidUTF8String(str) {
		if ver == MQTT50 {
			return nil, ErrV5MalformedPacket
		}

		return nil, errors.New("invalid UTF8 string")
	}

	return str, nil
}

func readBinary(buf *bytes.Buffer, ver MQTTVersion) ([]byte, error) {
	if buf.Len() < 2 {
		if ver == MQTT50 {
			return nil, ErrV5MalformedPacket
		}

		return nil, errors.New("no enough bytes to decode binary")
	}

	length := int(binary.BigEndian.Uint16(buf.Next(2)))
	if length > buf.Len() {
		if ver == MQTT50 {
			return nil, ErrV5MalformedPacket
		}

		return nil, errors.New("invalid length of binary")
	}

	val := buf.Next(length)
	return val, nil
}

func writeVarInteger(w io.ByteWriter, val int) error {
	var data byte
	var err error

	for {
		data = byte(val % 128)

		val /= 128
		if val > 0 {
			data |= 128
		}

		err = w.WriteByte(data)
		if err != nil || val == 0 {
			return err
		}
	}
}

func writeBinary(w io.Writer, val []byte) (n int, err error) {
	_ = binary.Write(w, binary.BigEndian, uint16(len(val)))
	return w.Write(val)
}

func isValidUTF8String(str []byte) bool {
	for len(str) > 0 {
		r, size := utf8.DecodeRune(str)
		if r == utf8.RuneError || !utf8.ValidRune(r) {
			return false
		}
		if r >= '\u0000' && r <= '\u001F' {
			return false
		}
		if r >= '\u007F' && r <= '\u009F' {
			return false
		}

		str = str[size:]
	}

	return true
}

func isValidTopicName(topic string) bool {
	if len(topic) == 0 {
		return false
	}

	words := strings.Split(topic, "/")

	for _, word := range words {
		if strings.Contains(word, "#") || strings.Contains(word, "+") {
			return false
		}
	}

	return true
}
