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
	"encoding/binary"
	"errors"
	"strings"
	"unicode/utf8"
)

func decodeVarInteger(r *bufio.Reader, val *int) (n int, err error) {
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

func readVarInteger(buf *bytes.Buffer, val *int) (n int, err error) {
	multiplier := 1
	for {
		var b byte

		b, err = buf.ReadByte()
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

func readUint16(buf *bytes.Buffer, ver MQTTVersion) (uint16, error) {
	if buf.Len() < 2 {
		if ver == MQTT50 {
			return 0, ErrV5MalformedPacket
		}

		return 0, errors.New("no enough bytes to read 2-bytes integer")
	}

	return binary.BigEndian.Uint16(buf.Next(2)), nil
}

func readUint32(buf *bytes.Buffer, ver MQTTVersion) (uint32, error) {
	if buf.Len() < 4 {
		if ver == MQTT50 {
			return 0, ErrV5MalformedPacket
		}

		return 0, errors.New("no enough bytes to read 4-bytes integer")
	}

	return binary.BigEndian.Uint32(buf.Next(4)), nil
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

func encodeBinary(w *bufio.Writer, data []byte) (n int, err error) {
	err = encodeUint16(w, uint16(len(data)))
	if err != nil {
		return
	}

	n, err = w.Write(data)
	if err != nil {
		return
	}

	n += 2
	return
}

func encodeUint16(w *bufio.Writer, val uint16) error {
	if err := w.WriteByte(byte(val >> 8)); err != nil {
		return err
	}

	return w.WriteByte(byte(val))
}

func encodeVarInteger(buf *bufio.Writer, val int) error {
	var data byte
	var err error

	for {
		data = byte(val % 128)

		val /= 128
		if val > 0 {
			data |= 128
		}

		err = buf.WriteByte(data)
		if err != nil || val == 0 {
			return err
		}
	}
}

func writeVarInteger(buf *bytes.Buffer, val int) error {
	var data byte
	var err error

	for {
		data = byte(val % 128)

		val /= 128
		if val > 0 {
			data |= 128
		}

		err = buf.WriteByte(data)
		if err != nil || val == 0 {
			return err
		}
	}
}

func writeUint16(buf *bytes.Buffer, val uint16) {
	buf.WriteByte(byte(val >> 8))
	buf.WriteByte(byte(val))
}

func writeUint32(buf *bytes.Buffer, val uint32) {
	buf.WriteByte(byte(val >> 24))
	buf.WriteByte(byte(val >> 16))
	buf.WriteByte(byte(val >> 8))
	buf.WriteByte(byte(val))
}

func writeBinary(buf *bytes.Buffer, val []byte) {
	writeUint16(buf, uint16(len(val)))
	buf.Write(val)
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
