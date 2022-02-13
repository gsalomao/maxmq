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
	"encoding/binary"
	"errors"
	"io"
	"unicode/utf8"
)

func readVarInteger(rd io.Reader) (int, error) {
	value := 0
	multiplier := 1
	data := make([]byte, 1)

	for {
		_, err := rd.Read(data)
		if err != nil {
			return 0, errors.New("invalid variable integer")
		}

		value += int(data[0]&127) * multiplier
		multiplier *= 128

		if multiplier > (128 * 128 * 128) {
			return 0, errors.New("invalid variable integer")
		}

		if data[0]&128 == 0 {
			break
		}
	}

	return value, nil
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

func writeVarInteger(w *bufio.Writer, val int) error {
	var data byte
	var err error

	for {
		data = byte(val % 128)

		val /= 128
		if val > 0 {
			data |= 128
		}

		err = w.WriteByte(data)
		if err != nil {
			return err
		}

		if val == 0 {
			return nil
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
