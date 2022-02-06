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
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"unicode/utf8"
)

func readVariableInteger(rd io.Reader) (int, error) {
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

func unpackByte(buf *bytes.Buffer, ver MQTTVersion) (byte, error) {
	b, err := buf.ReadByte()
	if err != nil {
		if ver == MQTT_V5_0 {
			return 0, ErrV5MalformedPacket
		}

		return 0, err
	}

	return b, nil
}

func unpackUint16(buf *bytes.Buffer, ver MQTTVersion) (uint16, error) {
	u, err := decodeUint16(buf)
	if err != nil {
		if ver == MQTT_V5_0 {
			return 0, ErrV5MalformedPacket
		}

		return 0, err
	}

	return u, nil
}

func unpackUint32(buf *bytes.Buffer, ver MQTTVersion) (uint32, error) {
	u, err := decodeUint32(buf)
	if err != nil {
		if ver == MQTT_V5_0 {
			return 0, ErrV5MalformedPacket
		}

		return 0, err
	}

	return u, nil
}

func unpackString(buf *bytes.Buffer, ver MQTTVersion) ([]byte, error) {
	str, err := decodeString(buf)
	if err != nil {
		if ver == MQTT_V5_0 {
			return nil, ErrV5MalformedPacket
		}

		return nil, err
	}

	return str, nil
}

func unpackBinary(buf *bytes.Buffer, ver MQTTVersion) ([]byte, error) {
	b, err := decodeBinary(buf)
	if err != nil {
		if ver == MQTT_V5_0 {
			return nil, ErrV5MalformedPacket
		}

		return nil, err
	}

	return b, nil
}

func decodeUint16(buf *bytes.Buffer) (uint16, error) {
	if buf.Len() < 2 {
		return 0, errors.New("no enough bytes to decode 2-bytes integer data")
	}

	return binary.BigEndian.Uint16(buf.Next(2)), nil
}

func decodeUint32(buf *bytes.Buffer) (uint32, error) {
	if buf.Len() < 4 {
		return 0, errors.New("no enough bytes to decode 4-bytes integer data")
	}

	return binary.BigEndian.Uint32(buf.Next(4)), nil
}

func decodeString(buf *bytes.Buffer) ([]byte, error) {
	str, err := decodeBinary(buf)
	if err != nil {
		return nil, err
	}

	if len(str) > 0 && !isValidUTF8String(str) {
		return nil, errors.New("invalid UTF8 string")
	}

	return str, nil
}

func decodeBinary(buf *bytes.Buffer) ([]byte, error) {
	if buf.Len() < 2 {
		return nil, errors.New("no enough bytes to decode binary data")
	}

	length := int(binary.BigEndian.Uint16(buf.Next(2)))
	if length > buf.Len() {
		return nil, errors.New("invalid length of binary data")
	}

	val := buf.Next(length)
	return val, nil
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
