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
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPacketConnect_InvalidPacketType(t *testing.T) {
	fh := FixedHeader{
		PacketType: DISCONNECT, // invalid
	}

	pkt, err := newPacketConnect(fh)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestPacketConnect_InvalidControlFlags(t *testing.T) {
	fh := FixedHeader{
		PacketType:   CONNECT,
		ControlFlags: 1, // invalid
	}

	pkt, err := newPacketConnect(fh)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestPacketConnect_PackUnsupported(t *testing.T) {
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 0, // variable header
		0, 1, 'a', // client ID
	}

	fh := FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: len(msg),
	}

	pkt, err := newPacketConnect(fh)
	require.Nil(t, err)
	require.NotNil(t, pkt)

	buf := new(bytes.Buffer)
	err = pkt.Pack(buf)
	require.NotNil(t, err)
}

func TestPacketConnect_Unpack(t *testing.T) {
	versions := []struct {
		test    string
		version MQTTVersion
		name    string
	}{
		{test: "V3.1", version: MQTT_V3_1, name: "MQIsdp"},
		{test: "V3.1.1", version: MQTT_V3_1_1, name: "MQTT"},
		{test: "V5.0", version: MQTT_V5_0, name: "MQTT"},
	}

	for _, v := range versions {
		t.Run(v.test, func(t *testing.T) {
			msg := []byte{0, byte(len(v.name))}
			msg = append(msg, []byte(v.name)...)
			msg = append(msg, byte(v.version), 0, 0, 0)
			if v.version == MQTT_V5_0 {
				msg = append(msg, 0)
			}
			msg = append(msg, 0, 1, 'a')

			fh := FixedHeader{
				PacketType:      CONNECT,
				RemainingLength: len(msg),
			}

			pkt, err := newPacketConnect(fh)
			require.Nil(t, err)
			require.NotNil(t, pkt)

			require.Equal(t, CONNECT, pkt.Type())
			connPkt, _ := pkt.(*PacketConnect)

			err = pkt.Unpack(bytes.NewBuffer(msg))
			require.Nil(t, err)
			assert.Equal(t, v.version, connPkt.Version)
			assert.Equal(t, []byte{'a'}, connPkt.ClientID)
		})
	}
}

func TestPacketConnect_UnpackProtocolNameMissing(t *testing.T) {
	msg := []byte{}
	fh := FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: len(msg),
	}

	pkt, err := newPacketConnect(fh)
	require.Nil(t, err)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.NotNil(t, err)
}

func TestPacketConnect_UnpackProtocolNameInvalid(t *testing.T) {
	names := []string{"MQT", "MQTTT", "MQTQ"}

	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			nLen := byte(len(name))
			buf := []byte(name)
			msg := []byte{0, nLen}
			msg = append(msg, buf...)
			msg = append(msg, 4, 0, 0, 0)     // variable header
			msg = append(msg, 0, 2, 'a', 'b') // client ID
			fh := FixedHeader{
				PacketType:      CONNECT,
				RemainingLength: len(msg),
			}

			pkt, err := newPacketConnect(fh)
			require.Nil(t, err)

			err = pkt.Unpack(bytes.NewBuffer(msg))
			assert.NotNil(t, err)
			assert.NotErrorIs(t, err, ErrV3UnacceptableProtocolVersion)
		})
	}
}

func TestPacketConnect_UnpackVersionMissing(t *testing.T) {
	msg := []byte{0, 4, 'M', 'Q', 'T', 'T'}
	fh := FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: len(msg),
	}

	pkt, err := newPacketConnect(fh)
	require.Nil(t, err)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.NotNil(t, err)
}

func TestPacketConnect_UnpackVersionInvalid(t *testing.T) {
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 0, 0, 0, 0, // variable header
		0, 1, 'a', // client ID
	}
	fh := FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: len(msg),
	}

	pkt, err := newPacketConnect(fh)
	require.Nil(t, err)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.NotNil(t, err)
	assert.ErrorIs(t, err, ErrV3UnacceptableProtocolVersion)
}

func TestPacketConnect_UnpackFlagsMissing(t *testing.T) {
	msg := []byte{0, 4, 'M', 'Q', 'T', 'T', 4}
	fh := FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: len(msg),
	}

	pkt, err := newPacketConnect(fh)
	require.Nil(t, err)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.NotNil(t, err)
}

func TestPacketConnect_UnpackFlagsReservedInvalid(t *testing.T) {
	// V3.1.1
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 1, 0, 0, // variable header
		0, 1, 'a', // client ID
	}
	fh := FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: len(msg),
	}

	pkt, err := newPacketConnect(fh)
	require.Nil(t, err)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.NotNil(t, err)
	assert.NotErrorIs(t, err, ErrV5MalformedPacket)

	// V5.0
	msg = []byte{
		0, 4, 'M', 'Q', 'T', 'T', 5, 1, 0, 0, // variable header
		0, 1, 'a', // client ID
	}
	fh = FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: len(msg),
	}

	pkt, err = newPacketConnect(fh)
	require.Nil(t, err)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.NotNil(t, err)
	assert.ErrorIs(t, err, ErrV5MalformedPacket)
}

func TestPacketConnect_UnpackFlagsWillQoS(t *testing.T) {
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 0x14, 0, 0, // variable header
		0, 1, 'a', // client ID
		0, 1, 't', // will topic
		0, 1, 'm', // will message
	}
	fh := FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: len(msg),
	}

	pkt, err := newPacketConnect(fh)
	require.Nil(t, err)

	require.Equal(t, CONNECT, pkt.Type())
	connPkt, _ := pkt.(*PacketConnect)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.Nil(t, err)
	assert.Equal(t, WillQoS2, connPkt.WillQoS)
}

func TestPacketConnect_UnpackFlagsWillQoSInvalid(t *testing.T) {
	// V3.1.1 - No Will Flag
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 0x10, 0, 0, // variable header
		0, 1, 'a', // client ID
	}
	fh := FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: len(msg),
	}

	pkt, err := newPacketConnect(fh)
	require.Nil(t, err)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.NotNil(t, err)
	assert.NotErrorIs(t, err, ErrV5MalformedPacket)

	// V3.1.1 - Invalid Will QoS
	msg = []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 0x1C, 0, 0, // variable header
		0, 1, 'a', // client ID
		0, 1, 't', // will topic
		0, 1, 'm', // will message
	}
	fh = FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: len(msg),
	}

	pkt, err = newPacketConnect(fh)
	require.Nil(t, err)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.NotNil(t, err)
	assert.NotErrorIs(t, err, ErrV5MalformedPacket)

	// V5.0 - No Will Flag
	msg = []byte{
		0, 4, 'M', 'Q', 'T', 'T', 5, 0x10, 0, 0, // variable header
		0,         // property length
		0, 1, 'a', // client ID
		0, // property length
	}
	fh = FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: len(msg),
	}

	pkt, err = newPacketConnect(fh)
	require.Nil(t, err)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.NotNil(t, err)
	assert.ErrorIs(t, err, ErrV5MalformedPacket)

	// V5.0 - Invalid Will QoS
	msg = []byte{
		0, 4, 'M', 'Q', 'T', 'T', 5, 0x1C, 0, 0, // variable header
		0,         // property length
		0, 1, 'a', // client ID
		0,         // property length
		0, 1, 't', // will topic
		0, 1, 'm', // will message
	}
	fh = FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: len(msg),
	}

	pkt, err = newPacketConnect(fh)
	require.Nil(t, err)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.NotNil(t, err)
	assert.ErrorIs(t, err, ErrV5MalformedPacket)
}

func TestPacketConnect_UnpackFlagsWillRetain(t *testing.T) {
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 0x24, 0, 0, // variable header
		0, 1, 'a', // client ID
		0, 1, 't', // will topic
		0, 1, 'm', // will message
	}
	fh := FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: len(msg),
	}

	pkt, err := newPacketConnect(fh)
	require.Nil(t, err)

	require.Equal(t, CONNECT, pkt.Type())
	connPkt, _ := pkt.(*PacketConnect)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.Nil(t, err)
	assert.True(t, connPkt.WillRetain)
}

func TestPacketConnect_UnpackFlagsWillRetainInvalid(t *testing.T) {
	// V3.1.1
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 0x20, 0, 0, // variable header
		0, 1, 'a', // client ID
		0, 1, 't', // will topic
		0, 1, 'm', // will message
	}
	fh := FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: len(msg),
	}

	pkt, err := newPacketConnect(fh)
	require.Nil(t, err)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.NotNil(t, err)
	assert.NotErrorIs(t, err, ErrV5MalformedPacket)

	// V5.0
	msg = []byte{
		0, 4, 'M', 'Q', 'T', 'T', 5, 0x20, 0, 0, // variable header
		0,         // property length
		0, 1, 'a', // client ID
		0,         // property length
		0, 1, 't', // will topic
		0, 1, 'm', // will message
	}
	fh = FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: len(msg),
	}

	pkt, err = newPacketConnect(fh)
	require.Nil(t, err)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.NotNil(t, err)
	assert.ErrorIs(t, err, ErrV5MalformedPacket)
}

func TestPacketConnect_UnpackFlagsUserNamePasswordInvalid(t *testing.T) {
	// V3.1.1
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 0x40, 0, 0, // variable header
		0, 1, 'a', // client ID
		0, 1, 'p', // password
	}
	fh := FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: len(msg),
	}

	pkt, err := newPacketConnect(fh)
	require.Nil(t, err)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.NotNil(t, err)
	assert.NotErrorIs(t, err, ErrV5MalformedPacket)

	// V5.0
	msg = []byte{
		0, 4, 'M', 'Q', 'T', 'T', 5, 0x40, 0, 0, // variable header
		0,         // property length
		0, 1, 'a', // client ID
		0,         // property length
		0, 1, 'p', // password
	}
	fh = FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: len(msg),
	}

	pkt, err = newPacketConnect(fh)
	require.Nil(t, err)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.NotNil(t, err)
	assert.ErrorIs(t, err, ErrV5MalformedPacket)
}

func TestPacketConnect_UnpackKeepAliveValid(t *testing.T) {
	keepAlives := []uint16{0, 60, 900, 65535}

	for _, ka := range keepAlives {
		msb := byte(ka >> 8)
		lsb := byte(ka & 0xFF)
		msg := []byte{
			0, 4, 'M', 'Q', 'T', 'T', 4, 0, msb, lsb, // variable header
			0, 1, 'a', // client ID
		}
		fh := FixedHeader{
			PacketType:      CONNECT,
			RemainingLength: len(msg),
		}

		pkt, err := newPacketConnect(fh)
		require.Nil(t, err)

		require.Equal(t, CONNECT, pkt.Type())
		connPkt, _ := pkt.(*PacketConnect)

		err = pkt.Unpack(bytes.NewBuffer(msg))
		require.Nil(t, err)
		assert.Equal(t, ka, connPkt.KeepAlive)
	}
}

func TestPacketConnect_UnpackKeepAliveInvalid(t *testing.T) {
	// V3.1.1
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, // variable header
	}
	fh := FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: len(msg),
	}

	pkt, err := newPacketConnect(fh)
	require.Nil(t, err)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	assert.NotNil(t, err)

	// V5.0
	msg = []byte{
		0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, // variable header
	}
	fh = FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: len(msg),
	}

	pkt, err = newPacketConnect(fh)
	require.Nil(t, err)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	assert.NotNil(t, err)
	assert.ErrorIs(t, err, ErrV5MalformedPacket)
}

func TestPacketConnect_UnpackClientIDValid(t *testing.T) {
	codePoints := []rune{
		'\u0020', '\u007E',
		'\u00A0', '\uD7FF',
		'\uE000', '\U0010FFFF',
	}

	for _, codePoint := range codePoints {
		cp := make([]byte, 4)

		cpLen := utf8.EncodeRune(cp, codePoint)
		cpLenBuf := make([]byte, 2)

		cp = cp[:cpLen]
		binary.BigEndian.PutUint16(cpLenBuf, uint16(cpLen))

		msg := []byte{
			0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 0, // variable header
		}
		msg = append(msg, cpLenBuf...)
		msg = append(msg, cp...)
		fh := FixedHeader{
			PacketType:      CONNECT,
			RemainingLength: len(msg),
		}

		pkt, err := newPacketConnect(fh)
		require.Nil(t, err)

		require.Equal(t, CONNECT, pkt.Type())
		connPkt, _ := pkt.(*PacketConnect)

		err = pkt.Unpack(bytes.NewBuffer(msg))
		require.Nil(t, err)
		assert.Equal(t, cp, connPkt.ClientID)
	}

	// Clear session
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 2, 0, 0, // variable header
		0, 0, // client ID
	}
	fh := FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: len(msg),
	}

	pkt, err := newPacketConnect(fh)
	require.Nil(t, err)

	require.Equal(t, CONNECT, pkt.Type())
	connPkt, _ := pkt.(*PacketConnect)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.Nil(t, err)
	assert.Equal(t, []byte{}, connPkt.ClientID)
}

func TestPacketConnect_UnpackClientIDMalformed(t *testing.T) {
	cIDs := []struct {
		len  []byte
		data []byte
	}{
		{len: []byte{}, data: []byte{}},
		{len: []byte{1}, data: []byte{}},
		{len: []byte{0, 1}, data: []byte{}},
		{len: []byte{0, 2}, data: []byte{'a'}},
	}

	for _, id := range cIDs {
		// V3.1.1
		msg := []byte{
			0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 0, // variable header
		}
		msg = append(msg, id.len...)
		msg = append(msg, id.data...)
		fh := FixedHeader{
			PacketType:      CONNECT,
			RemainingLength: len(msg),
		}

		pkt, err := newPacketConnect(fh)
		require.Nil(t, err)

		err = pkt.Unpack(bytes.NewBuffer(msg))
		require.NotNil(t, err)
		assert.NotErrorIs(t, err, ErrV5MalformedPacket)

		// V5.0
		msg = []byte{
			0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 0, // variable header
			0, // property length
		}
		msg = append(msg, id.len...)
		msg = append(msg, id.data...)
		fh = FixedHeader{
			PacketType:      CONNECT,
			RemainingLength: len(msg),
		}

		pkt, err = newPacketConnect(fh)
		require.Nil(t, err)

		err = pkt.Unpack(bytes.NewBuffer(msg))
		require.NotNil(t, err)
		assert.ErrorIs(t, err, ErrV5MalformedPacket)
	}

	codePoints := []rune{
		'\u0000', '\u001F',
		'\u007F', '\u009F',
		0xD800,
	}

	for _, codePoint := range codePoints {
		cp := make([]byte, 4)

		cpLen := utf8.EncodeRune(cp, codePoint)
		cpLenBuf := make([]byte, 2)

		cp = cp[:cpLen]
		binary.BigEndian.PutUint16(cpLenBuf, uint16(cpLen))

		// V3.1.1
		msg := []byte{
			0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 0, // variable header
		}
		msg = append(msg, cpLenBuf...)
		msg = append(msg, cp...)
		fh := FixedHeader{
			PacketType:      CONNECT,
			RemainingLength: len(msg),
		}

		pkt, err := newPacketConnect(fh)
		require.Nil(t, err)

		err = pkt.Unpack(bytes.NewBuffer(msg))
		require.NotNil(t, err)
		assert.NotErrorIs(t, err, ErrV5MalformedPacket)

		// V5.0
		msg = []byte{
			0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 0, // variable header
		}
		msg = append(msg, cpLenBuf...)
		msg = append(msg, cp...)
		fh = FixedHeader{
			PacketType:      CONNECT,
			RemainingLength: len(msg),
		}

		pkt, err = newPacketConnect(fh)
		require.Nil(t, err)

		err = pkt.Unpack(bytes.NewBuffer(msg))
		require.NotNil(t, err)
		assert.ErrorIs(t, err, ErrV5MalformedPacket)
	}
}

func TestPacketConnect_UnpackClientIDRejected(t *testing.T) {
	// V3.1
	msg := []byte{
		0, 6, 'M', 'Q', 'I', 's', 'd', 'p', 3, 0, 0, 0, // variable header
		0, 0, // client ID
	}
	fh := FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: len(msg),
	}

	pkt, err := newPacketConnect(fh)
	require.Nil(t, err)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.NotNil(t, err)
	assert.ErrorIs(t, err, ErrV3IdentifierRejected)

	// V3.1.1
	msg = []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 0, // variable header
		0, 0, // payload
	}
	fh = FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: len(msg),
	}

	pkt, err = newPacketConnect(fh)
	require.Nil(t, err)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.NotNil(t, err)
	assert.ErrorIs(t, err, ErrV3IdentifierRejected)
}

func TestPacketConnect_UnpackWillTopicValid(t *testing.T) {
	topics := []string{"topic", "dev/client-1/will"}

	for _, wt := range topics {
		t.Run(wt, func(t *testing.T) {
			msg := []byte{
				0, 4, 'M', 'Q', 'T', 'T', 4, 4, 0, 0, // variable header
				0, 1, 'a', // client ID
			}

			buf := []byte(wt)
			wtLenMSB := byte(len(wt) >> 8)
			wtLenLSB := byte(len(wt) & 0xFF)
			msg = append(msg, wtLenMSB, wtLenLSB)
			msg = append(msg, buf...)
			msg = append(msg, 0, 1, 'm') // will message
			fh := FixedHeader{
				PacketType:      CONNECT,
				RemainingLength: len(msg),
			}

			pkt, err := newPacketConnect(fh)
			require.Nil(t, err)

			require.Equal(t, CONNECT, pkt.Type())
			connPkt, _ := pkt.(*PacketConnect)

			err = pkt.Unpack(bytes.NewBuffer(msg))
			require.Nil(t, err)
			assert.Equal(t, buf, connPkt.WillTopic)
		})
	}
}

func TestPacketConnect_UnpackWillTopicMissing(t *testing.T) {
	// V3.1.1
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 4, 0, 0, // variable header
		0, 1, 'a', // client ID
	}
	fh := FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: len(msg),
	}

	pkt, err := newPacketConnect(fh)
	require.Nil(t, err)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.NotNil(t, err)
	assert.NotErrorIs(t, err, ErrV5MalformedPacket)

	// V5.0
	msg = []byte{
		0, 4, 'M', 'Q', 'T', 'T', 5, 4, 0, 0, // variable header
		0,         // property length
		0, 1, 'a', // client ID
		0, // will property length
	}
	fh = FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: len(msg),
	}

	pkt, err = newPacketConnect(fh)
	require.Nil(t, err)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.NotNil(t, err)
	assert.ErrorIs(t, err, ErrV5MalformedPacket)
}

func TestPacketConnect_UnpackWillMessageValid(t *testing.T) {
	messages := []string{"", "hello"}

	for _, m := range messages {
		t.Run(m, func(t *testing.T) {
			msg := []byte{
				0, 4, 'M', 'Q', 'T', 'T', 4, 4, 0, 0, // variable header
				0, 1, 'a', // client ID
				0, 1, 't', // will topic
			}

			buf := []byte(m)
			wmLenMSB := byte(len(m) >> 8)
			wmLenLSB := byte(len(m) & 0xFF)
			msg = append(msg, wmLenMSB, wmLenLSB)
			msg = append(msg, buf...)
			fh := FixedHeader{
				PacketType:      CONNECT,
				RemainingLength: len(msg),
			}

			pkt, err := newPacketConnect(fh)
			require.Nil(t, err)

			require.Equal(t, CONNECT, pkt.Type())
			connPkt, _ := pkt.(*PacketConnect)

			err = pkt.Unpack(bytes.NewBuffer(msg))
			require.Nil(t, err)
			assert.Equal(t, buf, connPkt.WillMessage)
		})
	}
}

func TestPacketConnect_UnpackWillMessageMissing(t *testing.T) {
	// V3.1.1
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 4, 0, 0, // variable header
		0, 1, 'a', // client ID
		0, 1, 't', // will topic
	}
	fh := FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: len(msg),
	}

	pkt, err := newPacketConnect(fh)
	require.Nil(t, err)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.NotNil(t, err)
	assert.NotErrorIs(t, err, ErrV5MalformedPacket)

	// V5.0
	msg = []byte{
		0, 4, 'M', 'Q', 'T', 'T', 5, 4, 0, 0, // variable header
		0,         // property length
		0, 1, 'a', // client ID
		0,         // will property length
		0, 1, 't', // will topic
	}
	fh = FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: len(msg),
	}

	pkt, err = newPacketConnect(fh)
	require.Nil(t, err)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.NotNil(t, err)
	assert.ErrorIs(t, err, ErrV5MalformedPacket)
}

func TestPacketConnect_UnpackUserNameValid(t *testing.T) {
	userNames := []string{"", "username"}

	for _, n := range userNames {
		t.Run(n, func(t *testing.T) {
			msg := []byte{
				0, 4, 'M', 'Q', 'T', 'T', 4, 0x80, 0, 0, // variable header
				0, 1, 'a', // client ID
			}

			buf := []byte(n)
			nLenMSB := byte(len(n) >> 8)
			nLenLSB := byte(len(n) & 0xFF)
			msg = append(msg, nLenMSB, nLenLSB)
			msg = append(msg, buf...)
			fh := FixedHeader{
				PacketType:      CONNECT,
				RemainingLength: len(msg),
			}

			pkt, err := newPacketConnect(fh)
			require.Nil(t, err)

			require.Equal(t, CONNECT, pkt.Type())
			connPkt, _ := pkt.(*PacketConnect)

			err = pkt.Unpack(bytes.NewBuffer(msg))
			require.Nil(t, err)
			assert.Equal(t, buf, connPkt.UserName)
		})
	}
}

func TestPacketConnect_UnpackUserNameMissing(t *testing.T) {
	// v3.1.1
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 0x80, 0, 0, // variable header
		0, 1, 'a', // client ID
	}

	fh := FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: len(msg),
	}

	pkt, err := newPacketConnect(fh)
	require.Nil(t, err)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.NotNil(t, err)
	assert.NotErrorIs(t, err, ErrV5MalformedPacket)

	// v5.0
	msg = []byte{
		0, 4, 'M', 'Q', 'T', 'T', 5, 0x80, 0, 0, // variable header
		0,         // property length
		0, 1, 'a', // client ID
		0, // will property length
	}

	fh = FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: len(msg),
	}

	pkt, err = newPacketConnect(fh)
	require.Nil(t, err)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.NotNil(t, err)
	assert.ErrorIs(t, err, ErrV5MalformedPacket)
}

func TestPacketConnect_UnpackPasswordValid(t *testing.T) {
	passwords := []string{"", "password"}

	for _, p := range passwords {
		t.Run(p, func(t *testing.T) {
			msg := []byte{
				0, 4, 'M', 'Q', 'T', 'T', 4, 0xC0, 0, 0, // variable header
				0, 1, 'a', // client ID
				0, 1, 'u', // user name
			}

			buf := []byte(p)
			pLenMSB := byte(len(p) >> 8)
			pLenLSB := byte(len(p) & 0xFF)
			msg = append(msg, pLenMSB, pLenLSB)
			msg = append(msg, buf...)
			fh := FixedHeader{
				PacketType:      CONNECT,
				RemainingLength: len(msg),
			}

			pkt, err := newPacketConnect(fh)
			require.Nil(t, err)

			require.Equal(t, CONNECT, pkt.Type())
			connPkt, _ := pkt.(*PacketConnect)

			err = pkt.Unpack(bytes.NewBuffer(msg))
			require.Nil(t, err)
			assert.Equal(t, buf, connPkt.Password)
		})
	}
}

func TestPacketConnect_UnpackPasswordInvalid(t *testing.T) {
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 0xC0, 0, 0, // variable header
		0, 1, 'a', // client ID
		0, 1, 'u', // user name
	}

	fh := FixedHeader{
		PacketType:      CONNECT,
		RemainingLength: len(msg),
	}

	pkt, err := newPacketConnect(fh)
	require.Nil(t, err)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.NotNil(t, err)
	assert.NotErrorIs(t, err, ErrV5MalformedPacket)
}
