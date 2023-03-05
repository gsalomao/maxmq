// Copyright 2022-2023 The MaxMQ Authors
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
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectInvalidPacketType(t *testing.T) {
	opts := options{packetType: DISCONNECT}
	pkt, err := newPacketConnect(opts)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestConnectInvalidControlFlags(t *testing.T) {
	opts := options{packetType: CONNECT, controlFlags: 1}
	pkt, err := newPacketConnect(opts)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestConnectWriteUnsupported(t *testing.T) {
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 0, // variable header
		0, 1, 'a', // client ID
	}

	opts := options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err := newPacketConnect(opts)
	require.Nil(t, err)
	require.NotNil(t, pkt)

	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)
	err = pkt.Write(wr)
	require.NotNil(t, err)
}

func TestConnectRead(t *testing.T) {
	testCases := []struct {
		test    string
		version Version
		name    string
	}{
		{test: "V3.1", version: MQTT31, name: "MQIsdp"},
		{test: "V3.1.1", version: MQTT311, name: "MQTT"},
		{test: "V5.0", version: MQTT50, name: "MQTT"},
	}

	for _, tc := range testCases {
		t.Run(tc.test, func(t *testing.T) {
			msg := []byte{0, byte(len(tc.name))}
			msg = append(msg, []byte(tc.name)...)
			msg = append(msg, byte(tc.version), 0, 0, 0)
			if tc.version == MQTT50 {
				msg = append(msg, 0)
			}
			msg = append(msg, 0, 1, 'a')

			opts := options{packetType: CONNECT, remainingLength: len(msg)}
			pkt, err := newPacketConnect(opts)
			require.Nil(t, err)
			require.NotNil(t, pkt)

			require.Equal(t, CONNECT, pkt.Type())
			connPkt, _ := pkt.(*Connect)

			err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
			require.Nil(t, err)
			assert.Equal(t, tc.version, connPkt.Version)
			assert.Equal(t, []byte{'a'}, connPkt.ClientID)
		})
	}
}

func BenchmarkConnectReadV3(b *testing.B) {
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 60, // variable header
		0, 1, 'a', // client ID
	}
	opts := options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, _ := newPacketConnect(opts)
	buf := bytes.NewBuffer(msg)
	rd := bufio.NewReader(buf)

	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		err := pkt.Read(rd)
		if err != nil {
			b.Fatal(err)
		}

		buf = bytes.NewBuffer(msg)
		rd.Reset(buf)
	}
}

func BenchmarkConnectReadV5(b *testing.B) {
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 60, // variable header
		0,         // property length
		0, 1, 'a', // client ID
	}
	opts := options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, _ := newPacketConnect(opts)
	buf := bytes.NewBuffer(msg)
	rd := bufio.NewReader(buf)

	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		err := pkt.Read(rd)
		if err != nil {
			b.Fatal(err)
		}

		buf = bytes.NewBuffer(msg)
		rd.Reset(buf)
	}
}

func TestConnectReadProtocolNameMissing(t *testing.T) {
	var msg []byte
	opts := options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err := newPacketConnect(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	require.ErrorIs(t, err, ErrV5MalformedPacket)
}

func TestConnectReadProtocolNameInvalid(t *testing.T) {
	testCases := []string{"MQT", "MQTT_", "MTT"}

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			nLen := byte(len(tc))
			buf := []byte(tc)
			msg := []byte{0, nLen}
			msg = append(msg, buf...)
			msg = append(msg, 4, 0, 0, 0)     // variable header
			msg = append(msg, 0, 2, 'a', 'b') // client ID

			opts := options{packetType: CONNECT, remainingLength: len(msg)}
			pkt, err := newPacketConnect(opts)
			require.Nil(t, err)

			err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
			assert.NotNil(t, err)
			assert.NotErrorIs(t, err, ErrV3UnacceptableProtocolVersion)
		},
		)
	}
}

func TestConnectReadVersionMissing(t *testing.T) {
	msg := []byte{0, 4, 'M', 'Q', 'T', 'T'}
	opts := options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err := newPacketConnect(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	require.ErrorIs(t, err, ErrV3UnacceptableProtocolVersion)
}

func TestConnectReadVersionInvalid(t *testing.T) {
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 0, 0, 0, 0, // variable header
		0, 1, 'a', // client ID
	}
	opts := options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err := newPacketConnect(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	assert.ErrorIs(t, err, ErrV3UnacceptableProtocolVersion)
}

func TestConnectReadFlagsMissing(t *testing.T) {
	msg := []byte{0, 4, 'M', 'Q', 'T', 'T', 4}
	opts := options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err := newPacketConnect(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	require.ErrorIs(t, err, ErrV5MalformedPacket)
}

func TestConnectReadFlagsReservedInvalid(t *testing.T) {
	// V3.1.1
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 1, 0, 0, // variable header
		0, 1, 'a', // client ID
	}
	opts := options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err := newPacketConnect(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	assert.ErrorIs(t, err, ErrV5MalformedPacket)

	// V5.0
	msg = []byte{
		0, 4, 'M', 'Q', 'T', 'T', 5, 1, 0, 0, // variable header
		0,         // property length
		0, 1, 'a', // client ID
		0, // will property length
	}
	opts = options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err = newPacketConnect(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	assert.ErrorIs(t, err, ErrV5MalformedPacket)
}

func TestConnectReadFlagsWillQoS(t *testing.T) {
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 0x14, 0, 0, // variable header
		0, 1, 'a', // client ID
		0, 1, 't', // will topic
		0, 1, 'm', // will message
	}
	opts := options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err := newPacketConnect(opts)
	require.Nil(t, err)

	require.Equal(t, CONNECT, pkt.Type())
	connPkt, _ := pkt.(*Connect)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	require.Nil(t, err)
	assert.Equal(t, WillQoS2, connPkt.WillQoS)
}

func TestConnectReadFlagsWillQoSInvalid(t *testing.T) {
	// V3.1.1 - No Will Flag
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 0x10, 0, 0, // variable header
		0, 1, 'a', // client ID
	}
	opts := options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err := newPacketConnect(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	assert.ErrorIs(t, err, ErrV5MalformedPacket)

	// V3.1.1 - Invalid Will QoS
	msg = []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 0x1C, 0, 0, // variable header
		0, 1, 'a', // client ID
		0, 1, 't', // will topic
		0, 1, 'm', // will message
	}
	opts = options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err = newPacketConnect(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	assert.ErrorIs(t, err, ErrV5MalformedPacket)

	// V5.0 - No Will Flag
	msg = []byte{
		0, 4, 'M', 'Q', 'T', 'T', 5, 0x10, 0, 0, // variable header
		0,         // property length
		0, 1, 'a', // client ID
		0, // will property length
	}
	opts = options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err = newPacketConnect(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	assert.ErrorIs(t, err, ErrV5MalformedPacket)

	// V5.0 - Invalid Will QoS
	msg = []byte{
		0, 4, 'M', 'Q', 'T', 'T', 5, 0x1C, 0, 0, // variable header
		0,         // property length
		0, 1, 'a', // client ID
		0,         // will property length
		0, 1, 't', // will topic
		0, 1, 'm', // will message
	}
	opts = options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err = newPacketConnect(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	assert.ErrorIs(t, err, ErrV5MalformedPacket)
}

func TestConnectReadFlagsWillRetain(t *testing.T) {
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 0x24, 0, 0, // variable header
		0, 1, 'a', // client ID
		0, 1, 't', // will topic
		0, 1, 'm', // will message
	}
	opts := options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err := newPacketConnect(opts)
	require.Nil(t, err)

	require.Equal(t, CONNECT, pkt.Type())
	connPkt, _ := pkt.(*Connect)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	require.Nil(t, err)
	assert.True(t, connPkt.WillRetain)
}

func TestConnectReadFlagsWillRetainInvalid(t *testing.T) {
	// V3.1.1
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 0x20, 0, 0, // variable header
		0, 1, 'a', // client ID
		0, 1, 't', // will topic
		0, 1, 'm', // will message
	}
	opts := options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err := newPacketConnect(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	assert.ErrorIs(t, err, ErrV5MalformedPacket)

	// V5.0
	msg = []byte{
		0, 4, 'M', 'Q', 'T', 'T', 5, 0x20, 0, 0, // variable header
		0,         // property length
		0, 1, 'a', // client ID
		0,         // will property length
		0, 1, 't', // will topic
		0, 1, 'm', // will message
	}
	opts = options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err = newPacketConnect(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	assert.ErrorIs(t, err, ErrV5MalformedPacket)
}

func TestConnectReadFlagsUserNamePasswordInvalid(t *testing.T) {
	// V3.1.1
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 0x40, 0, 0, // variable header
		0, 1, 'a', // client ID
		0, 1, 'p', // password
	}
	opts := options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err := newPacketConnect(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	assert.ErrorIs(t, err, ErrV5MalformedPacket)

	// V5.0
	msg = []byte{
		0, 4, 'M', 'Q', 'T', 'T', 5, 0x40, 0, 0, // variable header
		0,         // property length
		0, 1, 'a', // client ID
		0,         // will property length
		0, 1, 'p', // password
	}
	opts = options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err = newPacketConnect(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	assert.ErrorIs(t, err, ErrV5MalformedPacket)
}

func TestConnectReadKeepAliveValid(t *testing.T) {
	keepAlive := []uint16{0, 60, 900, 65535}

	for _, ka := range keepAlive {
		msb := byte(ka >> 8)
		lsb := byte(ka & 0xFF)
		msg := []byte{
			0, 4, 'M', 'Q', 'T', 'T', 4, 0, msb, lsb, // variable header
			0, 1, 'a', // client ID
		}
		opts := options{packetType: CONNECT, remainingLength: len(msg)}
		pkt, err := newPacketConnect(opts)
		require.Nil(t, err)

		require.Equal(t, CONNECT, pkt.Type())
		connPkt, _ := pkt.(*Connect)

		err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
		require.Nil(t, err)
		assert.Equal(t, ka, connPkt.KeepAlive)
	}
}

func TestConnectReadKeepAliveInvalid(t *testing.T) {
	// V3.1.1
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, // variable header
	}
	opts := options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err := newPacketConnect(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	assert.NotNil(t, err)

	// V5.0
	msg = []byte{
		0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, // variable header
	}
	opts = options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err = newPacketConnect(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	assert.ErrorIs(t, err, ErrV5MalformedPacket)
}

func TestConnectReadPropertiesValid(t *testing.T) {
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 0, // variable header
		5,               // property length
		17, 0, 0, 0, 10, // SessionExpiryInterval
		0,         // will property length
		0, 1, 'a', // client ID
	}
	opts := options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err := newPacketConnect(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	require.Nil(t, err)

	require.Equal(t, CONNECT, pkt.Type())
	connPkt, _ := pkt.(*Connect)

	require.NotNil(t, connPkt.Properties)
	assert.Equal(t, uint32(10), *connPkt.Properties.SessionExpiryInterval)
}

func TestConnectReadPropertiesMalformed(t *testing.T) {
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 0, // variable header
		2,     // property length
		99, 0, // Invalid
		0,         // will property length
		0, 1, 'a', // client ID
	}
	opts := options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err := newPacketConnect(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	assert.ErrorIs(t, err, ErrV5MalformedPacket)
}

func TestConnectReadClientIDValid(t *testing.T) {
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

		opts := options{packetType: CONNECT, remainingLength: len(msg)}
		pkt, err := newPacketConnect(opts)
		require.Nil(t, err)

		require.Equal(t, CONNECT, pkt.Type())
		connPkt, _ := pkt.(*Connect)

		err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
		require.Nil(t, err)
		assert.Equal(t, cp, connPkt.ClientID)
	}

	// Clear session
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 2, 0, 0, // variable header
		0, 0, // client ID
	}
	opts := options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err := newPacketConnect(opts)
	require.Nil(t, err)

	require.Equal(t, CONNECT, pkt.Type())
	connPkt, _ := pkt.(*Connect)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	require.Nil(t, err)
	assert.Equal(t, []byte{}, connPkt.ClientID)
}

func TestConnectReadClientIDMalformed(t *testing.T) {
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

		opts := options{packetType: CONNECT, remainingLength: len(msg)}
		pkt, err := newPacketConnect(opts)
		require.Nil(t, err)

		err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
		assert.ErrorIs(t, err, ErrV5MalformedPacket)

		// V5.0
		msg = []byte{
			0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 0, // variable header
			0, // property length
		}
		msg = append(msg, id.len...)
		msg = append(msg, id.data...)

		opts = options{packetType: CONNECT, remainingLength: len(msg)}
		pkt, err = newPacketConnect(opts)
		require.Nil(t, err)

		err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
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

		opts := options{packetType: CONNECT, remainingLength: len(msg)}
		pkt, err := newPacketConnect(opts)
		require.Nil(t, err)

		err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
		require.NotNil(t, err)
		assert.ErrorIs(t, err, ErrV5MalformedPacket)

		// V5.0
		msg = []byte{
			0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 0, // variable header
			0, // property length
		}
		msg = append(msg, cpLenBuf...)
		msg = append(msg, cp...)

		opts = options{packetType: CONNECT, remainingLength: len(msg)}
		pkt, err = newPacketConnect(opts)
		require.Nil(t, err)

		err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
		assert.ErrorIs(t, err, ErrV5MalformedPacket)
	}
}

func TestConnectReadClientIDRejected(t *testing.T) {
	// V3.1
	msg := []byte{
		0, 6, 'M', 'Q', 'I', 's', 'd', 'p', 3, 0, 0, 0, // variable header
		0, 0, // client ID
	}
	opts := options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err := newPacketConnect(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	require.NotNil(t, err)
	assert.ErrorIs(t, err, ErrV3IdentifierRejected)

	// V3.1.1
	msg = []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 0, // variable header
		0, 0, // payload
	}
	opts = options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err = newPacketConnect(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	assert.ErrorIs(t, err, ErrV3IdentifierRejected)
}

func TestConnectReadWillPropertiesValid(t *testing.T) {
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 5, 4, 0, 0, // variable header
		0,         // property length
		0, 1, 'a', // client ID
		5,               // will property length
		17, 0, 0, 0, 10, // SessionExpiryInterval
		0, 5, 't', 'o', 'p', 'i', 'c', // will topic
		0, 1, 'm', // will message
	}
	opts := options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err := newPacketConnect(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	require.Nil(t, err)

	require.Equal(t, CONNECT, pkt.Type())
	connPkt, _ := pkt.(*Connect)

	require.NotNil(t, connPkt.WillProperties)
	assert.Equal(t, uint32(10), *connPkt.WillProperties.SessionExpiryInterval)
}

func TestConnectReadWillPropertiesMalformed(t *testing.T) {
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 5, 4, 0, 0, // variable header
		0,         // property length
		0, 1, 'a', // client ID
		2,     // will property length
		99, 0, // invalid
		0, 5, 't', 'o', 'p', 'i', 'c', // will topic
		0, 1, 'm', // will message
	}
	opts := options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err := newPacketConnect(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	assert.ErrorIs(t, err, ErrV5MalformedPacket)
}

func TestConnectReadWillTopicValid(t *testing.T) {
	testCases := []string{"topic", "dev/client-1/will"}

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			msg := []byte{
				0, 4, 'M', 'Q', 'T', 'T', 4, 4, 0, 0, // variable header
				0, 1, 'a', // client ID
			}

			buf := []byte(tc)
			wtLenMSB := byte(len(tc) >> 8)
			wtLenLSB := byte(len(tc) & 0xFF)
			msg = append(msg, wtLenMSB, wtLenLSB)
			msg = append(msg, buf...)
			msg = append(msg, 0, 1, 'm') // will message

			opts := options{packetType: CONNECT, remainingLength: len(msg)}
			pkt, err := newPacketConnect(opts)
			require.Nil(t, err)

			require.Equal(t, CONNECT, pkt.Type())
			connPkt, _ := pkt.(*Connect)

			err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
			require.Nil(t, err)
			assert.Equal(t, buf, connPkt.WillTopic)
		})
	}
}

func TestConnectReadWillTopicMissing(t *testing.T) {
	// V3.1.1
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 4, 0, 0, // variable header
		0, 1, 'a', // client ID
	}
	opts := options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err := newPacketConnect(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	assert.ErrorIs(t, err, ErrV5MalformedPacket)

	// V5.0
	msg = []byte{
		0, 4, 'M', 'Q', 'T', 'T', 5, 4, 0, 0, // variable header
		0,         // property length
		0, 1, 'a', // client ID
		0, // will property length
	}

	opts = options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err = newPacketConnect(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	assert.ErrorIs(t, err, ErrV5MalformedPacket)
}

func TestConnectReadWillMessageValid(t *testing.T) {
	testCases := []string{"", "hello"}

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			msg := []byte{
				0, 4, 'M', 'Q', 'T', 'T', 4, 4, 0, 0, // variable header
				0, 1, 'a', // client ID
				0, 1, 't', // will topic
			}

			buf := []byte(tc)
			wmLenMSB := byte(len(tc) >> 8)
			wmLenLSB := byte(len(tc) & 0xFF)
			msg = append(msg, wmLenMSB, wmLenLSB)
			msg = append(msg, buf...)

			opts := options{packetType: CONNECT, remainingLength: len(msg)}
			pkt, err := newPacketConnect(opts)
			require.Nil(t, err)

			require.Equal(t, CONNECT, pkt.Type())
			connPkt, _ := pkt.(*Connect)

			err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
			require.Nil(t, err)
			assert.Equal(t, buf, connPkt.WillMessage)
		})
	}
}

func TestConnectReadWillMessageMissing(t *testing.T) {
	// V3.1.1
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 4, 0, 0, // variable header
		0, 1, 'a', // client ID
		0, 1, 't', // will topic
	}

	opts := options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err := newPacketConnect(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	assert.ErrorIs(t, err, ErrV5MalformedPacket)

	// V5.0
	msg = []byte{
		0, 4, 'M', 'Q', 'T', 'T', 5, 4, 0, 0, // variable header
		0,         // property length
		0, 1, 'a', // client ID
		0,         // will property length
		0, 1, 't', // will topic
	}

	opts = options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err = newPacketConnect(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	assert.ErrorIs(t, err, ErrV5MalformedPacket)
}

func TestConnectReadUserNameValid(t *testing.T) {
	testCases := []string{"", "username"}

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			msg := []byte{
				0, 4, 'M', 'Q', 'T', 'T', 4, 0x80, 0, 0, // variable header
				0, 1, 'a', // client ID
			}

			buf := []byte(tc)
			nLenMSB := byte(len(tc) >> 8)
			nLenLSB := byte(len(tc) & 0xFF)
			msg = append(msg, nLenMSB, nLenLSB)
			msg = append(msg, buf...)

			opts := options{packetType: CONNECT, remainingLength: len(msg)}
			pkt, err := newPacketConnect(opts)
			require.Nil(t, err)

			require.Equal(t, CONNECT, pkt.Type())
			connPkt, _ := pkt.(*Connect)

			err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
			require.Nil(t, err)
			assert.Equal(t, buf, connPkt.UserName)
		})
	}
}

func TestConnectReadUserNameMissing(t *testing.T) {
	// v3.1.1
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 0x80, 0, 0, // variable header
		0, 1, 'a', // client ID
	}

	opts := options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err := newPacketConnect(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	assert.ErrorIs(t, err, ErrV5MalformedPacket)

	// v5.0
	msg = []byte{
		0, 4, 'M', 'Q', 'T', 'T', 5, 0x80, 0, 0, // variable header
		0,         // property length
		0, 1, 'a', // client ID
		0, // will property length
	}

	opts = options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err = newPacketConnect(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	assert.ErrorIs(t, err, ErrV5MalformedPacket)
}

func TestConnectReadPasswordValid(t *testing.T) {
	testCases := []string{"", "password"}

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			msg := []byte{
				0, 4, 'M', 'Q', 'T', 'T', 4, 0xC0, 0, 0, // variable header
				0, 1, 'a', // client ID
				0, 1, 'u', // username
			}

			buf := []byte(tc)
			pLenMSB := byte(len(tc) >> 8)
			pLenLSB := byte(len(tc) & 0xFF)
			msg = append(msg, pLenMSB, pLenLSB)
			msg = append(msg, buf...)

			opts := options{packetType: CONNECT, remainingLength: len(msg)}
			pkt, err := newPacketConnect(opts)
			require.Nil(t, err)

			require.Equal(t, CONNECT, pkt.Type())
			connPkt, _ := pkt.(*Connect)

			err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
			require.Nil(t, err)
			assert.Equal(t, buf, connPkt.Password)
		})
	}
}

func TestConnectReadPasswordInvalid(t *testing.T) {
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 0xC0, 0, 0, // variable header
		0, 1, 'a', // client ID
		0, 1, 'u', // username
	}

	opts := options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err := newPacketConnect(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	assert.ErrorIs(t, err, ErrV5MalformedPacket)
}

func TestConnectSize(t *testing.T) {
	t.Run("V3", func(t *testing.T) {
		msg := []byte{
			0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 0, // variable header
			0, 1, 'a', // client ID
		}

		opts := options{
			packetType:        CONNECT,
			remainingLength:   len(msg),
			fixedHeaderLength: 2,
		}
		pkt, err := newPacketConnect(opts)
		require.Nil(t, err)
		require.NotNil(t, pkt)

		assert.Equal(t, 15, pkt.Size())
	})

	t.Run("V5", func(t *testing.T) {
		msg := []byte{
			0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 0, // variable header
			0,         // property length
			0, 1, 'a', // client ID
			0, // will property length
		}

		opts := options{
			packetType:        CONNECT,
			remainingLength:   len(msg),
			fixedHeaderLength: 2,
		}
		pkt, err := newPacketConnect(opts)
		require.Nil(t, err)
		require.NotNil(t, pkt)

		assert.Equal(t, 17, pkt.Size())
	})

	t.Run("V5-Properties", func(t *testing.T) {
		msg := []byte{
			0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 0, // variable header
			5,               // property length
			17, 0, 0, 0, 10, // SessionExpiryInterval
			0, 1, 'a', // client ID
			0, // will property length
		}

		opts := options{
			packetType:        CONNECT,
			remainingLength:   len(msg),
			fixedHeaderLength: 2,
		}
		pkt, err := newPacketConnect(opts)
		require.Nil(t, err)
		require.NotNil(t, pkt)

		assert.Equal(t, 22, pkt.Size())
	})
}

func TestConnectTimestamp(t *testing.T) {
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 60, // variable header
		0, 1, 'a', // client ID
	}
	opts := options{packetType: CONNECT, remainingLength: len(msg)}
	pkt, err := newPacketConnect(opts)
	require.Nil(t, err)
	require.NotNil(t, pkt)

	assert.NotNil(t, pkt.Timestamp())
}
