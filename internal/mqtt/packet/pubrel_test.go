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
	"math"
	"net"
	"strconv"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPubRelInvalidPacketType(t *testing.T) {
	opts := options{packetType: DISCONNECT}
	pkt, err := newPacketPubRel(opts)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestPubRelInvalidControlFlags(t *testing.T) {
	opts := options{packetType: PUBREL, version: MQTT311, remainingLength: 2}
	pkt, err := newPacketPubRel(opts)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestPubRelInvalidVersion(t *testing.T) {
	opts := options{packetType: PUBREL, controlFlags: 2, remainingLength: 2}
	pkt, err := newPacketPubRel(opts)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestPulRecInvalidLength(t *testing.T) {
	testCases := []struct {
		name    string
		version MQTTVersion
		length  int
	}{
		{name: "V3.1-TooShort", version: MQTT31, length: 0},
		{name: "V3.1.1-TooShort", version: MQTT311, length: 1},
		{name: "V3.1.1-TooLong", version: MQTT311, length: 3},
		{name: "V5.0", version: MQTT50, length: 1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opts := options{packetType: PUBREL, controlFlags: 2,
				version: tc.version, remainingLength: tc.length}

			pkt, err := newPacketPubRel(opts)
			require.NotNil(t, err)
			require.Nil(t, pkt)
		})
	}
}

func TestPubRelWrite(t *testing.T) {
	testCases := []struct {
		name    string
		id      ID
		version MQTTVersion
		code    ReasonCode
		props   *Properties
		msg     []byte
	}{
		{
			name:    "V3.1",
			id:      0x01,
			version: MQTT31,
			msg:     []byte{0x62, 2, 0, 1},
		},
		{
			name:    "V3.1.1",
			id:      0xFF,
			version: MQTT311,
			msg:     []byte{0x62, 2, 0, 0xFF},
		},
		{
			name:    "V5.0-Success",
			id:      0x100,
			version: MQTT50,
			msg:     []byte{0x62, 2, 1, 0},
		},
		{
			name:    "V5.0-PacketIDNotFound",
			id:      0x01FF,
			version: MQTT50,
			code:    ReasonCodeV5PacketIDNotFound,
			msg:     []byte{0x62, 4, 1, 0xFF, 0x92, 0},
		},
		{
			name:    "V5.0-Properties",
			id:      0xFFFE,
			version: MQTT50,
			code:    ReasonCodeV5Success,
			props:   &Properties{ReasonString: []byte{'a'}},
			msg:     []byte{0x62, 8, 0xFF, 0xFE, 0, 4, 0x1F, 0, 1, 'a'},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pkt := NewPubRel(tc.id, tc.version, tc.code, tc.props)
			assert.Equal(t, PUBREL, pkt.Type())

			buf := &bytes.Buffer{}
			wr := bufio.NewWriter(buf)

			err := pkt.Write(wr)
			require.Nil(t, err)

			err = wr.Flush()
			assert.Nil(t, err)
			assert.Equal(t, tc.msg, buf.Bytes())
		})
	}
}

func BenchmarkPubRelWriteV3(b *testing.B) {
	b.ReportAllocs()
	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)
	pkt := NewPubRel(4, MQTT311, ReasonCodeV5Success, nil)

	for n := 0; n < b.N; n++ {
		buf.Reset()

		err := pkt.Write(wr)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPubRelWriteV5(b *testing.B) {
	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)
	pkt := NewPubRel(4, MQTT50, ReasonCodeV5Success, nil)

	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		buf.Reset()

		err := pkt.Write(wr)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestPubRelWriteFailure(t *testing.T) {
	pkt := NewPubRel(5, MQTT50, ReasonCodeV5Success, nil)
	require.NotNil(t, pkt)

	conn, _ := net.Pipe()
	w := bufio.NewWriterSize(conn, 1)
	_ = conn.Close()

	err := pkt.Write(w)
	assert.NotNil(t, err)
}

func TestPubRelWriteV5InvalidProperty(t *testing.T) {
	props := &Properties{TopicAlias: new(uint16)}
	*props.TopicAlias = 10

	pkt := NewPubRel(5, MQTT50, ReasonCodeV5Success, props)
	require.NotNil(t, pkt)

	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)

	err := pkt.Write(wr)
	assert.NotNil(t, err)

	err = wr.Flush()
	assert.Nil(t, err)
	assert.Empty(t, buf)
}

func TestPubRelWriteV5InvalidReasonCode(t *testing.T) {
	validCodes := []ReasonCode{ReasonCodeV5Success,
		ReasonCodeV5PacketIDNotFound}

	isValidReasonCode := func(code int) bool {
		for _, c := range validCodes {
			if int(c) == code {
				return true
			}
		}
		return false
	}

	reasonCodeSize := int(math.Pow(2,
		float64(unsafe.Sizeof(ReasonCodeV5Success))*8))

	for code := 0; code < reasonCodeSize; code++ {
		if !isValidReasonCode(code) {
			t.Run(strconv.Itoa(code), func(t *testing.T) {
				pkt := NewPubRel(5, MQTT50, ReasonCode(code), nil)
				require.NotNil(t, pkt)

				buf := &bytes.Buffer{}
				wr := bufio.NewWriter(buf)

				err := pkt.Write(wr)
				require.NotNil(t, err)
			})
		}
	}
}

func TestPubRelRead(t *testing.T) {
	testCases := []struct {
		name    string
		version MQTTVersion
		msg     []byte
		id      ID
		code    ReasonCode
		props   *Properties
	}{
		{name: "V3.1", version: MQTT31, msg: []byte{0, 1}, id: 1},
		{name: "V3.1.1", version: MQTT311, msg: []byte{1, 0}, id: 0x100},
		{name: "V5.0-Success", version: MQTT50, msg: []byte{1, 0xFF},
			id: 0x1FF, code: ReasonCodeV5Success},
		{name: "V5.0-PacketIDNotFound", version: MQTT50,
			msg: []byte{1, 0xFF, 0x92, 0}, id: 0x1FF,
			code: ReasonCodeV5PacketIDNotFound},
		{name: "V5.0-Properties", version: MQTT50,
			msg: []byte{0xFF, 0xFE, 0, 8, 0x1F, 0, 5, 'H', 'e', 'l', 'l', 'o'},
			id:  0xFFFE, code: ReasonCodeV5Success,
			props: &Properties{ReasonString: []byte("Hello")}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opts := options{packetType: PUBREL, version: tc.version,
				controlFlags: 2, remainingLength: len(tc.msg)}
			pkt, err := newPacketPubRel(opts)
			require.Nil(t, err)
			require.NotNil(t, pkt)
			require.Equal(t, PUBREL, pkt.Type())

			err = pkt.Read(bufio.NewReader(bytes.NewBuffer(tc.msg)))
			require.Nil(t, err)

			pubRel := pkt.(*PubRel)
			assert.Equal(t, tc.version, pubRel.Version)
			assert.Equal(t, tc.id, pubRel.PacketID)
			assert.Equal(t, tc.code, pubRel.ReasonCode)
			assert.Equal(t, tc.props, pubRel.Properties)
		})
	}
}

func BenchmarkPubRelReadV3(b *testing.B) {
	b.ReportAllocs()
	msg := []byte{0, 1}
	opts := options{packetType: PUBREL, version: MQTT311, controlFlags: 2,
		remainingLength: len(msg)}
	pkt, _ := newPacketPubRel(opts)
	rd := bufio.NewReaderSize(nil, len(msg))

	for n := 0; n < b.N; n++ {
		buf := bytes.NewBuffer(msg)
		rd.Reset(buf)

		err := pkt.Read(rd)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPubRelReadV5(b *testing.B) {
	b.ReportAllocs()
	msg := []byte{0, 1, 0, 0}
	opts := options{packetType: PUBREL, version: MQTT50, controlFlags: 2,
		remainingLength: len(msg)}
	pkt, _ := newPacketPubRel(opts)
	rd := bufio.NewReaderSize(nil, len(msg))

	for n := 0; n < b.N; n++ {
		buf := bytes.NewBuffer(msg)
		rd.Reset(buf)

		err := pkt.Read(rd)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestPubRelReadMissingData(t *testing.T) {
	testCases := []struct {
		name   string
		length int
		msg    []byte
	}{
		{name: "LengthGreaterThanMsg", length: 10},
		{name: "MissingPropertiesLength", length: 3, msg: []byte{0, 1, 0x92}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opts := options{packetType: PUBREL, version: MQTT50,
				controlFlags: 2, remainingLength: tc.length}
			pkt, err := newPacketPubRel(opts)
			require.Nil(t, err)

			err = pkt.Read(bufio.NewReader(bytes.NewBuffer(tc.msg)))
			require.NotNil(t, err)
		})
	}
}

func TestPubRelReadV5InvalidReasonCode(t *testing.T) {
	validCodes := []ReasonCode{ReasonCodeV5Success,
		ReasonCodeV5PacketIDNotFound}

	isValidReasonCode := func(code int) bool {
		for _, c := range validCodes {
			if int(c) == code {
				return true
			}
		}
		return false
	}

	reasonCodeSize := int(math.Pow(2,
		float64(unsafe.Sizeof(ReasonCodeV5Success))*8))

	for code := 0; code < reasonCodeSize; code++ {
		if !isValidReasonCode(code) {
			t.Run(strconv.Itoa(code), func(t *testing.T) {
				msg := []byte{1, 0xFF, byte(code), 0}

				opts := options{packetType: PUBREL, version: MQTT50,
					controlFlags: 2, remainingLength: len(msg)}
				pkt, err := newPacketPubRel(opts)
				require.Nil(t, err)
				require.NotNil(t, pkt)

				err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
				require.NotNil(t, err)
			})
		}
	}
}

func TestPubRelSize(t *testing.T) {
	testCases := []struct {
		name    string
		version MQTTVersion
		code    ReasonCode
		props   *Properties
		size    int
	}{
		{name: "V3.1", version: MQTT31, code: ReasonCodeV5Success, size: 4},
		{name: "V3.1.1", version: MQTT311, code: ReasonCodeV5Success, size: 4},
		{name: "V5.0-Success", version: MQTT50, code: ReasonCodeV5Success,
			size: 4},
		{name: "V5.0-NoSuccess", version: MQTT50,
			code: ReasonCodeV5PacketIDNotFound, size: 6},
		{name: "V5.0-Properties", version: MQTT50,
			props: &Properties{ReasonString: []byte{'a'}},
			code:  ReasonCodeV5Success, size: 10},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pkt := NewPubRel(1, tc.version, tc.code, tc.props)
			require.NotNil(t, pkt)

			buf := &bytes.Buffer{}
			wr := bufio.NewWriter(buf)

			err := pkt.Write(wr)
			require.Nil(t, err)
			assert.Equal(t, tc.size, pkt.Size())
		})
	}
}

func TestPubRelTimestamp(t *testing.T) {
	pkt := NewPubRel(4, MQTT50, ReasonCodeV5Success, nil)
	require.NotNil(t, pkt)
	assert.NotNil(t, pkt.Timestamp())
}
