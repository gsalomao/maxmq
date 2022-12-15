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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPubAckInvalidPacketType(t *testing.T) {
	opts := options{packetType: DISCONNECT, version: MQTT311}
	pkt, err := newPacketPubAck(opts)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestPubAckInvalidControlFlags(t *testing.T) {
	opts := options{packetType: PUBACK, controlFlags: 1, version: MQTT50}
	pkt, err := newPacketPubAck(opts)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestPubAckInvalidVersion(t *testing.T) {
	opts := options{packetType: PUBACK}
	pkt, err := newPacketPubAck(opts)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestPubAckInvalidLength(t *testing.T) {
	testCases := []struct {
		name    string
		version MQTTVersion
		length  int
	}{
		{name: "V3.1", version: MQTT31, length: 0},
		{name: "V3.1.1", version: MQTT311, length: 1},
		{name: "V5.0", version: MQTT50, length: 3},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			opts := options{packetType: PUBACK, version: test.version,
				remainingLength: test.length}

			pkt, err := newPacketPubAck(opts)
			require.NotNil(t, err)
			require.Nil(t, pkt)
		})
	}
}

func TestPubAckWrite(t *testing.T) {
	tests := []struct {
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
			msg:     []byte{0x40, 2, 0, 1},
		},
		{
			name:    "V3.1.1",
			id:      0xFF,
			version: MQTT311,
			msg:     []byte{0x40, 2, 0, 0xFF},
		},
		{
			name:    "V5.0-Success",
			id:      0x100,
			version: MQTT50,
			code:    ReasonCodeV5Success,
			msg:     []byte{0x40, 2, 1, 0},
		},
		{
			name:    "V5.0-NoMatchingSubscribers",
			id:      0x01FF,
			version: MQTT50,
			code:    ReasonCodeV5NoMatchingSubscribers,
			msg:     []byte{0x40, 4, 1, 0xFF, 0x10, 0},
		},
		{
			name:    "V5.0-Properties",
			id:      0xFFFE,
			version: MQTT50,
			code:    ReasonCodeV5UnspecifiedError,
			props:   &Properties{ReasonString: []byte{'a'}},
			msg:     []byte{0x40, 8, 0xFF, 0xFE, 0x80, 4, 0x1F, 0, 1, 'a'},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pkt := NewPubAck(test.id, test.version, test.code, test.props)
			assert.Equal(t, PUBACK, pkt.Type())

			buf := &bytes.Buffer{}
			wr := bufio.NewWriter(buf)

			err := pkt.Write(wr)
			assert.Nil(t, err)

			err = wr.Flush()
			assert.Nil(t, err)

			assert.Equal(t, test.msg, buf.Bytes())
		})
	}
}

func BenchmarkPubAckWriteV3(b *testing.B) {
	b.ReportAllocs()
	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)
	pkt := NewPubAck(4, MQTT311, ReasonCodeV5Success, nil)

	for n := 0; n < b.N; n++ {
		buf.Reset()

		err := pkt.Write(wr)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPubAckWriteV5(b *testing.B) {
	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)
	pkt := NewPubAck(4, MQTT50, ReasonCodeV5Success, nil)

	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		buf.Reset()

		err := pkt.Write(wr)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestPubAckWriteV5InvalidProperty(t *testing.T) {
	props := &Properties{TopicAlias: new(uint16)}
	*props.TopicAlias = 10

	pkt := NewPubAck(5, MQTT50, ReasonCodeV5Success, props)
	require.NotNil(t, pkt)

	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)

	err := pkt.Write(wr)
	assert.NotNil(t, err)

	err = wr.Flush()
	assert.Nil(t, err)
	assert.Empty(t, buf)
}

func TestPubAckRead(t *testing.T) {
	testCases := []struct {
		id        ID
		version   MQTTVersion
		code      ReasonCode
		withProps bool
		msg       []byte
	}{
		{id: 1, version: MQTT31, msg: []byte{0, 1}},
		{id: 2, version: MQTT311, msg: []byte{0, 2}},
		{id: 3, version: MQTT50, code: ReasonCodeV5Success,
			msg: []byte{0, 3, 0, 0}},
		{id: 4, version: MQTT50, code: ReasonCodeV5Success,
			msg: []byte{0, 4, 0, 0}},
		{id: 5, version: MQTT50, code: ReasonCodeV5NoMatchingSubscribers,
			msg: []byte{0, 5, 16, 0}},
		{id: 6, version: MQTT50, code: ReasonCodeV5NoMatchingSubscribers,
			withProps: true, msg: []byte{0, 6, 16, 6, 31, 0, 3, 'a', 'b', 'c'}},
	}

	for _, test := range testCases {
		name := fmt.Sprintf("%v-%v", test.version, test.id)
		t.Run(name, func(t *testing.T) {
			opts := options{
				packetType:      PUBACK,
				version:         test.version,
				remainingLength: len(test.msg),
			}
			pkt, err := newPacketPubAck(opts)
			require.Nil(t, err)

			require.Equal(t, PUBACK, pkt.Type())
			pubAckPkt, _ := pkt.(*PubAck)

			err = pkt.Read(bufio.NewReader(bytes.NewBuffer(test.msg)))
			require.Nil(t, err)

			assert.Equal(t, test.version, pubAckPkt.Version)
			assert.Equal(t, test.id, pubAckPkt.PacketID)
			if test.withProps {
				require.NotNil(t, pubAckPkt.Properties)
				assert.Equal(t, []byte("abc"),
					pubAckPkt.Properties.ReasonString)
			} else {
				assert.Nil(t, pubAckPkt.Properties)
			}
		})
	}
}

func BenchmarkPubAckReadV3(b *testing.B) {
	b.ReportAllocs()
	msg := []byte{0, 1}
	opts := options{
		packetType:      PUBACK,
		version:         MQTT311,
		remainingLength: len(msg),
	}
	pkt, _ := newPacketPubAck(opts)
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

func BenchmarkPubAckReadV5(b *testing.B) {
	b.ReportAllocs()
	msg := []byte{0, 1, 16, 0}
	opts := options{
		packetType:      PUBACK,
		version:         MQTT50,
		remainingLength: len(msg),
	}
	pkt, _ := newPacketPubAck(opts)
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

func TestPubAckReadMissingData(t *testing.T) {
	var msg []byte
	opts := options{
		packetType:      PUBACK,
		version:         MQTT50,
		remainingLength: 10,
	}
	pkt, err := newPacketPubAck(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	require.NotNil(t, err)
}

func TestPubAckSize(t *testing.T) {
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
			code: ReasonCodeV5NoMatchingSubscribers, size: 6},
		{name: "V5.0-Properties", version: MQTT50,
			props: &Properties{ReasonString: []byte{'a'}},
			code:  ReasonCodeV5NoMatchingSubscribers, size: 10},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			pkt := NewPubAck(1, test.version, test.code, test.props)
			require.NotNil(t, pkt)

			buf := &bytes.Buffer{}
			wr := bufio.NewWriter(buf)

			err := pkt.Write(wr)
			require.Nil(t, err)

			assert.Equal(t, test.size, pkt.Size())
		})
	}
}

func TestPubAckTimestamp(t *testing.T) {
	pkt := NewPubAck(4, MQTT50, ReasonCodeV5Success, nil)
	require.NotNil(t, pkt)
	assert.NotNil(t, pkt.Timestamp())
}
