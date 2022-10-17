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

func TestPubAck_InvalidPacketType(t *testing.T) {
	opts := options{packetType: DISCONNECT, version: MQTT311}
	pkt, err := newPacketPubAck(opts)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestPubAck_InvalidControlFlags(t *testing.T) {
	opts := options{packetType: PUBACK, controlFlags: 1, version: MQTT50}
	pkt, err := newPacketPubAck(opts)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestPubAck_InvalidVersion(t *testing.T) {
	opts := options{packetType: PUBACK}
	pkt, err := newPacketPubAck(opts)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestPubAck_Pack(t *testing.T) {
	tests := []struct {
		id      ID
		version MQTTVersion
		code    ReasonCode
		msg     []byte
	}{
		{
			id:      1,
			version: MQTT31,
			msg:     []byte{0x40, 2, 0, 1},
		},
		{
			id:      2,
			version: MQTT311,
			msg:     []byte{0x40, 2, 0, 2},
		},
		{
			id:      3,
			version: MQTT50,
			msg:     []byte{0x40, 2, 0, 3},
		},
		{
			id:      4,
			version: MQTT50,
			code:    ReasonCodeV5NoMatchingSubscribers,
			msg:     []byte{0x40, 4, 0, 4, 16, 0},
		},
	}

	for _, test := range tests {
		name := fmt.Sprintf("%v-%v", test.version, test.id)
		t.Run(name, func(t *testing.T) {
			pkt := NewPubAck(test.id, test.version, test.code, nil)
			assert.Equal(t, PUBACK, pkt.Type())

			buf := &bytes.Buffer{}
			wr := bufio.NewWriter(buf)

			err := pkt.Pack(wr)
			assert.Nil(t, err)

			err = wr.Flush()
			assert.Nil(t, err)

			assert.Equal(t, test.msg, buf.Bytes())
		})
	}
}

func BenchmarkPubAck_PackV3(b *testing.B) {
	b.ReportAllocs()
	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)
	pkt := NewPubAck(4, MQTT311, ReasonCodeV5Success, nil)

	for n := 0; n < b.N; n++ {
		buf.Reset()

		err := pkt.Pack(wr)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPubAck_PackV5(b *testing.B) {
	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)
	pkt := NewPubAck(4, MQTT50, ReasonCodeV5Success, nil)

	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		buf.Reset()

		err := pkt.Pack(wr)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestPubAck_PackV5Properties(t *testing.T) {
	props := &Properties{}
	props.ReasonString = []byte("abc")

	pkt := NewPubAck(5, MQTT50, ReasonCodeV5Success, props)
	require.NotNil(t, pkt)

	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)

	err := pkt.Pack(wr)
	assert.Nil(t, err)

	err = wr.Flush()
	assert.Nil(t, err)

	msg := []byte{0x40, 10, 0, 5, 0, 6, 31, 0, 3, 'a', 'b', 'c'}
	assert.Equal(t, msg, buf.Bytes())
}

func TestPubAck_PackV5InvalidProperty(t *testing.T) {
	props := &Properties{TopicAlias: new(uint16)}
	*props.TopicAlias = 10

	pkt := NewPubAck(5, MQTT50, ReasonCodeV5Success, props)
	require.NotNil(t, pkt)

	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)

	err := pkt.Pack(wr)
	assert.NotNil(t, err)

	err = wr.Flush()
	assert.Nil(t, err)
	assert.Empty(t, buf)
}

func TestPubAck_Unpack(t *testing.T) {
	testCases := []struct {
		id        ID
		version   MQTTVersion
		code      ReasonCode
		withProps bool
		msg       []byte
	}{
		{id: 1, version: MQTT31, msg: []byte{0, 1}},
		{id: 2, version: MQTT311, msg: []byte{0, 2}},
		{id: 3, version: MQTT50, code: ReasonCodeV5Success, msg: []byte{0, 3}},
		{id: 4, version: MQTT50, code: ReasonCodeV5NoMatchingSubscribers,
			msg: []byte{0, 4, 16, 0}},
		{id: 5, version: MQTT50, code: ReasonCodeV5NoMatchingSubscribers,
			withProps: true, msg: []byte{0, 5, 16, 6, 31, 0, 3, 'a', 'b', 'c'}},
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

			err = pkt.Unpack(bufio.NewReader(bytes.NewBuffer(test.msg)))
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

func BenchmarkPubAck_UnpackV3(b *testing.B) {
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

		err := pkt.Unpack(rd)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPubAck_UnpackV5(b *testing.B) {
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

		err := pkt.Unpack(rd)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestPubAck_UnpackInvalidLength(t *testing.T) {
	var msg []byte
	opts := options{
		packetType:      PUBACK,
		version:         MQTT311,
		remainingLength: 10,
	}
	pkt, err := newPacketPubAck(opts)
	require.Nil(t, err)

	err = pkt.Unpack(bufio.NewReader(bytes.NewBuffer(msg)))
	require.NotNil(t, err)
}

func TestPubAck_UnpackNoPacketID(t *testing.T) {
	var msg []byte
	opts := options{
		packetType:      PUBACK,
		version:         MQTT311,
		remainingLength: len(msg),
	}
	pkt, err := newPacketPubAck(opts)
	require.Nil(t, err)

	err = pkt.Unpack(bufio.NewReader(bytes.NewBuffer(msg)))
	require.NotNil(t, err)
}

func TestPubAck_Size(t *testing.T) {
	t.Run("Unknown", func(t *testing.T) {
		pkt := NewPubAck(4, MQTT311, ReasonCodeV5Success, nil)
		require.NotNil(t, pkt)
		assert.Equal(t, 0, pkt.Size())
	})

	t.Run("V3", func(t *testing.T) {
		pkt := NewPubAck(4, MQTT311, ReasonCodeV5Success, nil)
		require.NotNil(t, pkt)

		buf := &bytes.Buffer{}
		wr := bufio.NewWriter(buf)

		err := pkt.Pack(wr)
		require.Nil(t, err)

		assert.Equal(t, 4, pkt.Size())
	})

	t.Run("V5", func(t *testing.T) {
		pkt := NewPubAck(4, MQTT50, ReasonCodeV5Success, nil)
		require.NotNil(t, pkt)

		buf := &bytes.Buffer{}
		wr := bufio.NewWriter(buf)

		err := pkt.Pack(wr)
		require.Nil(t, err)

		assert.Equal(t, 4, pkt.Size())
	})

	t.Run("V5-ReasonCode", func(t *testing.T) {
		pkt := NewPubAck(4, MQTT50, ReasonCodeV5NoMatchingSubscribers, nil)
		require.NotNil(t, pkt)

		buf := &bytes.Buffer{}
		wr := bufio.NewWriter(buf)

		err := pkt.Pack(wr)
		require.Nil(t, err)

		assert.Equal(t, 6, pkt.Size())
	})

	t.Run("V5-Properties", func(t *testing.T) {
		props := &Properties{}
		props.ReasonString = []byte("abc")

		pkt := NewPubAck(5, MQTT50, ReasonCodeV5Success, props)
		require.NotNil(t, pkt)

		buf := &bytes.Buffer{}
		wr := bufio.NewWriter(buf)

		err := pkt.Pack(wr)
		require.Nil(t, err)

		assert.Equal(t, 12, pkt.Size())
	})
}

func TestPubAck_Timestamp(t *testing.T) {
	pkt := NewPubAck(4, MQTT50, ReasonCodeV5Success, nil)
	require.NotNil(t, pkt)
	assert.NotNil(t, pkt.Timestamp())
}
