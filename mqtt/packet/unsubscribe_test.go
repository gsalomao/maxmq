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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnsubscribe_InvalidPacketType(t *testing.T) {
	opts := options{packetType: DISCONNECT}
	pkt, err := newPacketUnsubscribe(opts)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestUnsubscribe_InvalidControlFlags(t *testing.T) {
	opts := options{packetType: UNSUBSCRIBE, controlFlags: 0}
	pkt, err := newPacketUnsubscribe(opts)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestUnsubscribe_InvalidVersion(t *testing.T) {
	opts := options{packetType: UNSUBSCRIBE, controlFlags: 2}
	pkt, err := newPacketUnsubscribe(opts)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestUnsubscribe_PackUnsupported(t *testing.T) {
	opts := options{packetType: UNSUBSCRIBE, controlFlags: 2, version: MQTT311}
	pkt, err := newPacketUnsubscribe(opts)
	require.Nil(t, err)
	require.NotNil(t, pkt)

	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)
	err = pkt.Pack(wr)
	require.NotNil(t, err)
}

func TestUnsubscribe_UnpackV3(t *testing.T) {
	msg := []byte{
		0, 10, // packet ID
		0, 3, 'a', '/', 'b', // topic filter
		0, 5, 'c', '/', 'd', '/', 'e', // topic filter
		0, 3, 'a', '/', '#', // topic filter
	}
	opts := options{
		packetType:      UNSUBSCRIBE,
		controlFlags:    2,
		version:         MQTT311,
		remainingLength: len(msg),
	}

	pkt, err := newPacketUnsubscribe(opts)
	require.Nil(t, err)

	require.Equal(t, UNSUBSCRIBE, pkt.Type())
	unsubPkt, _ := pkt.(*Unsubscribe)

	err = pkt.Unpack(bufio.NewReader(bytes.NewBuffer(msg)))
	require.Nil(t, err)

	assert.Equal(t, MQTT311, unsubPkt.Version)
	assert.Equal(t, ID(10), unsubPkt.PacketID)
	require.Len(t, unsubPkt.Topics, 3)
	assert.Equal(t, []byte{'a', '/', 'b'}, unsubPkt.Topics[0])
	assert.Equal(t, []byte{'c', '/', 'd', '/', 'e'}, unsubPkt.Topics[1])
	assert.Equal(t, []byte{'a', '/', '#'}, unsubPkt.Topics[2])
	assert.Nil(t, unsubPkt.Properties)
}

func BenchmarkUnsubscribe_UnpackV3(b *testing.B) {
	msg := []byte{
		0, 10, // packet ID
		0, 3, 'a', '/', 'b', // topic filter
		0, 5, 'c', '/', 'd', '/', 'e', // topic filter
		0, 3, 'a', '/', '#', // topic filter
	}
	opts := options{
		packetType:      UNSUBSCRIBE,
		controlFlags:    2,
		version:         MQTT311,
		remainingLength: len(msg),
	}
	pkt, _ := newPacketUnsubscribe(opts)
	buf := bytes.NewBuffer(msg)
	rd := bufio.NewReader(buf)

	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		err := pkt.Unpack(rd)
		if err != nil {
			b.Fatal(err)
		}

		buf = bytes.NewBuffer(msg)
		rd.Reset(buf)
	}
}

func TestUnsubscribe_UnpackV5(t *testing.T) {
	msg := []byte{
		0, 25, // packet ID
		0,                   // property length
		0, 3, 'a', '/', 'b', // topic filter
	}
	opts := options{
		packetType:      UNSUBSCRIBE,
		controlFlags:    2,
		version:         MQTT50,
		remainingLength: len(msg),
	}
	pkt, err := newPacketUnsubscribe(opts)
	require.Nil(t, err)

	require.Equal(t, UNSUBSCRIBE, pkt.Type())
	unsubPkt, _ := pkt.(*Unsubscribe)

	err = pkt.Unpack(bufio.NewReader(bytes.NewBuffer(msg)))
	require.Nil(t, err)

	assert.Equal(t, MQTT50, unsubPkt.Version)
	assert.Equal(t, ID(25), unsubPkt.PacketID)
	require.Len(t, unsubPkt.Topics, 1)
	assert.Equal(t, []byte{'a', '/', 'b'}, unsubPkt.Topics[0])
}

func BenchmarkUnsubscribe_UnpackV5(b *testing.B) {
	msg := []byte{
		0, 25, // packet ID
		0,                   // property length
		0, 3, 'a', '/', 'b', // topic filter
	}
	opts := options{
		packetType:      UNSUBSCRIBE,
		controlFlags:    2,
		version:         MQTT50,
		remainingLength: len(msg),
	}
	pkt, _ := newPacketUnsubscribe(opts)
	buf := bytes.NewBuffer(msg)
	rd := bufio.NewReader(buf)

	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		err := pkt.Unpack(rd)
		if err != nil {
			b.Fatal(err)
		}

		buf = bytes.NewBuffer(msg)
		rd.Reset(buf)
	}
}

func TestUnsubscribe_UnpackInvalidLength(t *testing.T) {
	var msg []byte
	opts := options{
		packetType:      UNSUBSCRIBE,
		controlFlags:    2,
		version:         MQTT311,
		remainingLength: 10,
	}
	pkt, err := newPacketUnsubscribe(opts)
	require.Nil(t, err)

	err = pkt.Unpack(bufio.NewReader(bytes.NewBuffer(msg)))
	require.NotNil(t, err)
}

func TestUnsubscribe_UnpackNoPacketID(t *testing.T) {
	var msg []byte
	opts := options{
		packetType:      UNSUBSCRIBE,
		controlFlags:    2,
		version:         MQTT311,
		remainingLength: len(msg),
	}
	pkt, err := newPacketUnsubscribe(opts)
	require.Nil(t, err)

	err = pkt.Unpack(bufio.NewReader(bytes.NewBuffer(msg)))
	require.NotNil(t, err)
}

func TestUnsubscribe_UnpackNoTopic(t *testing.T) {
	msg := []byte{0, 10}
	opts := options{
		packetType:      UNSUBSCRIBE,
		controlFlags:    2,
		version:         MQTT311,
		remainingLength: len(msg),
	}
	pkt, err := newPacketUnsubscribe(opts)
	require.Nil(t, err)

	err = pkt.Unpack(bufio.NewReader(bytes.NewBuffer(msg)))
	require.NotNil(t, err)
}

func TestUnsubscribe_UnpackInvalidTopicName(t *testing.T) {
	msg := []byte{
		0, 10, // packet ID
		0, 3, 'a', // invalid topic name
	}
	opts := options{
		packetType:      UNSUBSCRIBE,
		controlFlags:    2,
		version:         MQTT311,
		remainingLength: len(msg),
	}
	pkt, err := newPacketUnsubscribe(opts)
	require.Nil(t, err)

	err = pkt.Unpack(bufio.NewReader(bytes.NewBuffer(msg)))
	require.NotNil(t, err)
}

func TestUnsubscribe_UnpackV5InvalidProperties(t *testing.T) {
	msg := []byte{0, 10}
	opts := options{
		packetType:      UNSUBSCRIBE,
		controlFlags:    2,
		version:         MQTT50,
		remainingLength: len(msg),
	}
	pkt, err := newPacketUnsubscribe(opts)
	require.Nil(t, err)

	err = pkt.Unpack(bufio.NewReader(bytes.NewBuffer(msg)))
	require.NotNil(t, err)
}

func TestUnsubscribe_Size(t *testing.T) {
	t.Run("V3", func(t *testing.T) {
		msg := []byte{0, 5, 0, 3, 'a', '/', 'b'}
		opts := options{
			packetType:      UNSUBSCRIBE,
			controlFlags:    2,
			version:         MQTT311,
			remainingLength: len(msg),
		}
		pkt, err := newPacketUnsubscribe(opts)
		require.Nil(t, err)
		require.NotNil(t, pkt)
		assert.Equal(t, 7, pkt.Size())
	})

	t.Run("V5", func(t *testing.T) {
		msg := []byte{0, 5, 0, 0, 3, 'a', '/', 'b'}
		opts := options{
			packetType:      UNSUBSCRIBE,
			controlFlags:    2,
			version:         MQTT50,
			remainingLength: len(msg),
		}
		pkt, err := newPacketUnsubscribe(opts)
		require.Nil(t, err)
		require.NotNil(t, pkt)
		assert.Equal(t, 8, pkt.Size())
	})

	t.Run("V5-Properties", func(t *testing.T) {
		msg := []byte{0, 5, 7, 38, 0, 1, 'a', 0, 1, 0, 0, 3, 'a', '/', 'b'}
		opts := options{
			packetType:      UNSUBSCRIBE,
			controlFlags:    2,
			version:         MQTT50,
			remainingLength: len(msg),
		}
		pkt, err := newPacketUnsubscribe(opts)
		require.Nil(t, err)
		require.NotNil(t, pkt)
		assert.Equal(t, 15, pkt.Size())
	})
}

func TestUnsubscribe_Timestamp(t *testing.T) {
	msg := []byte{0, 5, 0, 3, 'a', '/', 'b', 1}
	opts := options{
		packetType:      UNSUBSCRIBE,
		controlFlags:    2,
		version:         MQTT50,
		remainingLength: len(msg),
	}
	pkt, err := newPacketUnsubscribe(opts)
	require.Nil(t, err)
	require.NotNil(t, pkt)
	assert.NotNil(t, pkt.Timestamp())
}
