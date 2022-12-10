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

func TestUnsubscribeInvalidPacketType(t *testing.T) {
	opts := options{packetType: DISCONNECT}
	pkt, err := newPacketUnsubscribe(opts)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestUnsubscribeInvalidControlFlags(t *testing.T) {
	opts := options{packetType: UNSUBSCRIBE, controlFlags: 0}
	pkt, err := newPacketUnsubscribe(opts)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestUnsubscribeInvalidVersion(t *testing.T) {
	opts := options{packetType: UNSUBSCRIBE, controlFlags: 2}
	pkt, err := newPacketUnsubscribe(opts)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestUnsubscribeWriteUnsupported(t *testing.T) {
	opts := options{packetType: UNSUBSCRIBE, controlFlags: 2, version: MQTT311}
	pkt, err := newPacketUnsubscribe(opts)
	require.Nil(t, err)
	require.NotNil(t, pkt)

	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)
	err = pkt.Write(wr)
	require.NotNil(t, err)
}

func TestUnsubscribeReadV3(t *testing.T) {
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

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	require.Nil(t, err)

	assert.Equal(t, MQTT311, unsubPkt.Version)
	assert.Equal(t, ID(10), unsubPkt.PacketID)
	require.Len(t, unsubPkt.Topics, 3)
	assert.Equal(t, "a/b", unsubPkt.Topics[0])
	assert.Equal(t, "c/d/e", unsubPkt.Topics[1])
	assert.Equal(t, "a/#", unsubPkt.Topics[2])
	assert.Nil(t, unsubPkt.Properties)
}

func BenchmarkUnsubscribeReadV3(b *testing.B) {
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
		err := pkt.Read(rd)
		if err != nil {
			b.Fatal(err)
		}

		buf = bytes.NewBuffer(msg)
		rd.Reset(buf)
	}
}

func TestUnsubscribeReadV5(t *testing.T) {
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

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	require.Nil(t, err)

	assert.Equal(t, MQTT50, unsubPkt.Version)
	assert.Equal(t, ID(25), unsubPkt.PacketID)
	require.Len(t, unsubPkt.Topics, 1)
	assert.Equal(t, "a/b", unsubPkt.Topics[0])
}

func BenchmarkUnsubscribeReadV5(b *testing.B) {
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
		err := pkt.Read(rd)
		if err != nil {
			b.Fatal(err)
		}

		buf = bytes.NewBuffer(msg)
		rd.Reset(buf)
	}
}

func TestUnsubscribeReadInvalidLength(t *testing.T) {
	var msg []byte
	opts := options{
		packetType:      UNSUBSCRIBE,
		controlFlags:    2,
		version:         MQTT311,
		remainingLength: 10,
	}
	pkt, err := newPacketUnsubscribe(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	require.NotNil(t, err)
}

func TestUnsubscribeReadNoPacketID(t *testing.T) {
	var msg []byte
	opts := options{
		packetType:      UNSUBSCRIBE,
		controlFlags:    2,
		version:         MQTT311,
		remainingLength: len(msg),
	}
	pkt, err := newPacketUnsubscribe(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	require.NotNil(t, err)
}

func TestUnsubscribeReadNoTopic(t *testing.T) {
	msg := []byte{0, 10}
	opts := options{
		packetType:      UNSUBSCRIBE,
		controlFlags:    2,
		version:         MQTT311,
		remainingLength: len(msg),
	}
	pkt, err := newPacketUnsubscribe(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	require.NotNil(t, err)
}

func TestUnsubscribeReadInvalidTopicName(t *testing.T) {
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

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	require.NotNil(t, err)
}

func TestUnsubscribeReadV5InvalidProperties(t *testing.T) {
	msg := []byte{0, 10}
	opts := options{
		packetType:      UNSUBSCRIBE,
		controlFlags:    2,
		version:         MQTT50,
		remainingLength: len(msg),
	}
	pkt, err := newPacketUnsubscribe(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	require.NotNil(t, err)
}

func TestUnsubscribeSize(t *testing.T) {
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

func TestUnsubscribeTimestamp(t *testing.T) {
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
