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

func TestSubscribe_InvalidPacketType(t *testing.T) {
	opts := options{packetType: DISCONNECT}
	pkt, err := newPacketSubscribe(opts)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestSubscribe_InvalidControlFlags(t *testing.T) {
	opts := options{packetType: SUBSCRIBE, controlFlags: 0}
	pkt, err := newPacketSubscribe(opts)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestSubscribe_InvalidVersion(t *testing.T) {
	opts := options{packetType: SUBSCRIBE, controlFlags: 2}
	pkt, err := newPacketSubscribe(opts)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestSubscribe_PackUnsupported(t *testing.T) {
	opts := options{packetType: SUBSCRIBE, controlFlags: 2, version: MQTT311}
	pkt, err := newPacketSubscribe(opts)
	require.Nil(t, err)
	require.NotNil(t, pkt)

	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)
	err = pkt.Pack(wr)
	require.NotNil(t, err)
}

func TestSubscribe_UnpackV3(t *testing.T) {
	msg := []byte{
		0, 10, // packet ID
		0, 3, 'a', '/', 'b', 1, // topic filter
		0, 5, 'c', '/', 'd', '/', 'e', 0, // topic filter
		0, 3, 'a', '/', '#', 2, // topic filter
	}
	opts := options{
		packetType:      SUBSCRIBE,
		controlFlags:    2,
		version:         MQTT311,
		remainingLength: len(msg),
	}
	pkt, err := newPacketSubscribe(opts)
	require.Nil(t, err)

	require.Equal(t, SUBSCRIBE, pkt.Type())
	subPkt, _ := pkt.(*Subscribe)

	err = pkt.Unpack(bufio.NewReader(bytes.NewBuffer(msg)))
	require.Nil(t, err)

	assert.Equal(t, MQTT311, subPkt.Version)
	assert.Equal(t, ID(10), subPkt.PacketID)
	require.Equal(t, 3, len(subPkt.Topics))
	assert.Equal(t, "a/b", subPkt.Topics[0].Name)
	assert.Equal(t, QoS1, subPkt.Topics[0].QoS)
	assert.Equal(t, "c/d/e", subPkt.Topics[1].Name)
	assert.Equal(t, QoS0, subPkt.Topics[1].QoS)
	assert.Equal(t, "a/#", subPkt.Topics[2].Name)
	assert.Equal(t, QoS2, subPkt.Topics[2].QoS)
	assert.Nil(t, subPkt.Properties)
}

func TestSubscribe_UnpackV5(t *testing.T) {
	msg := []byte{
		0, 25, // packet ID
		0,                         // property length
		0, 3, 'a', '/', 'b', 0x2E, // topic filter
	}
	opts := options{
		packetType:      SUBSCRIBE,
		controlFlags:    2,
		version:         MQTT50,
		remainingLength: len(msg),
	}
	pkt, err := newPacketSubscribe(opts)
	require.Nil(t, err)

	require.Equal(t, SUBSCRIBE, pkt.Type())
	subPkt, _ := pkt.(*Subscribe)

	err = pkt.Unpack(bufio.NewReader(bytes.NewBuffer(msg)))
	require.Nil(t, err)

	assert.Equal(t, MQTT50, subPkt.Version)
	assert.Equal(t, ID(25), subPkt.PacketID)
	require.Equal(t, 1, len(subPkt.Topics))
	assert.Equal(t, "a/b", subPkt.Topics[0].Name)
	assert.Equal(t, QoS2, subPkt.Topics[0].QoS)
	assert.Equal(t, byte(2), subPkt.Topics[0].RetainHandling)
	assert.True(t, subPkt.Topics[0].RetainAsPublished)
	assert.True(t, subPkt.Topics[0].NoLocal)
}

func TestSubscribe_UnpackInvalidLength(t *testing.T) {
	var msg []byte
	opts := options{
		packetType:      SUBSCRIBE,
		controlFlags:    2,
		version:         MQTT311,
		remainingLength: 10,
	}
	pkt, err := newPacketSubscribe(opts)
	require.Nil(t, err)

	err = pkt.Unpack(bufio.NewReader(bytes.NewBuffer(msg)))
	require.NotNil(t, err)
}

func TestSubscribe_UnpackNoPacketID(t *testing.T) {
	var msg []byte
	opts := options{
		packetType:      SUBSCRIBE,
		controlFlags:    2,
		version:         MQTT311,
		remainingLength: len(msg),
	}
	pkt, err := newPacketSubscribe(opts)
	require.Nil(t, err)

	err = pkt.Unpack(bufio.NewReader(bytes.NewBuffer(msg)))
	require.NotNil(t, err)
}

func TestSubscribe_UnpackNoTopic(t *testing.T) {
	msg := []byte{
		0, 10, // packet ID
	}
	opts := options{
		packetType:      SUBSCRIBE,
		controlFlags:    2,
		version:         MQTT311,
		remainingLength: len(msg),
	}
	pkt, err := newPacketSubscribe(opts)
	require.Nil(t, err)

	err = pkt.Unpack(bufio.NewReader(bytes.NewBuffer(msg)))
	require.NotNil(t, err)
}

func TestSubscribe_UnpackInvalidTopicName(t *testing.T) {
	msg := []byte{
		0, 10, // packet ID
		0, 3, 'a', // invalid topic name
	}
	opts := options{
		packetType:      SUBSCRIBE,
		controlFlags:    2,
		version:         MQTT311,
		remainingLength: len(msg),
	}
	pkt, err := newPacketSubscribe(opts)
	require.Nil(t, err)

	err = pkt.Unpack(bufio.NewReader(bytes.NewBuffer(msg)))
	require.NotNil(t, err)
}

func TestSubscribe_UnpackNoTopicQoS(t *testing.T) {
	msg := []byte{
		0, 10, // packet ID
		0, 1, 'a', // no topic QoS
	}
	opts := options{
		packetType:      SUBSCRIBE,
		controlFlags:    2,
		version:         MQTT311,
		remainingLength: len(msg),
	}
	pkt, err := newPacketSubscribe(opts)
	require.Nil(t, err)

	err = pkt.Unpack(bufio.NewReader(bytes.NewBuffer(msg)))
	require.NotNil(t, err)
}

func TestSubscribe_UnpackV3InvalidSubscriptionOptions(t *testing.T) {
	msg := []byte{
		0, 10, // packet ID
		0, 1, 'a', 0x10, // invalid subscription options
	}
	opts := options{
		packetType:      SUBSCRIBE,
		controlFlags:    2,
		version:         MQTT311,
		remainingLength: len(msg),
	}
	pkt, err := newPacketSubscribe(opts)
	require.Nil(t, err)

	err = pkt.Unpack(bufio.NewReader(bytes.NewBuffer(msg)))
	require.NotNil(t, err)
}

func TestSubscribe_UnpackV5InvalidSubscriptionOptions(t *testing.T) {
	msg := []byte{
		0, 10, // packet ID
		0,               // property length
		0, 1, 'a', 0x40, // invalid subscription options
	}
	opts := options{
		packetType:      SUBSCRIBE,
		controlFlags:    2,
		version:         MQTT50,
		remainingLength: len(msg),
	}
	pkt, err := newPacketSubscribe(opts)
	require.Nil(t, err)

	err = pkt.Unpack(bufio.NewReader(bytes.NewBuffer(msg)))
	require.NotNil(t, err)
}

func TestSubscribe_UnpackV3InvalidQoS(t *testing.T) {
	msg := []byte{
		0, 10, // packet ID
		0, 1, 'a', 0x03, // invalid QoS
	}
	opts := options{
		packetType:      SUBSCRIBE,
		controlFlags:    2,
		version:         MQTT311,
		remainingLength: len(msg),
	}
	pkt, err := newPacketSubscribe(opts)
	require.Nil(t, err)

	err = pkt.Unpack(bufio.NewReader(bytes.NewBuffer(msg)))
	require.NotNil(t, err)
}

func TestSubscribe_UnpackV5InvalidQoS(t *testing.T) {
	msg := []byte{
		0, 10, // packet ID
		0,               // property length
		0, 1, 'a', 0x03, // invalid QoS
	}
	opts := options{
		packetType:      SUBSCRIBE,
		controlFlags:    2,
		version:         MQTT50,
		remainingLength: len(msg),
	}
	pkt, err := newPacketSubscribe(opts)
	require.Nil(t, err)

	err = pkt.Unpack(bufio.NewReader(bytes.NewBuffer(msg)))
	require.NotNil(t, err)
}

func TestSubscribe_UnpackV5InvalidRetainHandling(t *testing.T) {
	msg := []byte{
		0, 10, // packet ID
		0,               // property length
		0, 1, 'a', 0x30, // invalid Retain Handling
	}
	opts := options{
		packetType:      SUBSCRIBE,
		controlFlags:    2,
		version:         MQTT50,
		remainingLength: len(msg),
	}
	pkt, err := newPacketSubscribe(opts)
	require.Nil(t, err)

	err = pkt.Unpack(bufio.NewReader(bytes.NewBuffer(msg)))
	require.NotNil(t, err)
}

func TestSubscribe_UnpackV5InvalidProperties(t *testing.T) {
	msg := []byte{
		0, 10, // packet ID
	}
	opts := options{
		packetType:      SUBSCRIBE,
		controlFlags:    2,
		version:         MQTT50,
		remainingLength: len(msg),
	}
	pkt, err := newPacketSubscribe(opts)
	require.Nil(t, err)

	err = pkt.Unpack(bufio.NewReader(bytes.NewBuffer(msg)))
	require.NotNil(t, err)
}

func TestSubscribe_Size(t *testing.T) {
	t.Run("V3", func(t *testing.T) {
		msg := []byte{
			0, 5, // packet ID
			0, 3, 'a', '/', 'b', 1, // topic filter
		}

		opts := options{
			packetType:      SUBSCRIBE,
			controlFlags:    2,
			version:         MQTT311,
			remainingLength: len(msg),
		}
		pkt, err := newPacketSubscribe(opts)
		require.Nil(t, err)
		require.NotNil(t, pkt)

		assert.Equal(t, 8, pkt.Size())
	})

	t.Run("V5", func(t *testing.T) {
		msg := []byte{
			0, 5, // packet ID
			0,                      // property length
			0, 3, 'a', '/', 'b', 1, // topic filter
		}

		opts := options{
			packetType:      SUBSCRIBE,
			controlFlags:    2,
			version:         MQTT50,
			remainingLength: len(msg),
		}
		pkt, err := newPacketSubscribe(opts)
		require.Nil(t, err)
		require.NotNil(t, pkt)

		assert.Equal(t, 9, pkt.Size())
	})

	t.Run("V5-Properties", func(t *testing.T) {
		msg := []byte{
			0, 5, // packet ID
			2,     // property length
			11, 5, // SubscriptionIdentifier
			0, 3, 'a', '/', 'b', 1, // topic filter
		}

		opts := options{
			packetType:      SUBSCRIBE,
			controlFlags:    2,
			version:         MQTT50,
			remainingLength: len(msg),
		}
		pkt, err := newPacketSubscribe(opts)
		require.Nil(t, err)
		require.NotNil(t, pkt)

		assert.Equal(t, 11, pkt.Size())
	})
}

func TestSubscribe_Timestamp(t *testing.T) {
	msg := []byte{
		0, 5, // packet ID
		0, 3, 'a', '/', 'b', 1, // topic filter
	}
	opts := options{
		packetType:      SUBSCRIBE,
		controlFlags:    2,
		version:         MQTT50,
		remainingLength: len(msg),
	}
	pkt, err := newPacketSubscribe(opts)
	require.Nil(t, err)
	require.NotNil(t, pkt)
	assert.NotNil(t, pkt.Timestamp())
}
