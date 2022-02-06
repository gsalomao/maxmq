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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProperties_Unpack(t *testing.T) {
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 0, // variable header
		0,               // property length
		17, 0, 0, 0, 10, // SessionExpiryInterval
		33, 0, 50, // ReceiveMaximum
		39, 0, 0, 0, 200, // MaximumPacketSize
		34, 0, 50, // TopicAliasMaximum
		25, 1, // RequestResponseInfo
		23, 0, // RequestProblemInfo
		38, 0, 1, 'a', 0, 1, 'b', // UserProperty
		38, 0, 1, 'c', 0, 1, 'd', // UserProperty
		21, 0, 2, 'e', 'f', // AuthMethod
		22, 0, 1, 10, // AuthData
		0, 1, 'a', // client ID
	}
	msg[10] = byte(len(msg)) - 14

	fh := FixedHeader{
		PacketType:      CONNECT,
		ControlFlags:    0,
		RemainingLength: len(msg),
	}

	pkt, err := newPacketConnect(fh)
	require.Nil(t, err)

	require.Equal(t, CONNECT, pkt.Type())
	connPkt, _ := pkt.(*PacketConnect)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.Nil(t, err)
	require.NotNil(t, connPkt.Properties)

	assert.Equal(t, uint32(10), *connPkt.Properties.SessionExpiryInterval)
	assert.Equal(t, uint16(50), *connPkt.Properties.ReceiveMaximum)
	assert.Equal(t, uint32(200), *connPkt.Properties.MaximumPacketSize)
	assert.Equal(t, uint16(50), *connPkt.Properties.TopicAliasMaximum)
	assert.True(t, *connPkt.Properties.RequestResponseInfo)
	assert.False(t, *connPkt.Properties.RequestProblemInfo)
	assert.Equal(t, []byte{'a'}, connPkt.Properties.UserProperties[0].Key)
	assert.Equal(t, []byte{'b'}, connPkt.Properties.UserProperties[0].Value)
	assert.Equal(t, []byte{'c'}, connPkt.Properties.UserProperties[1].Key)
	assert.Equal(t, []byte{'d'}, connPkt.Properties.UserProperties[1].Value)
	assert.Equal(t, []byte("ef"), connPkt.Properties.AuthMethod)
	assert.Equal(t, []byte{10}, connPkt.Properties.AuthData)
}

func TestProperties_UnpackWill(t *testing.T) {
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 5, 4, 0, 0, // variable header
		0,         // property length
		0, 1, 'a', // client ID
		0,               // property length
		24, 0, 0, 0, 15, // WillDelayInterval
		1, 1, // PayloadFormatIndicator
		2, 0, 0, 0, 10, // MessageExpiryInterval
		3, 0, 4, 'j', 's', 'o', 'n', // ContentType
		8, 0, 1, 'b', // ResponseTopic
		9, 0, 2, 20, 1, // CorrelationData
		38, 0, 1, 'a', 0, 1, 'b', // UserProperty
		0, 1, 'a', // Will Topic
		0, 1, 'b', // Will Message
	}
	msg[14] = byte(len(msg)) - 15 - 3 - 3

	fh := FixedHeader{
		PacketType:      CONNECT,
		ControlFlags:    0,
		RemainingLength: len(msg),
	}

	pkt, err := newPacketConnect(fh)
	require.Nil(t, err)

	require.Equal(t, CONNECT, pkt.Type())
	connPkt, _ := pkt.(*PacketConnect)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.Nil(t, err)
	require.NotNil(t, connPkt.WillProperties)

	assert.Equal(t, uint32(15), *connPkt.WillProperties.WillDelayInterval)
	assert.Equal(t, byte(1), *connPkt.WillProperties.PayloadFormatIndicator)
	assert.Equal(t, uint32(10), *connPkt.WillProperties.MessageExpiryInterval)
	assert.Equal(t, []byte("json"), connPkt.WillProperties.ContentType)
	assert.Equal(t, []byte("b"), connPkt.WillProperties.ResponseTopic)
	assert.Equal(t, []byte{20, 1}, connPkt.WillProperties.CorrelationData)
	assert.Equal(t, []byte{'a'}, connPkt.WillProperties.UserProperties[0].Key)
	assert.Equal(t, []byte{'b'}, connPkt.WillProperties.UserProperties[0].Value)
}

func TestProperties_UnpackMalformed(t *testing.T) {
	props := [][]byte{
		{38, 0, 1, 'a', 0, 1}, // invalid user property
		{1},                   // invalid byte
		{23},                  // invalid bool
		{3, 0, 4, 'j'},        // invalid string
		{34, 0},               // invalid uint16
		{24, 0, 0, 0},         // invalid uint32
		{22, 0, 1},            // invalid binary
		{0xFF, 0, 0, 0, 10},   // invalid property
	}

	for _, prop := range props {
		msg := []byte{
			0, 4, 'M', 'Q', 'T', 'T', 5, 4, 0, 0, // variable header
			0,         // property length
			0, 1, 'a', // client ID
		}

		msg = append(msg, byte(len(prop)))
		msg = append(msg, prop...)

		fh := FixedHeader{
			PacketType:      CONNECT,
			ControlFlags:    0,
			RemainingLength: len(msg),
		}

		pkt, err := newPacketConnect(fh)
		require.Nil(t, err)

		err = pkt.Unpack(bytes.NewBuffer(msg))
		require.NotNil(t, err)
		assert.ErrorIs(t, err, ErrV5MalformedPacket)
	}
}

func TestProperties_UnpackProtocolError(t *testing.T) {
	props := [][]byte{
		{17, 0, 0, 0, 5, 17, 0, 0, 0, 9}, // SessionExpiryInterval
		{33, 0, 0},                       // ReceiveMaximum
		{33, 0, 52, 33, 0, 49},           // ReceiveMaximum
		{39, 0, 0, 0, 0},                 // MaximumPacketSize
		{39, 0, 0, 0, 9, 39, 0, 0, 0, 7}, // MaximumPacketSize
		{34, 0, 3, 34, 0, 4},             // TopicAliasMaximum
		{25, 2},                          // RequestResponseInfo
		{25, 0, 25, 1},                   // RequestResponseInfo
		{23, 2},                          // RequestProblemInfo
		{23, 0, 23, 1},                   // RequestProblemInfo
		{21, 0, 1, 'e', 21, 0, 1, 'f'},   // AuthMethod
		{22, 0, 1, 1},                    // AuthData
		{21, 0, 1, 'e', 22, 0, 1, 1, 22, 0, 1, 2}, // AuthData
		{24, 0, 0, 0, 1, 24, 0, 0, 0, 2},          // WillDelayInterval
		{1, 0, 1, 1},                              // PayloadFormatIndicator
		{1, 2},                                    // PayloadFormatIndicator
		{2, 0, 0, 0, 1, 2, 0, 0, 0, 2},            // MessageExpiryInterval
		{3, 0, 1, 'a', 3, 0, 1, 'b'},              // ContentType
		{8, 0, 1, 'a', 8, 0, 1, 'b'},              // ResponseTopic
		{9, 0, 1, 1, 9, 0, 1, 2},                  // CorrelationData
	}

	for _, p := range props {
		// Property
		msg := []byte{
			0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 0, // variable header
			byte(len(p)), // property length
		}
		msg = append(msg, p...)
		msg = append(msg, 0, 1, 'a') // client ID

		fh := FixedHeader{
			PacketType:      CONNECT,
			ControlFlags:    0,
			RemainingLength: len(msg),
		}

		pkt, err := newPacketConnect(fh)
		require.Nil(t, err)

		err = pkt.Unpack(bytes.NewBuffer(msg))
		assert.ErrorIs(t, err, ErrV5ProtocolError)

		// Will Property
		msg = []byte{
			0, 4, 'M', 'Q', 'T', 'T', 5, 4, 0, 0, // variable header
			0,
			0, 1, 'c',
			byte(len(p)), // property length
		}
		msg = append(msg, p...)

		fh = FixedHeader{
			PacketType:      CONNECT,
			ControlFlags:    0,
			RemainingLength: len(msg),
		}

		pkt, err = newPacketConnect(fh)
		require.Nil(t, err)

		err = pkt.Unpack(bytes.NewBuffer(msg))
		assert.ErrorIs(t, err, ErrV5ProtocolError)
	}
}

func TestProperties_UnpackInvalidLength(t *testing.T) {
	msg := []byte{
		0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 0, // variable header
	}

	fh := FixedHeader{
		PacketType:      CONNECT,
		ControlFlags:    0,
		RemainingLength: len(msg),
	}

	pkt, err := newPacketConnect(fh)
	require.Nil(t, err)

	err = pkt.Unpack(bytes.NewBuffer(msg))
	assert.NotNil(t, err)
}
