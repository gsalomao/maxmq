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

func TestProperties_PackEmpty(t *testing.T) {
	buf := &bytes.Buffer{}
	props := &Properties{}

	err := props.pack(buf, CONNACK)
	require.Nil(t, err)
	require.NotEmpty(t, buf)

	msg := buf.Bytes()
	assert.Equal(t, byte(0), msg[0])
}

func TestProperties_PackConnAck(t *testing.T) {
	buf := &bytes.Buffer{}
	props := &Properties{
		SessionExpiryInterval:         new(uint32),
		ReceiveMaximum:                new(uint16),
		MaximumQoS:                    new(byte),
		RetainAvailable:               new(byte),
		MaximumPacketSize:             new(uint32),
		TopicAliasMaximum:             new(uint16),
		WildcardSubscriptionAvailable: new(byte),
		SubscriptionIDAvailable:       new(byte),
		SharedSubscriptionAvailable:   new(byte),
		ServerKeepAlive:               new(uint16),
	}

	*props.SessionExpiryInterval = 10
	*props.ReceiveMaximum = 50
	*props.MaximumQoS = 1
	*props.RetainAvailable = 1
	*props.MaximumPacketSize = 200
	props.AssignedClientID = []byte("123")
	*props.TopicAliasMaximum = 40
	props.ReasonString = []byte("test")
	props.UserProperties = []UserProperty{
		{Key: []byte{'a'}, Value: []byte{0}},
		{Key: []byte{'b'}, Value: []byte{1}},
	}
	*props.WildcardSubscriptionAvailable = 1
	*props.SubscriptionIDAvailable = 1
	*props.SharedSubscriptionAvailable = 1
	*props.ServerKeepAlive = 30
	props.ResponseInfo = []byte("info")
	props.ServerReference = []byte("srv")
	props.AuthMethod = []byte("JWT")
	props.AuthData = []byte("token")

	err := props.pack(buf, CONNACK)
	require.Nil(t, err)
	require.NotEmpty(t, buf)

	msg := buf.Bytes()
	assert.Equal(t, byte(83), msg[0])
	assert.Equal(t, []byte{17, 0, 0, 0, 10}, msg[1:6])
	assert.Equal(t, []byte{33, 0, 50}, msg[6:9])
	assert.Equal(t, []byte{36, 1}, msg[9:11])
	assert.Equal(t, []byte{37, 1}, msg[11:13])
	assert.Equal(t, []byte{39, 0, 0, 0, 200}, msg[13:18])
	assert.Equal(t, []byte{18, 0, 3, '1', '2', '3'}, msg[18:24])
	assert.Equal(t, []byte{34, 0, 40}, msg[24:27])
	assert.Equal(t, []byte{31, 0, 4, 't', 'e', 's', 't'}, msg[27:34])
	assert.Equal(t, []byte{38, 0, 1, 'a', 0, 1, 0}, msg[34:41])
	assert.Equal(t, []byte{38, 0, 1, 'b', 0, 1, 1}, msg[41:48])
	assert.Equal(t, []byte{40, 1}, msg[48:50])
	assert.Equal(t, []byte{41, 1}, msg[50:52])
	assert.Equal(t, []byte{42, 1}, msg[52:54])
	assert.Equal(t, []byte{19, 0, 30}, msg[54:57])
	assert.Equal(t, []byte{26, 0, 4, 'i', 'n', 'f', 'o'}, msg[57:64])
	assert.Equal(t, []byte{28, 0, 3, 's', 'r', 'v'}, msg[64:70])
	assert.Equal(t, []byte{21, 0, 3, 'J', 'W', 'T'}, msg[70:76])
	assert.Equal(t, []byte{22, 0, 5, 't', 'o', 'k', 'e', 'n'}, msg[76:84])
}

func TestProperties_PackDisconnect(t *testing.T) {
	buf := &bytes.Buffer{}
	props := &Properties{SessionExpiryInterval: new(uint32)}

	*props.SessionExpiryInterval = 10
	props.ReasonString = []byte("test")
	props.UserProperties = []UserProperty{
		{Key: []byte{'a'}, Value: []byte{0}},
		{Key: []byte{'b'}, Value: []byte{1}},
	}
	props.ServerReference = []byte("srv")

	err := props.pack(buf, DISCONNECT)
	require.Nil(t, err)
	require.NotEmpty(t, buf)

	msg := buf.Bytes()
	assert.Equal(t, []byte{17, 0, 0, 0, 10}, msg[1:6])
	assert.Equal(t, []byte{31, 0, 4, 't', 'e', 's', 't'}, msg[6:13])
	assert.Equal(t, []byte{38, 0, 1, 'a', 0, 1, 0}, msg[13:20])
	assert.Equal(t, []byte{38, 0, 1, 'b', 0, 1, 1}, msg[20:27])
	assert.Equal(t, []byte{28, 0, 3, 's', 'r', 'v'}, msg[27:33])
}

func TestProperties_PackInvalidProperty(t *testing.T) {
	buf := &bytes.Buffer{}
	props := &Properties{MaximumQoS: new(byte), ServerKeepAlive: new(uint16)}
	*props.MaximumQoS = 1
	*props.ServerKeepAlive = 30

	err := props.pack(buf, CONNECT)
	require.NotNil(t, err)
}

func TestProperties_UnpackConnect(t *testing.T) {
	msg := []byte{
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
		24, 0, 0, 0, 15, // WillDelayInterval
		1, 1, // PayloadFormatIndicator
		2, 0, 0, 0, 10, // MessageExpiryInterval
		3, 0, 4, 'j', 's', 'o', 'n', // ContentType
		8, 0, 1, 'b', // ResponseTopic
		9, 0, 2, 20, 1, // CorrelationData
		38, 0, 1, 'a', 0, 1, 'b', // UserProperty
		0, 1, 'a', // client ID
	}
	msg[0] = byte(len(msg)) - 4

	props := Properties{}
	err := props.unpack(bytes.NewBuffer(msg), CONNECT)
	require.Nil(t, err)

	assert.Equal(t, uint32(10), *props.SessionExpiryInterval)
	assert.Equal(t, uint16(50), *props.ReceiveMaximum)
	assert.Equal(t, uint32(200), *props.MaximumPacketSize)
	assert.Equal(t, uint16(50), *props.TopicAliasMaximum)
	assert.Equal(t, byte(1), *props.RequestResponseInfo)
	assert.Equal(t, byte(0), *props.RequestProblemInfo)
	assert.Equal(t, []byte{'a'}, props.UserProperties[0].Key)
	assert.Equal(t, []byte{'b'}, props.UserProperties[0].Value)
	assert.Equal(t, []byte{'c'}, props.UserProperties[1].Key)
	assert.Equal(t, []byte{'d'}, props.UserProperties[1].Value)
	assert.Equal(t, []byte("ef"), props.AuthMethod)
	assert.Equal(t, []byte{10}, props.AuthData)
	assert.Equal(t, uint32(15), *props.WillDelayInterval)
	assert.Equal(t, byte(1), *props.PayloadFormatIndicator)
	assert.Equal(t, uint32(10), *props.MessageExpiryInterval)
	assert.Equal(t, []byte("json"), props.ContentType)
	assert.Equal(t, []byte("b"), props.ResponseTopic)
	assert.Equal(t, []byte{20, 1}, props.CorrelationData)
	assert.Equal(t, []byte{'a'}, props.UserProperties[0].Key)
	assert.Equal(t, []byte{'b'}, props.UserProperties[0].Value)
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
		msg := []byte{byte(len(prop))}
		msg = append(msg, prop...)
		msg = append(msg, 0, 1, 'a') // client ID

		props := Properties{}
		err := props.unpack(bytes.NewBuffer(msg), CONNECT)
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
		msg := []byte{byte(len(p))}
		msg = append(msg, p...)
		msg = append(msg, 0, 1, 'a') // client ID

		props := Properties{}
		err := props.unpack(bytes.NewBuffer(msg), CONNECT)
		assert.ErrorIs(t, err, ErrV5ProtocolError)
	}
}

func TestProperties_UnpackInvalidLength(t *testing.T) {
	var msg []byte

	props := Properties{}
	err := props.unpack(bytes.NewBuffer(msg), CONNECT)
	assert.NotNil(t, err)
}

func TestProperties_Size(t *testing.T) {
	props := &Properties{
		SessionExpiryInterval:         new(uint32),
		ReceiveMaximum:                new(uint16),
		MaximumQoS:                    new(byte),
		RetainAvailable:               new(byte),
		MaximumPacketSize:             new(uint32),
		TopicAliasMaximum:             new(uint16),
		WildcardSubscriptionAvailable: new(byte),
		SubscriptionIDAvailable:       new(byte),
		SharedSubscriptionAvailable:   new(byte),
		ServerKeepAlive:               new(uint16),
	}

	*props.SessionExpiryInterval = 10
	*props.ReceiveMaximum = 50
	*props.MaximumQoS = 1
	*props.RetainAvailable = 1
	*props.MaximumPacketSize = 200
	props.AssignedClientID = []byte("123")
	*props.TopicAliasMaximum = 40
	props.ReasonString = []byte("test")
	props.UserProperties = []UserProperty{
		{Key: []byte{'a'}, Value: []byte{0}},
		{Key: []byte{'b'}, Value: []byte{1}},
	}
	*props.WildcardSubscriptionAvailable = 1
	*props.SubscriptionIDAvailable = 1
	*props.SharedSubscriptionAvailable = 1
	*props.ServerKeepAlive = 30
	props.ResponseInfo = []byte("info")
	props.ServerReference = []byte("srv")
	props.AuthMethod = []byte("JWT")
	props.AuthData = []byte("token")

	assert.Equal(t, 83, props.size(CONNACK))
}
