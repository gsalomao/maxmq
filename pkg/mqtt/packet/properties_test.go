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
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProperties_WritePropertiesEmpty(t *testing.T) {
	buf := &bytes.Buffer{}

	err := writeProperties(buf, nil, CONNACK)
	require.Nil(t, err)
	require.NotEmpty(t, buf)

	msg := buf.Bytes()
	assert.Equal(t, byte(0), msg[0])
}

func TestProperties_WritePropertiesConnAck(t *testing.T) {
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

	err := writeProperties(buf, props, CONNACK)
	require.Nil(t, err)
	require.NotEmpty(t, buf)

	msg := buf.Bytes()
	assert.Equal(t, byte(83), msg[0])
	assert.Equal(t, []byte{17, 0, 0, 0, 10}, msg[1:6])
	assert.Equal(t, []byte{18, 0, 3, '1', '2', '3'}, msg[6:12])
	assert.Equal(t, []byte{19, 0, 30}, msg[12:15])
	assert.Equal(t, []byte{21, 0, 3, 'J', 'W', 'T'}, msg[15:21])
	assert.Equal(t, []byte{22, 0, 5, 't', 'o', 'k', 'e', 'n'}, msg[21:29])
	assert.Equal(t, []byte{26, 0, 4, 'i', 'n', 'f', 'o'}, msg[29:36])
	assert.Equal(t, []byte{28, 0, 3, 's', 'r', 'v'}, msg[36:42])
	assert.Equal(t, []byte{31, 0, 4, 't', 'e', 's', 't'}, msg[42:49])
	assert.Equal(t, []byte{33, 0, 50}, msg[49:52])
	assert.Equal(t, []byte{34, 0, 40}, msg[52:55])
	assert.Equal(t, []byte{36, 1}, msg[55:57])
	assert.Equal(t, []byte{37, 1}, msg[57:59])
	assert.Equal(t, []byte{38, 0, 1, 'a', 0, 1, 0}, msg[59:66])
	assert.Equal(t, []byte{38, 0, 1, 'b', 0, 1, 1}, msg[66:73])
	assert.Equal(t, []byte{39, 0, 0, 0, 200}, msg[73:78])
	assert.Equal(t, []byte{40, 1}, msg[78:80])
	assert.Equal(t, []byte{41, 1}, msg[80:82])
	assert.Equal(t, []byte{42, 1}, msg[82:84])
}

func TestProperties_WritePropertiesDisconnect(t *testing.T) {
	buf := &bytes.Buffer{}
	props := &Properties{SessionExpiryInterval: new(uint32)}

	*props.SessionExpiryInterval = 10
	props.ReasonString = []byte("test")
	props.UserProperties = []UserProperty{
		{Key: []byte{'a'}, Value: []byte{0}},
		{Key: []byte{'b'}, Value: []byte{1}},
	}
	props.ServerReference = []byte("srv")

	err := writeProperties(buf, props, DISCONNECT)
	require.Nil(t, err)
	require.NotEmpty(t, buf)

	msg := buf.Bytes()
	assert.Equal(t, []byte{17, 0, 0, 0, 10}, msg[1:6])
	assert.Equal(t, []byte{28, 0, 3, 's', 'r', 'v'}, msg[6:12])
	assert.Equal(t, []byte{31, 0, 4, 't', 'e', 's', 't'}, msg[12:19])
	assert.Equal(t, []byte{38, 0, 1, 'a', 0, 1, 0}, msg[19:26])
	assert.Equal(t, []byte{38, 0, 1, 'b', 0, 1, 1}, msg[26:33])
}

func TestProperties_WritePropertiesSubAck(t *testing.T) {
	buf := &bytes.Buffer{}
	props := &Properties{}

	props.ReasonString = []byte("test")
	props.UserProperties = []UserProperty{
		{Key: []byte{'a'}, Value: []byte{0}},
		{Key: []byte{'b'}, Value: []byte{1}},
	}

	err := writeProperties(buf, props, SUBACK)
	require.Nil(t, err)
	require.NotEmpty(t, buf)

	msg := buf.Bytes()
	assert.Equal(t, byte(21), msg[0])
	assert.Equal(t, []byte{31, 0, 4, 't', 'e', 's', 't'}, msg[1:8])
	assert.Equal(t, []byte{38, 0, 1, 'a', 0, 1, 0}, msg[8:15])
	assert.Equal(t, []byte{38, 0, 1, 'b', 0, 1, 1}, msg[15:22])
}

func TestProperties_WritePropertiesUnsubAck(t *testing.T) {
	buf := &bytes.Buffer{}
	props := &Properties{}

	props.ReasonString = []byte("test")
	props.UserProperties = []UserProperty{
		{Key: []byte{'a'}, Value: []byte{0}},
		{Key: []byte{'b'}, Value: []byte{1}},
	}

	err := writeProperties(buf, props, UNSUBACK)
	require.Nil(t, err)
	require.NotEmpty(t, buf)

	msg := buf.Bytes()
	assert.Equal(t, byte(21), msg[0])
	assert.Equal(t, []byte{31, 0, 4, 't', 'e', 's', 't'}, msg[1:8])
	assert.Equal(t, []byte{38, 0, 1, 'a', 0, 1, 0}, msg[8:15])
	assert.Equal(t, []byte{38, 0, 1, 'b', 0, 1, 1}, msg[15:22])
}

func TestProperties_WritePropertiesPublish(t *testing.T) {
	buf := &bytes.Buffer{}
	props := &Properties{
		PayloadFormatIndicator: new(byte),
		MessageExpiryInterval:  new(uint32),
		TopicAlias:             new(uint16),
		ResponseTopic:          new(string),
		SubscriptionIdentifier: new(int),
	}

	*props.PayloadFormatIndicator = 1
	*props.MessageExpiryInterval = 200
	*props.TopicAlias = 10
	*props.ResponseTopic = "a/b"
	props.CorrelationData = []byte{1, 2, 3}
	*props.SubscriptionIdentifier = 1
	props.ContentType = []byte("json")
	props.UserProperties = []UserProperty{
		{Key: []byte{'a'}, Value: []byte{0}},
		{Key: []byte{'b'}, Value: []byte{1}},
	}

	err := writeProperties(buf, props, PUBLISH)
	require.Nil(t, err)
	require.NotEmpty(t, buf)

	msg := buf.Bytes()
	assert.Equal(t, byte(45), msg[0])
	assert.Equal(t, []byte{1, 1}, msg[1:3])
	assert.Equal(t, []byte{2, 0, 0, 0, 200}, msg[3:8])
	assert.Equal(t, []byte{3, 0, 4, 'j', 's', 'o', 'n'}, msg[8:15])
	assert.Equal(t, []byte{8, 0, 3, 'a', '/', 'b'}, msg[15:21])
	assert.Equal(t, []byte{9, 0, 3, 1, 2, 3}, msg[21:27])
	assert.Equal(t, []byte{11, 1}, msg[27:29])
	assert.Equal(t, []byte{35, 0, 10}, msg[29:32])
	assert.Equal(t, []byte{38, 0, 1, 'a', 0, 1, 0}, msg[32:39])
	assert.Equal(t, []byte{38, 0, 1, 'b', 0, 1, 1}, msg[39:46])
}

func TestProperties_WritePropertiesPubAck(t *testing.T) {
	buf := &bytes.Buffer{}
	props := &Properties{}

	props.ReasonString = []byte("test")
	props.UserProperties = []UserProperty{
		{Key: []byte{'a'}, Value: []byte{0}},
		{Key: []byte{'b'}, Value: []byte{1}},
	}

	err := writeProperties(buf, props, PUBACK)
	require.Nil(t, err)
	require.NotEmpty(t, buf)

	msg := buf.Bytes()
	assert.Equal(t, byte(21), msg[0])
	assert.Equal(t, []byte{31, 0, 4, 't', 'e', 's', 't'}, msg[1:8])
	assert.Equal(t, []byte{38, 0, 1, 'a', 0, 1, 0}, msg[8:15])
	assert.Equal(t, []byte{38, 0, 1, 'b', 0, 1, 1}, msg[15:22])
}

func TestProperties_WritePropertiesInvalidProperty(t *testing.T) {
	buf := &bytes.Buffer{}
	props := &Properties{MaximumQoS: new(byte), ServerKeepAlive: new(uint16)}
	*props.MaximumQoS = 1
	*props.ServerKeepAlive = 30

	err := writeProperties(buf, props, CONNECT)
	require.NotNil(t, err)
}

func TestProperties_ReadPropertiesConnect(t *testing.T) {
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

	props, err := readProperties(bytes.NewBuffer(msg), CONNECT)
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
	assert.Equal(t, "b", *props.ResponseTopic)
	assert.Equal(t, []byte{20, 1}, props.CorrelationData)
	assert.Equal(t, []byte{'a'}, props.UserProperties[0].Key)
	assert.Equal(t, []byte{'b'}, props.UserProperties[0].Value)
}

func BenchmarkProperties_ReadPropertiesConnect(b *testing.B) {
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

	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := readProperties(bytes.NewBuffer(msg), CONNECT)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestProperties_ReadPropertiesDisconnect(t *testing.T) {
	msg := []byte{
		0,               // property length
		17, 0, 0, 0, 10, // SessionExpiryInterval
		31, 0, 2, 'r', 's', // ReasonString
		38, 0, 1, 'a', 0, 1, 'b', // UserProperty
		38, 0, 1, 'c', 0, 1, 'd', // UserProperty
		28, 0, 2, 'e', 'f', // ServerReference
	}
	msg[0] = byte(len(msg)) - 1

	props, err := readProperties(bytes.NewBuffer(msg), DISCONNECT)
	require.Nil(t, err)

	assert.Equal(t, uint32(10), *props.SessionExpiryInterval)
	assert.Equal(t, []byte("rs"), props.ReasonString)
	assert.Equal(t, []byte{'a'}, props.UserProperties[0].Key)
	assert.Equal(t, []byte{'b'}, props.UserProperties[0].Value)
	assert.Equal(t, []byte("ef"), props.ServerReference)
}

func TestProperties_ReadPropertiesSubscribe(t *testing.T) {
	msg := []byte{
		0,      // property length
		11, 10, // SubscriptionIdentifier
		38, 0, 1, 'a', 0, 1, 'b', // UserProperty
		38, 0, 1, 'c', 0, 1, 'd', // UserProperty
	}
	msg[0] = byte(len(msg)) - 1

	props, err := readProperties(bytes.NewBuffer(msg), SUBSCRIBE)
	require.Nil(t, err)

	assert.Equal(t, 10, *props.SubscriptionIdentifier)
	assert.Equal(t, []byte{'a'}, props.UserProperties[0].Key)
	assert.Equal(t, []byte{'b'}, props.UserProperties[0].Value)
}

func TestProperties_ReadPropertiesUnsubscribe(t *testing.T) {
	msg := []byte{
		0,                        // property length
		38, 0, 1, 'a', 0, 1, 'b', // UserProperty
		38, 0, 1, 'c', 0, 1, 'd', // UserProperty
	}
	msg[0] = byte(len(msg)) - 1

	props, err := readProperties(bytes.NewBuffer(msg), UNSUBSCRIBE)
	require.Nil(t, err)

	assert.Equal(t, []byte{'a'}, props.UserProperties[0].Key)
	assert.Equal(t, []byte{'b'}, props.UserProperties[0].Value)
}

func TestProperties_ReadPropertiesPublish(t *testing.T) {
	msg := []byte{
		0,    // property length
		1, 1, // PayloadFormatIndicator
		2, 0, 0, 0, 60, // MessageExpiryInterval
		3, 0, 4, 'j', 's', 'o', 'n', // ContentType
		8, 0, 3, 'a', '/', 'b', // ResponseTopic
		9, 0, 2, 0, 9, //  CorrelationData
		11, 10, // SubscriptionIdentifier
		35, 0, 15, // TopicAlias
		38, 0, 1, 'a', 0, 1, 'b', // UserProperty
		38, 0, 1, 'c', 0, 1, 'd', // UserProperty
	}
	msg[0] = byte(len(msg)) - 1

	props, err := readProperties(bytes.NewBuffer(msg), PUBLISH)
	require.Nil(t, err)

	assert.Equal(t, uint8(1), *props.PayloadFormatIndicator)
	assert.Equal(t, uint32(60), *props.MessageExpiryInterval)
	assert.Equal(t, []byte("json"), props.ContentType)
	assert.Equal(t, "a/b", *props.ResponseTopic)
	assert.Equal(t, []byte{0, 9}, props.CorrelationData)
	assert.Equal(t, 10, *props.SubscriptionIdentifier)
	assert.Equal(t, uint16(15), *props.TopicAlias)
	assert.Equal(t, []byte{'a'}, props.UserProperties[0].Key)
	assert.Equal(t, []byte{'b'}, props.UserProperties[0].Value)
}

func TestProperties_ReadPropertiesMalformed(t *testing.T) {
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

		_, err := readProperties(bytes.NewBuffer(msg), CONNECT)
		require.NotNil(t, err)
		assert.ErrorContains(t, err, ErrV5MalformedPacket.Error())
	}
}

func TestProperties_ReadPropertiesProtocolError(t *testing.T) {
	testCases := []struct {
		pktType Type
		props   []byte
	}{
		{pktType: CONNECT, props: []byte{
			17, 0, 0, 0, 5, 17, 0, 0, 0, 9}}, // SessionExpiryInterval
		{pktType: CONNECT, props: []byte{33, 0,
			0}}, // ReceiveMaximum
		{pktType: CONNECT, props: []byte{33, 0, 52, 33, 0,
			49}}, // ReceiveMaximum
		{pktType: CONNECT, props: []byte{
			39, 0, 0, 0, 0}}, // MaximumPacketSize
		{pktType: CONNECT, props: []byte{
			39, 0, 0, 0, 9, 39, 0, 0, 0, 7}}, // MaximumPacketSize
		{pktType: CONNECT, props: []byte{
			34, 0, 3, 34, 0, 4}}, // TopicAliasMaximum
		{pktType: CONNECT, props: []byte{
			25, 2}}, // RequestResponseInfo
		{pktType: CONNECT, props: []byte{
			25, 0, 25, 1}}, // RequestResponseInfo
		{pktType: CONNECT, props: []byte{
			23, 2}}, // RequestProblemInfo
		{pktType: CONNECT, props: []byte{
			23, 0, 23, 1}}, // RequestProblemInfo
		{pktType: CONNECT, props: []byte{
			21, 0, 1, 'e', 21, 0, 1, 'f'}}, // AuthMethod
		{pktType: CONNECT, props: []byte{
			22, 0, 1, 1}}, // AuthData
		{pktType: CONNECT, props: []byte{
			21, 0, 1, 'e', 22, 0, 1, 1, 22, 0, 1, 2}}, // AuthData
		{pktType: CONNECT, props: []byte{
			24, 0, 0, 0, 1, 24, 0, 0, 0, 2}}, // WillDelayInterval
		{pktType: CONNECT, props: []byte{
			1, 0, 1, 1}}, // PayloadFormatIndicator
		{pktType: CONNECT, props: []byte{
			1, 2}}, // PayloadFormatIndicator
		{pktType: CONNECT, props: []byte{
			2, 0, 0, 0, 1, 2, 0, 0, 0, 2}}, // MessageExpiryInterval
		{pktType: CONNECT, props: []byte{
			3, 0, 1, 'a', 3, 0, 1, 'b'}}, // ContentType
		{pktType: CONNECT, props: []byte{
			9, 0, 1, 1, 9, 0, 1, 2}}, // CorrelationData
		{pktType: PUBLISH, props: []byte{
			8, 0, 1, 'a', 8, 0, 1, 'b'}}, // ResponseTopic
		{pktType: PUBLISH, props: []byte{
			8, 0, 3, 'a', '/', '#'}}, // ResponseTopic
		{pktType: PUBLISH, props: []byte{
			8, 0, 0}}, // ResponseTopic
		{pktType: PUBLISH, props: []byte{
			35, 0, 0}}, // TopicAlias
		{pktType: PUBLISH, props: []byte{
			35, 0, 15, 35, 0, 16}}, // TopicAlias
	}

	for _, test := range testCases {
		n := fmt.Sprintf("%v-%v", test.pktType.String(), test.props[0])
		t.Run(n, func(t *testing.T) {
			// Property
			msg := []byte{byte(len(test.props))}
			msg = append(msg, test.props...)
			msg = append(msg, 0, 1, 'a') // client ID

			_, err := readProperties(bytes.NewBuffer(msg), test.pktType)
			assert.ErrorContains(t, err, ErrV5ProtocolError.Error())
		})
	}
}

func TestProperties_ReadPropertiesInvalidLength(t *testing.T) {
	_, err := readProperties(bytes.NewBuffer([]byte{}), CONNECT)
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

func TestProperties_Reset(t *testing.T) {
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

	props.Reset()

	assert.NotNil(t, props)
	assert.Nil(t, props.SessionExpiryInterval)
	assert.Nil(t, props.ReceiveMaximum)
	assert.Nil(t, props.MaximumQoS)
	assert.Nil(t, props.RetainAvailable)
	assert.Nil(t, props.MaximumPacketSize)
	assert.Nil(t, props.TopicAliasMaximum)
	assert.Nil(t, props.WildcardSubscriptionAvailable)
	assert.Nil(t, props.SubscriptionIDAvailable)
	assert.Nil(t, props.SharedSubscriptionAvailable)
	assert.Nil(t, props.ServerKeepAlive)
}

func TestProperties_SubscriptionIDValid(t *testing.T) {
	testCases := []int{0, 255, 65535, 4294967295}

	for _, id := range testCases {
		t.Run(fmt.Sprint(id), func(t *testing.T) {
			props := &Properties{SubscriptionIdentifier: new(int)}
			*props.SubscriptionIdentifier = id
			assert.Equal(t, id, props.SubscriptionID())
		})
	}
}

func TestProperties_SubscriptionIDInvalid(t *testing.T) {
	var props *Properties
	assert.Zero(t, props.SubscriptionID())

	props = &Properties{}
	assert.Zero(t, props.SubscriptionID())
}
