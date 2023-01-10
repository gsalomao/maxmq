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
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPublishInvalidPacketType(t *testing.T) {
	opts := options{packetType: DISCONNECT}
	pkt, err := newPacketPublish(opts)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestPublishInvalidQoS(t *testing.T) {
	opts := options{packetType: PUBLISH, version: MQTT311, controlFlags: 6}
	pkt, err := newPacketPublish(opts)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestPublishInvalidDup(t *testing.T) {
	opts := options{packetType: PUBLISH, version: MQTT50, controlFlags: 8}
	pkt, err := newPacketPublish(opts)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestPublishInvalidVersion(t *testing.T) {
	opts := options{packetType: PUBLISH}
	pkt, err := newPacketPublish(opts)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestPublishWrite(t *testing.T) {
	testCases := []struct {
		name    string
		id      ID
		version MQTTVersion
		topic   string
		qos     QoS
		retain  byte
		dup     byte
		payload string
	}{
		{name: "V3.1", id: 1, version: MQTT31, topic: "a", qos: QoS0, retain: 0,
			dup: 0, payload: "msg1"},
		{name: "V3.1.1", id: 2, version: MQTT311, topic: "a/b", qos: QoS1,
			retain: 1, dup: 1, payload: "msg2"},
		{name: "V5.0", id: 3, version: MQTT50, topic: "a/b/c", qos: QoS2,
			retain: 0, dup: 1, payload: "msg3"},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			ctrl := 0x30 | (test.dup << 3) |
				(byte(test.qos) << 1) | (test.retain)

			msg := []byte{ctrl, 0, 0, byte(len(test.topic))}
			msg = append(msg, []byte(test.topic)...)

			if test.qos > QoS0 {
				msg = append(msg, 0, byte(test.id))
			}
			if test.version == MQTT50 {
				msg = append(msg, 0)
			}

			msg = append(msg, []byte(test.payload)...)
			msg[1] = byte(len(msg) - 2)

			pkt := Publish{}
			pkt.PacketID = test.id
			pkt.Version = test.version
			pkt.TopicName = test.topic
			pkt.QoS = test.qos
			pkt.Retain = test.retain
			pkt.Dup = test.dup
			pkt.Payload = []byte(test.payload)

			buf := &bytes.Buffer{}
			wr := bufio.NewWriter(buf)

			err := pkt.Write(wr)
			assert.Nil(t, err)

			err = wr.Flush()
			assert.Nil(t, err)

			assert.Equal(t, msg, buf.Bytes())
		})
	}
}

func TestPublishWriteV5Properties(t *testing.T) {
	props := &Properties{PayloadFormatIndicator: new(byte)}
	*props.PayloadFormatIndicator = 1

	pkt := NewPublish(5, MQTT50, "a", QoS0, 0, 0,
		[]byte{'b'}, props)

	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)

	err := pkt.Write(wr)
	assert.Nil(t, err)

	err = wr.Flush()
	assert.Nil(t, err)

	msg := []byte{0x30, 7, 0, 1, 'a', 2, 1, 1, 'b'}
	assert.Equal(t, msg, buf.Bytes())
}

func TestPublishWriteFailure(t *testing.T) {
	pkt := NewPublish(5, MQTT50, "a", QoS0, 0, 0,
		[]byte{'b'}, nil)
	require.NotNil(t, pkt)

	conn, _ := net.Pipe()
	w := bufio.NewWriterSize(conn, 1)
	_ = conn.Close()

	err := pkt.Write(w)
	assert.NotNil(t, err)
}

func TestPublishWriteV5InvalidProperty(t *testing.T) {
	props := &Properties{MaximumQoS: new(byte)}
	*props.MaximumQoS = 1

	pkt := NewPublish(5, MQTT50, "a", QoS0, 0, 0,
		[]byte{'b'}, props)

	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)

	err := pkt.Write(wr)
	assert.NotNil(t, err)

	err = wr.Flush()
	assert.Nil(t, err)
	assert.Empty(t, buf)
}

func TestPublishRead(t *testing.T) {
	testCases := []struct {
		name    string
		id      ID
		version MQTTVersion
		topic   string
		qos     QoS
		retain  byte
		payload string
	}{
		{name: "3.1", id: 1, version: MQTT31, topic: "a", qos: QoS0, retain: 0,
			payload: "msg1"},
		{name: "3.1.1", id: 2, version: MQTT311, topic: "a/b", qos: QoS1,
			retain: 1, payload: "msg2"},
		{name: "5.0", id: 3, version: MQTT50, topic: "a/b/c", qos: QoS2,
			retain: 0, payload: "msg3"},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			msg := []byte{0, byte(len(test.topic))}
			msg = append(msg, []byte(test.topic)...)

			if test.qos > QoS0 {
				msg = append(msg, 0, byte(test.id))
			}
			if test.version == MQTT50 {
				msg = append(msg, 0)
			}

			msg = append(msg, []byte(test.payload)...)

			opts := options{
				packetType:      PUBLISH,
				controlFlags:    byte(test.qos)<<1 | test.retain,
				version:         test.version,
				remainingLength: len(msg),
			}

			pkt, err := newPacketPublish(opts)
			require.Nil(t, err)

			require.Equal(t, PUBLISH, pkt.Type())
			pubPkt, _ := pkt.(*Publish)

			err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
			require.Nil(t, err)

			assert.Equal(t, test.version, pubPkt.Version)
			if test.qos > QoS0 {
				assert.Equal(t, test.id, pubPkt.PacketID)
			}
			assert.Equal(t, test.topic, pubPkt.TopicName)
			assert.Equal(t, test.qos, pubPkt.QoS)
			assert.Equal(t, test.retain, pubPkt.Retain)
			assert.Equal(t, []byte(test.payload), pubPkt.Payload)
			assert.Nil(t, pubPkt.Properties)
		})
	}
}

func TestPublishReadInvalidLength(t *testing.T) {
	var msg []byte
	opts := options{
		packetType:      PUBLISH,
		version:         MQTT311,
		remainingLength: 10,
	}
	pkt, err := newPacketPublish(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	require.ErrorIs(t, err, ErrV5MalformedPacket)
}

func TestPublishReadV5InvalidProperty(t *testing.T) {
	msg := []byte{
		0, 3, 'a', '/', 'b', // topic
		0, 5, // packet ID
		3,          // property length
		0x13, 0, 5, // ServerKeepAlive
		'm', 's', 'g', // payload
	}
	opts := options{
		packetType:      PUBLISH,
		version:         MQTT50,
		remainingLength: len(msg),
		controlFlags:    0x2,
	}
	pkt, err := newPacketPublish(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	require.ErrorIs(t, err, ErrV5MalformedPacket)
}

func TestPublishReadNoTopic(t *testing.T) {
	msg := []byte{
		0, 10, // packet ID
	}
	opts := options{
		packetType:      PUBLISH,
		version:         MQTT31,
		controlFlags:    0x2,
		remainingLength: len(msg),
	}
	pkt, err := newPacketPublish(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	require.ErrorIs(t, err, ErrV5MalformedPacket)
}

func TestPublishReadNoPacketID(t *testing.T) {
	msg := []byte{
		0, 3, 'a', '/', 'b', // topic
	}
	opts := options{
		packetType:      PUBLISH,
		version:         MQTT311,
		controlFlags:    0x2,
		remainingLength: len(msg),
	}
	pkt, err := newPacketPublish(opts)
	require.Nil(t, err)

	err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
	require.ErrorIs(t, err, ErrV5MalformedPacket)
}

func TestPublishReadInvalidTopicName(t *testing.T) {
	testCases := []string{"#", "+", "a/#", "a/+", "a/b/#", "a/b/+"}

	for _, test := range testCases {
		t.Run(test, func(t *testing.T) {
			msg := []byte{0, byte(len(test))}
			msg = append(msg, []byte(test)...)
			msg = append(msg, 0, 30)

			opts := options{
				packetType:      PUBLISH,
				version:         MQTT311,
				remainingLength: len(msg),
			}
			pkt, err := newPacketPublish(opts)
			require.Nil(t, err)

			err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
			require.ErrorIs(t, err, ErrV5MalformedPacket)
		})
	}
}

func TestPublishSize(t *testing.T) {
	testCases := []struct {
		name      string
		version   MQTTVersion
		remainLen int
		size      int
	}{
		{name: "V3.1", version: MQTT31, remainLen: 10, size: 12},
		{name: "V3.1.1", version: MQTT311, remainLen: 10, size: 12},
		{name: "V5.0", version: MQTT50, remainLen: 11, size: 13},
		{name: "V5.0-Properties", version: MQTT50, remainLen: 13, size: 15},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			opts := options{
				packetType:        PUBLISH,
				version:           MQTT311,
				fixedHeaderLength: 2,
				remainingLength:   test.remainLen,
			}
			pkt, err := newPacketPublish(opts)
			require.Nil(t, err)
			require.NotNil(t, pkt)

			assert.Equal(t, test.size, pkt.Size())
		})
	}
}

func TestPublishTimestamp(t *testing.T) {
	msg := []byte{
		0, 3, 'a', '/', 'b', // topic
		0, 5, // packet ID
		0,             // property length
		'm', 's', 'g', // payload
	}
	opts := options{
		packetType:      PUBLISH,
		version:         MQTT50,
		remainingLength: len(msg),
	}
	pkt, err := newPacketPublish(opts)
	require.Nil(t, err)
	require.NotNil(t, pkt)
	assert.NotNil(t, pkt.Timestamp())
}

func TestPublishClone(t *testing.T) {
	testCases := []struct {
		name    string
		id      ID
		version MQTTVersion
		topic   string
		qos     QoS
		dup     uint8
		retain  uint8
		payload []byte
	}{
		{name: "V3.1", id: 1, version: MQTT31, topic: "temp/0", qos: QoS0,
			dup: 0, retain: 0, payload: []byte("data-0")},
		{name: "V3.1.1", id: 2, version: MQTT311, topic: "temp/1", qos: QoS1,
			dup: 0, retain: 1, payload: []byte("data-1")},
		{name: "V5.0", id: 3, version: MQTT50, topic: "temp/2", qos: QoS2,
			dup: 1, retain: 0, payload: []byte("data-2")},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			pkt1 := NewPublish(test.id, test.version, test.topic, test.qos,
				test.dup, test.retain, test.payload, nil)
			pkt2 := pkt1.Clone()

			assert.Equal(t, pkt1.PacketID, pkt2.PacketID)
			assert.Equal(t, pkt1.Version, pkt2.Version)
			assert.Equal(t, pkt1.TopicName, pkt2.TopicName)
			assert.Equal(t, pkt1.QoS, pkt2.QoS)
			assert.Equal(t, pkt1.Dup, pkt2.Dup)
			assert.Equal(t, pkt1.Retain, pkt2.Retain)
			assert.Equal(t, pkt1.Payload, pkt2.Payload)
		})
	}
}

func TestPublishCloneProperties(t *testing.T) {
	props := Properties{PayloadFormatIndicator: new(byte)}
	*props.PayloadFormatIndicator = 5

	pkt1 := NewPublish(1, MQTT50, "data", QoS1,
		1, 1, []byte("raw"), &props)
	pkt2 := pkt1.Clone()

	require.NotNil(t, pkt2.Properties)
	require.NotNil(t, pkt2.Properties.PayloadFormatIndicator)
	assert.Equal(t, *props.PayloadFormatIndicator,
		*pkt2.Properties.PayloadFormatIndicator)
}
