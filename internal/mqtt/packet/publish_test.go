// Copyright 2022-2023 The MaxMQ Authors
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
		id      ID
		version Version
		topic   string
		qos     QoS
		retain  byte
		dup     byte
		payload string
	}{
		{id: 1, version: MQTT31, topic: "a", qos: QoS0, retain: 0, dup: 0, payload: "msg1"},
		{id: 2, version: MQTT311, topic: "a/b", qos: QoS1, retain: 1, dup: 1, payload: "msg2"},
		{id: 3, version: MQTT50, topic: "a/b/c", qos: QoS2, retain: 0, dup: 1, payload: "msg3"},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.id), func(t *testing.T) {
			ctrl := 0x30 | (tc.dup << 3) | (byte(tc.qos) << 1) | (tc.retain)

			msg := []byte{ctrl, 0, 0, byte(len(tc.topic))}
			msg = append(msg, []byte(tc.topic)...)

			if tc.qos > QoS0 {
				msg = append(msg, 0, byte(tc.id))
			}
			if tc.version == MQTT50 {
				msg = append(msg, 0)
			}

			msg = append(msg, []byte(tc.payload)...)
			msg[1] = byte(len(msg) - 2)

			pkt := Publish{}
			pkt.PacketID = tc.id
			pkt.Version = tc.version
			pkt.TopicName = tc.topic
			pkt.QoS = tc.qos
			pkt.Retain = tc.retain
			pkt.Dup = tc.dup
			pkt.Payload = []byte(tc.payload)

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

	pkt := NewPublish(5 /*id*/, MQTT50, "a" /*topic*/, QoS0,
		0 /*dup*/, 0 /*retain*/, []byte{'b'} /*payload*/, props)

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
	pkt := NewPublish(5 /*id*/, MQTT50, "a" /*topic*/, QoS0,
		0 /*dup*/, 0 /*retain*/, []byte{'b'} /*payload*/, nil /*props*/)
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

	pkt := NewPublish(5 /*id*/, MQTT50, "a" /*topic*/, QoS0,
		0 /*dup*/, 0 /*retain*/, []byte{'b'} /*payload*/, props /*props*/)

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
		id      ID
		version Version
		topic   string
		qos     QoS
		retain  byte
		payload string
	}{
		{id: 1, version: MQTT31, topic: "a", qos: QoS0, retain: 0, payload: "msg1"},
		{id: 2, version: MQTT311, topic: "a/b", qos: QoS1, retain: 1, payload: "msg2"},
		{id: 3, version: MQTT50, topic: "a/b/c", qos: QoS2, retain: 0, payload: "msg3"},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.id), func(t *testing.T) {
			msg := []byte{0, byte(len(tc.topic))}
			msg = append(msg, []byte(tc.topic)...)

			if tc.qos > QoS0 {
				msg = append(msg, 0, byte(tc.id))
			}
			if tc.version == MQTT50 {
				msg = append(msg, 0)
			}
			msg = append(msg, []byte(tc.payload)...)

			opts := options{
				packetType:      PUBLISH,
				controlFlags:    byte(tc.qos)<<1 | tc.retain,
				version:         tc.version,
				remainingLength: len(msg),
			}

			pkt, err := newPacketPublish(opts)
			require.Nil(t, err)

			require.Equal(t, PUBLISH, pkt.Type())
			pubPkt, _ := pkt.(*Publish)

			err = pkt.Read(bufio.NewReader(bytes.NewBuffer(msg)))
			require.Nil(t, err)

			assert.Equal(t, tc.version, pubPkt.Version)
			if tc.qos > QoS0 {
				assert.Equal(t, tc.id, pubPkt.PacketID)
			}
			assert.Equal(t, tc.topic, pubPkt.TopicName)
			assert.Equal(t, tc.qos, pubPkt.QoS)
			assert.Equal(t, tc.retain, pubPkt.Retain)
			assert.Equal(t, []byte(tc.payload), pubPkt.Payload)
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

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			msg := []byte{0, byte(len(tc))}
			msg = append(msg, []byte(tc)...)
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
		version   Version
		remainLen int
		size      int
	}{
		{name: "V3.1", version: MQTT31, remainLen: 10, size: 12},
		{name: "V3.1.1", version: MQTT311, remainLen: 10, size: 12},
		{name: "V5.0", version: MQTT50, remainLen: 11, size: 13},
		{name: "V5.0-Properties", version: MQTT50, remainLen: 13, size: 15},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opts := options{
				packetType:        PUBLISH,
				version:           MQTT311,
				fixedHeaderLength: 2,
				remainingLength:   tc.remainLen,
			}
			pkt, err := newPacketPublish(opts)
			require.Nil(t, err)
			require.NotNil(t, pkt)

			assert.Equal(t, tc.size, pkt.Size())
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
		id      ID
		version Version
		topic   string
		qos     QoS
		dup     uint8
		retain  uint8
		payload []byte
	}{
		{id: 1, version: MQTT31, topic: "temp/0", qos: QoS0, dup: 0, retain: 0,
			payload: []byte("data-0")},
		{id: 2, version: MQTT311, topic: "temp/1", qos: QoS1, dup: 0, retain: 1,
			payload: []byte("data-1")},
		{id: 3, version: MQTT50, topic: "temp/2", qos: QoS2, dup: 1, retain: 0,
			payload: []byte("data-2")},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.id), func(t *testing.T) {
			pkt1 := NewPublish(tc.id, tc.version, tc.topic, tc.qos, tc.dup, tc.retain, tc.payload,
				nil /*props*/)
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

	pkt1 := NewPublish(1 /*id*/, MQTT50, "data" /*topic*/, QoS1,
		1 /*dup*/, 1 /*retain*/, []byte("raw") /*payload*/, &props)
	pkt2 := pkt1.Clone()

	require.NotNil(t, pkt2.Properties)
	require.NotNil(t, pkt2.Properties.PayloadFormatIndicator)
	assert.Equal(t, *props.PayloadFormatIndicator, *pkt2.Properties.PayloadFormatIndicator)
}
