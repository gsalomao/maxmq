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

package mqtt

import (
	"testing"
	"time"

	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPublishHandlePacketQoS0(t *testing.T) {
	testCases := []struct {
		topic   string
		version packet.Version
		payload string
	}{
		{"data/temp/1", packet.MQTT31, "1"},
		{"data/temp/2", packet.MQTT311, "2"},
		{"data/temp/3", packet.MQTT50, "3"},
	}

	for _, tc := range testCases {
		t.Run(tc.topic, func(t *testing.T) {
			pubPkt := packet.NewPublish(1, tc.version, tc.topic, packet.QoS0, 0, 0, []byte(tc.payload), nil)
			msg := &message{id: 10, packetID: pubPkt.PacketID, packet: &pubPkt}

			gen := newIDGeneratorMock(int(msg.id))
			log := newLogger()
			ss := newSessionStore(gen, log)
			mt := newMetrics(true, log)
			ps := newPubSub(nil, ss, mt, log)
			h := newPublishHandler(ss, ps, gen, log)

			id := packet.ClientID("client-a")
			s := &session{clientID: id, version: tc.version}

			err := ss.saveSession(s)
			require.Nil(t, err)

			var replies []packet.Packet

			replies, err = h.handlePacket(id, &pubPkt)
			require.Nil(t, err)
			require.Empty(t, replies)
			gen.AssertExpectations(t)
		})
	}
}

func TestPublishHandlePacketQoS1(t *testing.T) {
	testCases := []struct {
		topic   string
		version packet.Version
		payload string
	}{
		{"data/temp/1", packet.MQTT31, "1"},
		{"data/temp/2", packet.MQTT311, "2"},
		{"data/temp/3", packet.MQTT50, "3"},
	}

	for _, tc := range testCases {
		t.Run(tc.topic, func(t *testing.T) {
			pubPkt := packet.NewPublish(1, tc.version, tc.topic, packet.QoS1, 0, 0, []byte(tc.payload), nil)
			msg := &message{id: 10, packetID: pubPkt.PacketID, packet: &pubPkt}

			gen := newIDGeneratorMock(int(msg.id))
			log := newLogger()
			ss := newSessionStore(gen, log)
			mt := newMetrics(true, log)
			ps := newPubSub(nil, ss, mt, log)
			h := newPublishHandler(ss, ps, gen, log)

			id := packet.ClientID("client-a")
			s := &session{clientID: id, version: tc.version}

			err := ss.saveSession(s)
			require.Nil(t, err)

			var replies []packet.Packet

			replies, err = h.handlePacket(id, &pubPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.PUBACK, reply.Type())

			pubAckPkt := reply.(*packet.PubAck)
			assert.Equal(t, pubPkt.PacketID, pubAckPkt.PacketID)
			assert.Equal(t, tc.version, pubAckPkt.Version)
			assert.Equal(t, packet.ReasonCodeV5Success, pubAckPkt.ReasonCode)
			gen.AssertExpectations(t)
		})
	}
}

func TestPublishHandlePacketQoS2(t *testing.T) {
	testCases := []struct {
		topic   string
		version packet.Version
		payload string
	}{
		{"data/temp/1", packet.MQTT31, "1"},
		{"data/temp/2", packet.MQTT311, "2"},
		{"data/temp/3", packet.MQTT50, "3"},
	}

	for _, tc := range testCases {
		t.Run(tc.topic, func(t *testing.T) {
			pubPkt := packet.NewPublish(1, tc.version, tc.topic, packet.QoS2, 0, 0, []byte(tc.payload), nil)
			msg := &message{id: 10, packetID: pubPkt.PacketID, packet: &pubPkt}

			gen := newIDGeneratorMock(int(msg.id))
			log := newLogger()
			ss := newSessionStore(gen, log)
			mt := newMetrics(true, log)
			ps := newPubSub(nil, ss, mt, log)
			h := newPublishHandler(ss, ps, gen, log)

			id := packet.ClientID("client-a")
			s := &session{
				clientID:      id,
				version:       tc.version,
				unAckMessages: make(map[packet.ID]*message),
			}

			err := ss.saveSession(s)
			require.Nil(t, err)

			var replies []packet.Packet

			replies, err = h.handlePacket(id, &pubPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.PUBREC, reply.Type())

			pubRecPkt := reply.(*packet.PubRec)
			assert.Equal(t, packet.ID(1), pubRecPkt.PacketID)
			assert.Equal(t, tc.version, pubRecPkt.Version)
			assert.Equal(t, packet.ReasonCodeV5Success, pubRecPkt.ReasonCode)

			require.Len(t, s.unAckMessages, 1)
			unAckMsg, ok := s.unAckMessages[msg.packetID]
			require.True(t, ok)
			assert.Equal(t, msg, unAckMsg)
			gen.AssertExpectations(t)
		})
	}
}

func TestPublishHandlePacketQoS2NoDuplication(t *testing.T) {
	testCases := []struct {
		topic   string
		version packet.Version
		payload string
	}{
		{"data/temp/1", packet.MQTT31, "1"},
		{"data/temp/2", packet.MQTT311, "2"},
		{"data/temp/3", packet.MQTT50, "3"},
	}

	for _, tc := range testCases {
		t.Run(tc.topic, func(t *testing.T) {
			pubPkt1 := packet.NewPublish(1, tc.version, tc.topic, packet.QoS2, 0, 0, []byte(tc.payload), nil)
			msg1 := &message{id: 10, packetID: pubPkt1.PacketID, packet: &pubPkt1}

			pubPkt2 := packet.NewPublish(2, tc.version, tc.topic, packet.QoS2, 0, 0, []byte(tc.payload), nil)
			msg2 := &message{id: 11, packetID: pubPkt2.PacketID, packet: &pubPkt2}

			gen := newIDGeneratorMock(int(msg1.id), int(msg2.id), int(msg2.id+1))
			log := newLogger()
			ss := newSessionStore(gen, log)
			mt := newMetrics(true, log)
			ps := newPubSub(nil, ss, mt, log)
			h := newPublishHandler(ss, ps, gen, log)

			id := packet.ClientID("client-a")
			s := &session{
				clientID:      id,
				version:       tc.version,
				unAckMessages: make(map[packet.ID]*message),
			}

			err := ss.saveSession(s)
			require.Nil(t, err)

			var replies []packet.Packet

			replies, err = h.handlePacket(id, &pubPkt1)
			require.Nil(t, err)
			require.Len(t, replies, 1)
			require.Equal(t, packet.PUBREC, replies[0].Type())
			pubRecPkt := replies[0].(*packet.PubRec)
			assert.Equal(t, msg1.packetID, pubRecPkt.PacketID)
			assert.Equal(t, packet.ReasonCodeV5Success, pubRecPkt.ReasonCode)

			replies, err = h.handlePacket(id, &pubPkt2)
			require.Nil(t, err)
			require.Len(t, replies, 1)
			require.Equal(t, packet.PUBREC, replies[0].Type())
			pubRecPkt = replies[0].(*packet.PubRec)
			assert.Equal(t, msg2.packetID, pubRecPkt.PacketID)
			assert.Equal(t, packet.ReasonCodeV5Success, pubRecPkt.ReasonCode)

			replies, err = h.handlePacket(id, &pubPkt1)
			require.Nil(t, err)
			require.Len(t, replies, 1)
			require.Equal(t, packet.PUBREC, replies[0].Type())
			pubRecPkt = replies[0].(*packet.PubRec)
			assert.Equal(t, msg1.packetID, pubRecPkt.PacketID)
			assert.Equal(t, packet.ReasonCodeV5Success, pubRecPkt.ReasonCode)

			require.Len(t, s.unAckMessages, 2)
			unAckMsg1, ok := s.unAckMessages[msg1.packetID]
			require.True(t, ok)
			assert.Equal(t, msg1, unAckMsg1)

			unAckMsg2, ok := s.unAckMessages[msg2.packetID]
			require.True(t, ok)
			assert.Equal(t, msg2, unAckMsg2)
			gen.AssertExpectations(t)
		})
	}
}

func TestPublishHandlePacketQoS2SamePacketIDNewMessage(t *testing.T) {
	testCases := []struct {
		topic   string
		version packet.Version
		payload string
	}{
		{"data/temp/1", packet.MQTT31, "1"},
		{"data/temp/2", packet.MQTT311, "2"},
		{"data/temp/3", packet.MQTT50, "3"},
	}

	for _, tc := range testCases {
		t.Run(tc.topic, func(t *testing.T) {
			id := packet.ClientID("client-a")
			s := &session{
				clientID:      id,
				version:       tc.version,
				unAckMessages: make(map[packet.ID]*message),
			}

			msg1 := &message{id: 10, packetID: 10, tries: 1, lastSent: time.Now().UnixMicro()}
			s.unAckMessages[msg1.packetID] = msg1

			pubPkt := packet.NewPublish(msg1.packetID, tc.version, tc.topic, packet.QoS2, 0, 0, []byte(tc.payload), nil)
			msg2 := &message{id: msg1.id + 1, packetID: pubPkt.PacketID, packet: &pubPkt}

			gen := newIDGeneratorMock(int(msg2.id))
			log := newLogger()
			ss := newSessionStore(gen, log)
			mt := newMetrics(true, log)
			ps := newPubSub(nil, ss, mt, log)
			h := newPublishHandler(ss, ps, gen, log)

			err := ss.saveSession(s)
			require.Nil(t, err)

			var replies []packet.Packet

			replies, err = h.handlePacket(id, &pubPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)
			require.Equal(t, packet.PUBREC, replies[0].Type())
			pubRecPkt := replies[0].(*packet.PubRec)
			assert.Equal(t, msg2.packetID, pubRecPkt.PacketID)
			assert.Equal(t, packet.ReasonCodeV5Success, pubRecPkt.ReasonCode)

			require.Len(t, s.unAckMessages, 1)
			unAckMsg, ok := s.unAckMessages[msg2.packetID]
			require.True(t, ok)
			assert.Equal(t, msg2, unAckMsg)
			gen.AssertExpectations(t)
		})
	}
}

func TestPublishHandlePacketReadSessionError(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			gen := newIDGeneratorMock()
			log := newLogger()
			ss := newSessionStore(gen, log)
			mt := newMetrics(true, log)
			ps := newPubSub(nil, ss, mt, log)
			h := newPublishHandler(ss, ps, gen, log)

			id := packet.ClientID("client-a")
			pubPkt := packet.NewPublish(1, tc, "t", packet.QoS0, 0, 0, []byte("d"), nil)

			replies, err := h.handlePacket(id, &pubPkt)
			assert.NotNil(t, err)
			assert.Empty(t, replies)
			gen.AssertExpectations(t)
		})
	}
}
