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

	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPubRelHandlePacket(t *testing.T) {
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
			h := newPubRelHandler(ss, ps, log)

			id := packet.ClientID("client-a")
			s := &session{clientID: id, version: tc, unAckMessages: make(map[packet.ID]*message)}

			err := ss.saveSession(s)
			require.Nil(t, err)

			pubPkt := packet.NewPublish(1, tc, "t", packet.QoS2, 0, 0, []byte("d"), nil)
			msg := &message{packetID: pubPkt.PacketID, packet: &pubPkt}
			s.unAckMessages[msg.packetID] = msg.clone()

			pubRelPkt := packet.NewPubRel(pubPkt.PacketID, tc, packet.ReasonCodeV5Success, nil)
			var replies []packet.Packet

			replies, err = h.handlePacket(id, &pubRelPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.PUBCOMP, reply.Type())

			pubCompPkt := reply.(*packet.PubComp)
			assert.Equal(t, pubPkt.PacketID, pubCompPkt.PacketID)
			assert.Equal(t, tc, pubCompPkt.Version)
			assert.Equal(t, packet.ReasonCodeV5Success, pubCompPkt.ReasonCode)

			require.Len(t, s.unAckMessages, 1)
			unAckMsg, ok := s.unAckMessages[msg.packetID]
			require.True(t, ok)
			assert.Equal(t, msg.packetID, unAckMsg.packetID)
			assert.Nil(t, unAckMsg.packet)
			assert.NotZero(t, unAckMsg.lastSent)
			assert.Equal(t, 1, unAckMsg.tries)
		})
	}
}

func TestPubRelHandlePacketAlreadyReleased(t *testing.T) {
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
			h := newPubRelHandler(ss, ps, log)

			id := packet.ClientID("client-a")
			s := &session{clientID: id, version: tc, unAckMessages: make(map[packet.ID]*message)}

			err := ss.saveSession(s)
			require.Nil(t, err)

			msg := &message{packetID: 1}
			s.unAckMessages[msg.packetID] = msg

			pubRelPkt := packet.NewPubRel(msg.packetID, tc, packet.ReasonCodeV5Success, nil)
			var replies []packet.Packet

			replies, err = h.handlePacket(id, &pubRelPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.PUBCOMP, reply.Type())

			pubCompPkt := reply.(*packet.PubComp)
			assert.Equal(t, msg.packetID, pubCompPkt.PacketID)
			assert.Equal(t, tc, pubCompPkt.Version)
			assert.Equal(t, packet.ReasonCodeV5Success, pubCompPkt.ReasonCode)

			require.Len(t, s.unAckMessages, 1)
			unAckMsg, ok := s.unAckMessages[msg.packetID]
			require.True(t, ok)
			assert.Equal(t, msg.packetID, unAckMsg.packetID)
			assert.Nil(t, unAckMsg.packet)
		})
	}
}

func TestPubRelHandlePacketV3PacketNotFound(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			gen := newIDGeneratorMock()
			log := newLogger()
			ss := newSessionStore(gen, log)
			mt := newMetrics(true, log)
			ps := newPubSub(nil, ss, mt, log)
			h := newPubRelHandler(ss, ps, log)

			id := packet.ClientID("client-a")
			s := &session{clientID: id, version: tc, unAckMessages: make(map[packet.ID]*message)}

			err := ss.saveSession(s)
			require.Nil(t, err)

			pubRelPkt := packet.NewPubRel(10, tc, packet.ReasonCodeV5Success, nil)
			var replies []packet.Packet

			replies, err = h.handlePacket(id, &pubRelPkt)
			assert.NotNil(t, err)
			assert.Len(t, replies, 0)
		})
	}
}

func TestPubRelHandlePacketV5PacketNotFound(t *testing.T) {
	gen := newIDGeneratorMock()
	log := newLogger()
	ss := newSessionStore(gen, log)
	mt := newMetrics(true, log)
	ps := newPubSub(nil, ss, mt, log)
	h := newPubRelHandler(ss, ps, log)

	id := packet.ClientID("client-a")
	s := &session{clientID: id, version: packet.MQTT50, unAckMessages: make(map[packet.ID]*message)}

	err := ss.saveSession(s)
	require.Nil(t, err)

	pubRelPkt := packet.NewPubRel(10, packet.MQTT50, packet.ReasonCodeV5Success, nil)
	var replies []packet.Packet

	replies, err = h.handlePacket(id, &pubRelPkt)
	require.Nil(t, err)
	require.Len(t, replies, 1)

	reply := replies[0]
	require.Equal(t, packet.PUBCOMP, reply.Type())

	pubCompPkt := reply.(*packet.PubComp)
	assert.Equal(t, pubRelPkt.PacketID, pubCompPkt.PacketID)
	assert.Equal(t, packet.MQTT50, pubCompPkt.Version)
	assert.Equal(t, packet.ReasonCodeV5PacketIDNotFound, pubCompPkt.ReasonCode)
}

func TestPubRelHandlePacketReadSessionError(t *testing.T) {
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
			h := newPubRelHandler(ss, ps, log)

			id := packet.ClientID("client-a")
			pubRelPkt := packet.NewPubRel(1, tc, packet.ReasonCodeV5Success, nil)

			replies, err := h.handlePacket(id, &pubRelPkt)
			assert.NotNil(t, err)
			assert.Empty(t, replies)
		})
	}
}
