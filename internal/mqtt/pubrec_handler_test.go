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

func TestPubRecHandlePacket(t *testing.T) {
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
			h := newPubRecHandler(ss, log)

			id := packet.ClientID("client-a")
			s := &session{clientID: id, version: tc}

			err := ss.saveSession(s)
			require.Nil(t, err)

			pubPkt := packet.NewPublish(1, tc, "t", packet.QoS2, 0, 0, []byte("d"), nil)
			msg := &message{
				packetID: pubPkt.PacketID,
				packet:   &pubPkt,
				tries:    1,
				lastSent: time.Now().UnixMicro(),
			}
			s.inflightMessages.PushBack(msg)
			pubRecPkt := packet.NewPubRec(pubPkt.PacketID, pubPkt.Version, packet.ReasonCodeV5Success, nil)
			var replies []packet.Packet

			replies, err = h.handlePacket(id, &pubRecPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.PUBREL, reply.Type())

			pubRelPkt := reply.(*packet.PubRel)
			assert.Equal(t, pubPkt.PacketID, pubRelPkt.PacketID)
			assert.Equal(t, tc, pubRelPkt.Version)
			assert.Equal(t, packet.ReasonCodeV5Success, pubRelPkt.ReasonCode)

			require.Equal(t, 1, s.inflightMessages.Len())
			inflightMsg := s.inflightMessages.Front().Value.(*message)
			assert.Equal(t, msg.id, inflightMsg.id)
			assert.Equal(t, msg.packetID, inflightMsg.packetID)
			assert.Nil(t, inflightMsg.packet)
			assert.Equal(t, msg.tries, inflightMsg.tries)
			assert.Equal(t, msg.lastSent, inflightMsg.lastSent)
		})
	}
}

func TestPubRecHandlePacketAlreadyConfirmed(t *testing.T) {
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
			h := newPubRecHandler(ss, log)

			id := packet.ClientID("client-a")
			s := &session{clientID: id, version: tc}

			err := ss.saveSession(s)
			require.Nil(t, err)

			msg := &message{packetID: 1, tries: 1, lastSent: time.Now().UnixMicro()}
			s.inflightMessages.PushBack(msg)
			pubRecPkt := packet.NewPubRec(msg.packetID, tc, packet.ReasonCodeV5Success, nil)
			var replies []packet.Packet

			replies, err = h.handlePacket(id, &pubRecPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)
			require.Equal(t, packet.PUBREL, replies[0].Type())

			replies, err = h.handlePacket(id, &pubRecPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.PUBREL, reply.Type())

			pubRelPkt := reply.(*packet.PubRel)
			assert.Equal(t, msg.packetID, pubRelPkt.PacketID)
			assert.Equal(t, tc, pubRelPkt.Version)
			assert.Equal(t, packet.ReasonCodeV5Success, pubRelPkt.ReasonCode)

			require.Equal(t, 1, s.inflightMessages.Len())
			inflightMsg := s.inflightMessages.Front().Value.(*message)
			assert.Equal(t, msg.id, inflightMsg.id)
			assert.Equal(t, msg.packetID, inflightMsg.packetID)
			assert.Nil(t, inflightMsg.packet)
			assert.Equal(t, msg.tries, inflightMsg.tries)
			assert.Equal(t, msg.lastSent, inflightMsg.lastSent)
		})
	}
}

func TestPubRecHandlePacketV3PacketIDNotFound(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			gen := newIDGeneratorMock()
			log := newLogger()
			ss := newSessionStore(gen, log)
			h := newPubRecHandler(ss, log)

			id := packet.ClientID("client-a")
			s := &session{clientID: id, version: tc}

			err := ss.saveSession(s)
			require.Nil(t, err)

			pubRecPkt := packet.NewPubRec(2, tc, packet.ReasonCodeV5Success, nil)
			var replies []packet.Packet

			replies, err = h.handlePacket(id, &pubRecPkt)
			assert.NotNil(t, err)
			assert.Len(t, replies, 0)
		})
	}
}

func TestPubRecHandlePacketV5PacketIDNotFound(t *testing.T) {
	gen := newIDGeneratorMock()
	log := newLogger()
	ss := newSessionStore(gen, log)
	h := newPubRecHandler(ss, log)

	id := packet.ClientID("client-a")
	s := &session{clientID: id, version: packet.MQTT50}

	err := ss.saveSession(s)
	require.Nil(t, err)

	pubRecPkt := packet.NewPubRec(2, packet.MQTT50, packet.ReasonCodeV5Success, nil)
	var replies []packet.Packet

	replies, err = h.handlePacket(id, &pubRecPkt)
	require.Nil(t, err)
	require.Len(t, replies, 1)

	reply := replies[0]
	require.Equal(t, packet.PUBREL, reply.Type())

	pubRelPkt := reply.(*packet.PubRel)
	assert.Equal(t, pubRecPkt.PacketID, pubRelPkt.PacketID)
	assert.Equal(t, packet.MQTT50, pubRelPkt.Version)
	assert.Equal(t, packet.ReasonCodeV5PacketIDNotFound, pubRelPkt.ReasonCode)
}

func TestPubRecHandlePacketReadSessionError(t *testing.T) {
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
			h := newPubRecHandler(ss, log)

			id := packet.ClientID("client-a")
			pubRecPkt := packet.NewPubRec(1, tc, packet.ReasonCodeV5Success, nil)

			replies, err := h.handlePacket(id, &pubRecPkt)
			assert.NotNil(t, err)
			assert.Empty(t, replies)
		})
	}
}
