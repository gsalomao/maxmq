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

package handler

import (
	"bytes"
	"errors"
	"testing"

	"github.com/gsalomao/maxmq/internal/logger"
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
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			log := logger.New(&bytes.Buffer{}, nil, logger.Json)
			h := NewPubRel(st, subMgr, log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc, UnAckMessages: make(map[packet.ID]*Message)}

			pubPkt := packet.NewPublish(1, tc, "t", packet.QoS2, 0, 0, []byte("d"), nil)
			msg := &Message{PacketID: pubPkt.PacketID, Packet: &pubPkt}
			s.UnAckMessages[msg.PacketID] = msg.Clone()
			pubRelPkt := packet.NewPubRel(pubPkt.PacketID, tc, packet.ReasonCodeV5Success, nil)

			st.On("ReadSession", id).Return(s, nil)
			st.On("SaveSession", s).Return(nil)
			subMgr.On("Publish", msg).Return(nil)

			replies, err := h.HandlePacket(id, &pubRelPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.PUBCOMP, reply.Type())

			pubCompPkt := reply.(*packet.PubComp)
			assert.Equal(t, pubPkt.PacketID, pubCompPkt.PacketID)
			assert.Equal(t, tc, pubCompPkt.Version)
			assert.Equal(t, packet.ReasonCodeV5Success, pubCompPkt.ReasonCode)

			require.Len(t, s.UnAckMessages, 1)
			unAckMsg, ok := s.UnAckMessages[msg.PacketID]
			require.True(t, ok)
			assert.Equal(t, msg.PacketID, unAckMsg.PacketID)
			assert.Nil(t, unAckMsg.Packet)
			assert.NotZero(t, unAckMsg.LastSent)
			assert.Equal(t, 1, unAckMsg.Tries)
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
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
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			log := logger.New(&bytes.Buffer{}, nil, logger.Json)
			h := NewPubRel(st, subMgr, log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc, UnAckMessages: make(map[packet.ID]*Message)}

			msg := &Message{PacketID: 1}
			s.UnAckMessages[msg.PacketID] = msg
			pubRelPkt := packet.NewPubRel(msg.PacketID, tc, packet.ReasonCodeV5Success, nil)
			st.On("ReadSession", id).Return(s, nil)

			replies, err := h.HandlePacket(id, &pubRelPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.PUBCOMP, reply.Type())

			pubCompPkt := reply.(*packet.PubComp)
			assert.Equal(t, msg.PacketID, pubCompPkt.PacketID)
			assert.Equal(t, tc, pubCompPkt.Version)
			assert.Equal(t, packet.ReasonCodeV5Success, pubCompPkt.ReasonCode)

			require.Len(t, s.UnAckMessages, 1)
			unAckMsg, ok := s.UnAckMessages[msg.PacketID]
			require.True(t, ok)
			assert.Equal(t, msg.PacketID, unAckMsg.PacketID)
			assert.Nil(t, unAckMsg.Packet)
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
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
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			log := logger.New(&bytes.Buffer{}, nil, logger.Json)
			h := NewPubRel(st, subMgr, log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc, UnAckMessages: make(map[packet.ID]*Message)}
			pubRelPkt := packet.NewPubRel(10, tc, packet.ReasonCodeV5Success, nil)
			st.On("ReadSession", id).Return(s, nil)

			replies, err := h.HandlePacket(id, &pubRelPkt)
			assert.NotNil(t, err)
			assert.Len(t, replies, 0)
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
		})
	}
}

func TestPubRelHandlePacketV5PacketNotFound(t *testing.T) {
	st := &sessionStoreMock{}
	subMgr := &subscriptionMgrMock{}
	log := logger.New(&bytes.Buffer{}, nil, logger.Json)
	h := NewPubRel(st, subMgr, log)

	id := packet.ClientID("a")
	s := &Session{ClientID: id, Version: packet.MQTT50, UnAckMessages: make(map[packet.ID]*Message)}
	pubRelPkt := packet.NewPubRel(10, packet.MQTT50, packet.ReasonCodeV5Success, nil)
	st.On("ReadSession", id).Return(s, nil)

	replies, err := h.HandlePacket(id, &pubRelPkt)
	require.Nil(t, err)
	require.Len(t, replies, 1)

	reply := replies[0]
	require.Equal(t, packet.PUBCOMP, reply.Type())

	pubCompPkt := reply.(*packet.PubComp)
	assert.Equal(t, pubRelPkt.PacketID, pubCompPkt.PacketID)
	assert.Equal(t, packet.MQTT50, pubCompPkt.Version)
	assert.Equal(t, packet.ReasonCodeV5PacketIDNotFound, pubCompPkt.ReasonCode)
	st.AssertExpectations(t)
	subMgr.AssertExpectations(t)
}

func TestPubRelHandlePacketPublishError(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			log := logger.New(&bytes.Buffer{}, nil, logger.Json)
			h := NewPubRel(st, subMgr, log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc, UnAckMessages: make(map[packet.ID]*Message)}
			pubPkt := packet.NewPublish(1, tc, "t", packet.QoS2, 0, 0, []byte("d"), nil)

			msg := &Message{PacketID: pubPkt.PacketID, Packet: &pubPkt}
			s.UnAckMessages[msg.PacketID] = msg.Clone()
			pubRelPkt := packet.NewPubRel(1, tc, packet.ReasonCodeV5Success, nil)

			st.On("ReadSession", id).Return(s, nil)
			subMgr.On("Publish", msg).Return(errors.New("failed"))

			replies, err := h.HandlePacket(id, &pubRelPkt)
			assert.NotNil(t, err)
			assert.Empty(t, replies)
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
		})
	}
}

func TestPubRelHandlePacketReadSessionError(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			log := logger.New(&bytes.Buffer{}, nil, logger.Json)
			h := NewPubRel(st, subMgr, log)

			id := packet.ClientID("a")
			pubRelPkt := packet.NewPubRel(1, tc, packet.ReasonCodeV5Success, nil)
			st.On("ReadSession", id).Return(nil, ErrSessionNotFound)

			replies, err := h.HandlePacket(id, &pubRelPkt)
			assert.NotNil(t, err)
			assert.Empty(t, replies)
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
		})
	}
}

func TestPubRelHandlePacketSaveSessionError(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			log := logger.New(&bytes.Buffer{}, nil, logger.Json)
			h := NewPubRel(st, subMgr, log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc, UnAckMessages: make(map[packet.ID]*Message)}
			pubPkt := packet.NewPublish(1, tc, "t", packet.QoS2, 0, 0, []byte("d"), nil)

			msg := &Message{PacketID: pubPkt.PacketID, Packet: &pubPkt}
			s.UnAckMessages[msg.PacketID] = msg.Clone()
			pubRelPkt := packet.NewPubRel(1, tc, packet.ReasonCodeV5Success, nil)

			st.On("ReadSession", id).Return(s, nil)
			st.On("SaveSession", s).Return(errors.New("failed"))
			subMgr.On("Publish", msg).Return(nil)

			replies, err := h.HandlePacket(id, &pubRelPkt)
			assert.NotNil(t, err)
			assert.Empty(t, replies)
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
		})
	}
}
