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

func TestPubRelHandlerHandlePacket(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			log := logger.New(&bytes.Buffer{}, nil, logger.LogFormatJson)
			h := NewPubRelHandler(st, subMgr, log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc, UnAckMessages: make(map[packet.ID]*Message)}

			pubPkt := packet.NewPublish(
				1,       /*id*/
				tc,      /*version*/
				"topic", /*topic*/
				packet.QoS2,
				0,              /*dup*/
				0,              /*retain*/
				[]byte("data"), /*payload*/
				nil,            /*props*/
			)

			msg := &Message{PacketID: pubPkt.PacketID, Packet: &pubPkt}
			s.UnAckMessages[msg.PacketID] = msg.Clone()

			st.On("ReadSession", id).Return(s, nil)
			st.On("SaveSession", s).Return(nil)
			subMgr.On("Publish", msg).Return(nil)

			pubRelPkt := packet.NewPubRel(
				pubPkt.PacketID,
				tc, /*version*/
				packet.ReasonCodeV5Success,
				nil, /*props*/
			)
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

func TestPubRelHandlerHandlePacketAlreadyReleased(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			log := logger.New(&bytes.Buffer{}, nil, logger.LogFormatJson)
			h := NewPubRelHandler(st, subMgr, log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc, UnAckMessages: make(map[packet.ID]*Message)}

			msg := &Message{PacketID: 1}
			s.UnAckMessages[msg.PacketID] = msg
			st.On("ReadSession", id).Return(s, nil)

			pubRelPkt := packet.NewPubRel(
				msg.PacketID,
				tc, /*version*/
				packet.ReasonCodeV5Success,
				nil, /*props*/
			)
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

func TestPubRelHandlerHandlePacketV3PacketNotFound(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			log := logger.New(&bytes.Buffer{}, nil, logger.LogFormatJson)
			h := NewPubRelHandler(st, subMgr, log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc, UnAckMessages: make(map[packet.ID]*Message)}
			st.On("ReadSession", id).Return(s, nil)

			pubRelPkt := packet.NewPubRel(
				10, /*id*/
				tc, /*version*/
				packet.ReasonCodeV5Success,
				nil, /*props*/
			)
			replies, err := h.HandlePacket(id, &pubRelPkt)
			assert.NotNil(t, err)
			assert.Len(t, replies, 0)
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
		})
	}
}

func TestPubRelHandlerHandlePacketV5PacketNotFound(t *testing.T) {
	st := &sessionStoreMock{}
	subMgr := &subscriptionMgrMock{}
	log := logger.New(&bytes.Buffer{}, nil, logger.LogFormatJson)
	h := NewPubRelHandler(st, subMgr, log)

	id := packet.ClientID("a")
	s := &Session{ClientID: id, Version: packet.MQTT50, UnAckMessages: make(map[packet.ID]*Message)}
	st.On("ReadSession", id).Return(s, nil)

	pubRelPkt := packet.NewPubRel(
		10, /*id*/
		packet.MQTT50,
		packet.ReasonCodeV5Success,
		nil, /*props*/
	)
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

func TestPubRelHandlerHandlePacketPublishError(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			log := logger.New(&bytes.Buffer{}, nil, logger.LogFormatJson)
			h := NewPubRelHandler(st, subMgr, log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc, UnAckMessages: make(map[packet.ID]*Message)}

			pubPkt := packet.NewPublish(
				1,       /*id*/
				tc,      /*version*/
				"topic", /*topic*/
				packet.QoS2,
				0,              /*dup*/
				0,              /*retain*/
				[]byte("data"), /*payload*/
				nil,            /*props*/
			)

			msg := &Message{PacketID: pubPkt.PacketID, Packet: &pubPkt}
			s.UnAckMessages[msg.PacketID] = msg.Clone()

			st.On("ReadSession", id).Return(s, nil)
			subMgr.On("Publish", msg).Return(errors.New("failed"))

			pubRelPkt := packet.NewPubRel(
				1,  /*id*/
				tc, /*version*/
				packet.ReasonCodeV5Success,
				nil, /*props*/
			)
			replies, err := h.HandlePacket(id, &pubRelPkt)
			assert.NotNil(t, err)
			assert.Empty(t, replies)
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
		})
	}
}

func TestPubRelHandlerHandlePacketReadSessionError(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			log := logger.New(&bytes.Buffer{}, nil, logger.LogFormatJson)
			h := NewPubRelHandler(st, subMgr, log)

			id := packet.ClientID("a")
			st.On("ReadSession", id).Return(nil, ErrSessionNotFound)

			pubRelPkt := packet.NewPubRel(
				1,  /*id*/
				tc, /*version*/
				packet.ReasonCodeV5Success,
				nil, /*props*/
			)
			replies, err := h.HandlePacket(id, &pubRelPkt)
			assert.NotNil(t, err)
			assert.Empty(t, replies)
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
		})
	}
}

func TestPubRelHandlerHandlePacketSaveSessionError(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			log := logger.New(&bytes.Buffer{}, nil, logger.LogFormatJson)
			h := NewPubRelHandler(st, subMgr, log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc, UnAckMessages: make(map[packet.ID]*Message)}

			pubPkt := packet.NewPublish(
				1,       /*id*/
				tc,      /*version*/
				"topic", /*topic*/
				packet.QoS2,
				0,              /*dup*/
				0,              /*retain*/
				[]byte("data"), /*payload*/
				nil,            /*props*/
			)

			msg := &Message{PacketID: pubPkt.PacketID, Packet: &pubPkt}
			s.UnAckMessages[msg.PacketID] = msg.Clone()

			st.On("ReadSession", id).Return(s, nil)
			st.On("SaveSession", s).Return(errors.New("failed"))
			subMgr.On("Publish", msg).Return(nil)

			pubRelPkt := packet.NewPubRel(
				1,  /*id*/
				tc, /*version*/
				packet.ReasonCodeV5Success,
				nil, /*props*/
			)
			replies, err := h.HandlePacket(id, &pubRelPkt)
			assert.NotNil(t, err)
			assert.Empty(t, replies)
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
		})
	}
}
