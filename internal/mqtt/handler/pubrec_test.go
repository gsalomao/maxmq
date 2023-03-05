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
	"time"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPubRecHandlerHandlePacket(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewPubRecHandler(st, &log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc}
			st.On("ReadSession", id).Return(s, nil)
			st.On("SaveSession", s).Return(nil)

			pubPkt := packet.NewPublish(1 /*id*/, tc, "topic" /*topic*/, packet.QoS2,
				0 /*dup*/, 0 /*retain*/, []byte("data") /*payload*/, nil /*props*/)

			msg := &Message{PacketID: pubPkt.PacketID, Packet: &pubPkt, Tries: 1,
				LastSent: time.Now().UnixMicro()}
			s.InflightMessages.PushBack(msg)

			pubRecPkt := packet.NewPubRec(pubPkt.PacketID, pubPkt.Version, packet.ReasonCodeV5Success,
				nil /*props*/)
			replies, err := h.HandlePacket(id, &pubRecPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.PUBREL, reply.Type())

			pubRelPkt := reply.(*packet.PubRel)
			assert.Equal(t, pubPkt.PacketID, pubRelPkt.PacketID)
			assert.Equal(t, tc, pubRelPkt.Version)
			assert.Equal(t, packet.ReasonCodeV5Success, pubRelPkt.ReasonCode)

			require.Equal(t, 1, s.InflightMessages.Len())
			inflightMsg := s.InflightMessages.Front().Value.(*Message)
			assert.Equal(t, msg.ID, inflightMsg.ID)
			assert.Equal(t, msg.PacketID, inflightMsg.PacketID)
			assert.Nil(t, inflightMsg.Packet)
			assert.Equal(t, msg.Tries, inflightMsg.Tries)
			assert.Equal(t, msg.LastSent, inflightMsg.LastSent)
			st.AssertExpectations(t)
		})
	}
}

func TestPubRecHandlerHandlePacketAlreadyConfirmed(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewPubRecHandler(st, &log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc}
			st.On("ReadSession", id).Return(s, nil)

			msg := &Message{PacketID: 1, Tries: 1, LastSent: time.Now().UnixMicro()}
			s.InflightMessages.PushBack(msg)

			pubRecPkt := packet.NewPubRec(msg.PacketID, tc, packet.ReasonCodeV5Success, nil /*props*/)
			replies, err := h.HandlePacket(id, &pubRecPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)
			require.Equal(t, packet.PUBREL, replies[0].Type())

			replies, err = h.HandlePacket(id, &pubRecPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.PUBREL, reply.Type())

			pubRelPkt := reply.(*packet.PubRel)
			assert.Equal(t, msg.PacketID, pubRelPkt.PacketID)
			assert.Equal(t, tc, pubRelPkt.Version)
			assert.Equal(t, packet.ReasonCodeV5Success, pubRelPkt.ReasonCode)

			require.Equal(t, 1, s.InflightMessages.Len())

			inflightMsg := s.InflightMessages.Front().Value.(*Message)
			assert.Equal(t, msg.ID, inflightMsg.ID)
			assert.Equal(t, msg.PacketID, inflightMsg.PacketID)
			assert.Nil(t, inflightMsg.Packet)
			assert.Equal(t, msg.Tries, inflightMsg.Tries)
			assert.Equal(t, msg.LastSent, inflightMsg.LastSent)
			st.AssertExpectations(t)
		})
	}
}

func TestPubRecHandlerHandlePacketV3PacketIDNotFound(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewPubRecHandler(st, &log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc}
			st.On("ReadSession", id).Return(s, nil)

			pubRecPkt := packet.NewPubRec(2 /*id*/, tc, packet.ReasonCodeV5Success, nil /*props*/)
			replies, err := h.HandlePacket(id, &pubRecPkt)
			assert.NotNil(t, err)
			assert.Len(t, replies, 0)
			st.AssertExpectations(t)
		})
	}
}

func TestPubRecHandlerHandlePacketV5PacketIDNotFound(t *testing.T) {
	st := &sessionStoreMock{}
	log := logger.New(&bytes.Buffer{}, nil)
	h := NewPubRecHandler(st, &log)

	id := packet.ClientID("a")
	s := &Session{ClientID: id, Version: packet.MQTT50}
	st.On("ReadSession", id).Return(s, nil)

	pubRecPkt := packet.NewPubRec(2 /*id*/, packet.MQTT50, packet.ReasonCodeV5Success, nil /*props*/)
	replies, err := h.HandlePacket(id, &pubRecPkt)
	require.Nil(t, err)
	require.Len(t, replies, 1)

	reply := replies[0]
	require.Equal(t, packet.PUBREL, reply.Type())

	pubRelPkt := reply.(*packet.PubRel)
	assert.Equal(t, pubRecPkt.PacketID, pubRelPkt.PacketID)
	assert.Equal(t, packet.MQTT50, pubRelPkt.Version)
	assert.Equal(t, packet.ReasonCodeV5PacketIDNotFound, pubRelPkt.ReasonCode)
	st.AssertExpectations(t)
}

func TestPubRecHandlerHandlePacketReadSessionError(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewPubRecHandler(st, &log)
			id := packet.ClientID("a")

			st.On("ReadSession", id).Return(nil, ErrSessionNotFound)

			pubRecPkt := packet.NewPubRec(1 /*id*/, tc, packet.ReasonCodeV5Success, nil /*props*/)
			replies, err := h.HandlePacket(id, &pubRecPkt)
			assert.NotNil(t, err)
			assert.Empty(t, replies)
			st.AssertExpectations(t)
		})
	}
}

func TestPubRecHandlerHandlePacketSaveSessionError(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewPubRecHandler(st, &log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: packet.MQTT50}

			st.On("ReadSession", id).Return(s, nil)
			st.On("SaveSession", s).Return(errors.New("failed"))

			pubPkt := packet.NewPublish(1 /*id*/, tc, "topic" /*topic*/, packet.QoS2,
				0 /*dup*/, 0 /*retain*/, []byte("data") /*payload*/, nil /*props*/)

			msg := &Message{PacketID: pubPkt.PacketID, Packet: &pubPkt, Tries: 1,
				LastSent: time.Now().UnixMicro()}
			s.InflightMessages.PushBack(msg)

			pubRecPkt := packet.NewPubRec(1 /*id*/, tc, packet.ReasonCodeV5Success, nil /*props*/)
			replies, err := h.HandlePacket(id, &pubRecPkt)
			assert.NotNil(t, err)
			assert.Empty(t, replies)
			st.AssertExpectations(t)
		})
	}
}
