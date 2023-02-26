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
	"fmt"
	"testing"
	"time"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPublishHandlerHandlePacketQoS0(t *testing.T) {
	testCases := []struct {
		topic   string
		version packet.Version
		payload string
	}{
		{topic: "data/temp/1", version: packet.MQTT31, payload: "1"},
		{topic: "data/temp/2", version: packet.MQTT311, payload: "2"},
		{topic: "data/temp/3", version: packet.MQTT50, payload: "3"},
	}

	for _, tc := range testCases {
		t.Run(tc.topic, func(t *testing.T) {
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			idGen := &messageIDGenMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewPublishHandler(st, subMgr, idGen, &log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc.version}

			pubPkt := packet.NewPublish(1, tc.version, tc.topic, packet.QoS0,
				0, 0, []byte(tc.payload), nil)
			msg := &Message{ID: 10, PacketID: pubPkt.PacketID, Packet: &pubPkt}

			st.On("ReadSession", id).Return(s, nil)
			idGen.On("NextID").Return(uint64(msg.ID))
			subMgr.On("Publish", msg).Return(nil)

			replies, err := h.HandlePacket(id, &pubPkt)
			require.Nil(t, err)
			require.Empty(t, replies)
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
			idGen.AssertExpectations(t)
		})
	}
}

func TestPublishHandlerHandlePacketQoS1(t *testing.T) {
	testCases := []struct {
		topic   string
		version packet.Version
		payload string
	}{
		{topic: "data/temp/1", version: packet.MQTT31, payload: "1"},
		{topic: "data/temp/2", version: packet.MQTT311, payload: "2"},
		{topic: "data/temp/3", version: packet.MQTT50, payload: "3"},
	}

	for _, tc := range testCases {
		t.Run(tc.topic, func(t *testing.T) {
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			idGen := &messageIDGenMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewPublishHandler(st, subMgr, idGen, &log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc.version}

			pubPkt := packet.NewPublish(1, tc.version, tc.topic, packet.QoS1,
				0, 0, []byte(tc.payload), nil)
			msg := &Message{ID: 10, PacketID: pubPkt.PacketID, Packet: &pubPkt}

			st.On("ReadSession", id).Return(s, nil)
			idGen.On("NextID").Return(uint64(msg.ID))
			subMgr.On("Publish", msg).Return(nil)

			replies, err := h.HandlePacket(id, &pubPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.PUBACK, reply.Type())

			pubAckPkt := reply.(*packet.PubAck)
			assert.Equal(t, pubPkt.PacketID, pubAckPkt.PacketID)
			assert.Equal(t, tc.version, pubAckPkt.Version)
			assert.Equal(t, packet.ReasonCodeV5Success, pubAckPkt.ReasonCode)
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
			idGen.AssertExpectations(t)
		})
	}
}

func TestPublishHandlerHandlePacketQoS2(t *testing.T) {
	testCases := []struct {
		topic   string
		version packet.Version
		payload string
	}{
		{topic: "data/temp/1", version: packet.MQTT31, payload: "1"},
		{topic: "data/temp/2", version: packet.MQTT311, payload: "2"},
		{topic: "data/temp/3", version: packet.MQTT50, payload: "3"},
	}

	for _, tc := range testCases {
		t.Run(tc.topic, func(t *testing.T) {
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			idGen := &messageIDGenMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewPublishHandler(st, subMgr, idGen, &log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc.version,
				UnAckMessages: make(map[packet.ID]*Message)}

			pubPkt := packet.NewPublish(1, tc.version, tc.topic, packet.QoS2,
				0, 0, []byte(tc.payload), nil)
			msg := &Message{ID: 10, PacketID: pubPkt.PacketID, Packet: &pubPkt}

			st.On("ReadSession", id).Return(s, nil)
			st.On("SaveSession", s).Return(nil)
			idGen.On("NextID").Return(uint64(msg.ID))

			replies, err := h.HandlePacket(id, &pubPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.PUBREC, reply.Type())

			pubRecPkt := reply.(*packet.PubRec)
			assert.Equal(t, packet.ID(1), pubRecPkt.PacketID)
			assert.Equal(t, tc.version, pubRecPkt.Version)
			assert.Equal(t, packet.ReasonCodeV5Success, pubRecPkt.ReasonCode)

			require.Len(t, s.UnAckMessages, 1)
			unAckMsg, ok := s.UnAckMessages[msg.PacketID]
			require.True(t, ok)
			assert.Equal(t, msg, unAckMsg)
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
			idGen.AssertExpectations(t)
		})
	}
}

func TestPublishHandlerHandlePacketQoS2NoDuplication(t *testing.T) {
	testCases := []struct {
		topic   string
		version packet.Version
		payload string
	}{
		{topic: "data/temp/1", version: packet.MQTT31, payload: "1"},
		{topic: "data/temp/2", version: packet.MQTT311, payload: "2"},
		{topic: "data/temp/3", version: packet.MQTT50, payload: "3"},
	}

	for _, tc := range testCases {
		t.Run(tc.topic, func(t *testing.T) {
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			idGen := &messageIDGenMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewPublishHandler(st, subMgr, idGen, &log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc.version,
				UnAckMessages: make(map[packet.ID]*Message)}

			pubPkt1 := packet.NewPublish(1, tc.version, tc.topic,
				packet.QoS2, 0, 0, []byte(tc.payload), nil)
			msg1 := &Message{ID: 10, PacketID: pubPkt1.PacketID,
				Packet: &pubPkt1}

			pubPkt2 := packet.NewPublish(2, tc.version, tc.topic,
				packet.QoS2, 0, 0, []byte(tc.payload), nil)
			msg2 := &Message{ID: 11, PacketID: pubPkt2.PacketID,
				Packet: &pubPkt2}

			st.On("ReadSession", id).Return(s, nil)
			st.On("SaveSession", s).Return(nil)
			idGen.On("NextID").Return(uint64(msg1.ID)).Once()
			idGen.On("NextID").Return(uint64(msg2.ID)).Once()
			idGen.On("NextID").Return(uint64(msg2.ID + 1)).Once()

			replies, err := h.HandlePacket(id, &pubPkt1)
			require.Nil(t, err)
			require.Len(t, replies, 1)
			require.Equal(t, packet.PUBREC, replies[0].Type())
			pubRecPkt := replies[0].(*packet.PubRec)
			assert.Equal(t, msg1.PacketID, pubRecPkt.PacketID)
			assert.Equal(t, packet.ReasonCodeV5Success, pubRecPkt.ReasonCode)

			replies, err = h.HandlePacket(id, &pubPkt2)
			require.Nil(t, err)
			require.Len(t, replies, 1)
			require.Equal(t, packet.PUBREC, replies[0].Type())
			pubRecPkt = replies[0].(*packet.PubRec)
			assert.Equal(t, msg2.PacketID, pubRecPkt.PacketID)
			assert.Equal(t, packet.ReasonCodeV5Success, pubRecPkt.ReasonCode)

			replies, err = h.HandlePacket(id, &pubPkt1)
			require.Nil(t, err)
			require.Len(t, replies, 1)
			require.Equal(t, packet.PUBREC, replies[0].Type())
			pubRecPkt = replies[0].(*packet.PubRec)
			assert.Equal(t, msg1.PacketID, pubRecPkt.PacketID)
			assert.Equal(t, packet.ReasonCodeV5Success, pubRecPkt.ReasonCode)

			require.Len(t, s.UnAckMessages, 2)
			unAckMsg1, ok := s.UnAckMessages[msg1.PacketID]
			require.True(t, ok)
			assert.Equal(t, msg1, unAckMsg1)

			unAckMsg2, ok := s.UnAckMessages[msg2.PacketID]
			require.True(t, ok)
			assert.Equal(t, msg2, unAckMsg2)
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
			idGen.AssertExpectations(t)
		})
	}
}

func TestPublishHandlerHandlePacketQoS2SamePacketIDNewMessage(t *testing.T) {
	testCases := []struct {
		topic   string
		version packet.Version
		payload string
	}{
		{topic: "data/temp/1", version: packet.MQTT31, payload: "1"},
		{topic: "data/temp/2", version: packet.MQTT311, payload: "2"},
		{topic: "data/temp/3", version: packet.MQTT50, payload: "3"},
	}

	for _, tc := range testCases {
		t.Run(tc.topic, func(t *testing.T) {
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			idGen := &messageIDGenMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewPublishHandler(st, subMgr, idGen, &log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc.version,
				UnAckMessages: make(map[packet.ID]*Message)}

			msg1 := &Message{ID: 10, PacketID: 10, Tries: 1,
				LastSent: time.Now().UnixMicro()}
			s.UnAckMessages[msg1.PacketID] = msg1

			pubPkt := packet.NewPublish(msg1.PacketID, tc.version, tc.topic,
				packet.QoS2, 0, 0, []byte(tc.payload), nil)
			msg2 := &Message{ID: msg1.ID + 1, PacketID: pubPkt.PacketID,
				Packet: &pubPkt}

			st.On("ReadSession", id).Return(s, nil)
			st.On("SaveSession", s).Return(nil)
			idGen.On("NextID").Return(uint64(msg2.ID))

			replies, err := h.HandlePacket(id, &pubPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)
			require.Equal(t, packet.PUBREC, replies[0].Type())
			pubRecPkt := replies[0].(*packet.PubRec)
			assert.Equal(t, msg2.PacketID, pubRecPkt.PacketID)
			assert.Equal(t, packet.ReasonCodeV5Success, pubRecPkt.ReasonCode)

			require.Len(t, s.UnAckMessages, 1)
			unAckMsg, ok := s.UnAckMessages[msg2.PacketID]
			require.True(t, ok)
			assert.Equal(t, msg2, unAckMsg)
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
			idGen.AssertExpectations(t)
		})
	}
}

func TestPublishHandlerHandlePacketReadSessionError(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			idGen := &messageIDGenMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewPublishHandler(st, subMgr, idGen, &log)
			id := packet.ClientID("a")

			pubPkt := packet.NewPublish(1, tc, "data", packet.QoS0,
				0, 0, []byte("data"), nil)

			st.On("ReadSession", id).
				Return(nil, ErrSessionNotFound)

			replies, err := h.HandlePacket(id, &pubPkt)
			assert.NotNil(t, err)
			assert.Empty(t, replies)
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
			idGen.AssertExpectations(t)
		})
	}
}

func TestPublishHandlerHandlePacketPublishMessageError(t *testing.T) {
	testCases := []struct {
		version packet.Version
		qos     packet.QoS
	}{
		{version: packet.MQTT31, qos: packet.QoS0},
		{version: packet.MQTT311, qos: packet.QoS0},
		{version: packet.MQTT50, qos: packet.QoS0},
		{version: packet.MQTT31, qos: packet.QoS1},
		{version: packet.MQTT311, qos: packet.QoS1},
		{version: packet.MQTT50, qos: packet.QoS1},
	}

	for _, tc := range testCases {
		name := fmt.Sprintf("%v-%v", tc.version.String(), tc.qos)
		t.Run(name, func(t *testing.T) {
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			idGen := &messageIDGenMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewPublishHandler(st, subMgr, idGen, &log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc.version}

			pubPkt := packet.NewPublish(1, tc.version, "data", tc.qos,
				0, 0, []byte("data"), nil)
			msg := &Message{ID: 10, PacketID: pubPkt.PacketID, Packet: &pubPkt}

			st.On("ReadSession", id).Return(s, nil)
			idGen.On("NextID").Return(uint64(msg.ID))
			subMgr.On("Publish", msg).Return(errors.New("failed"))

			replies, err := h.HandlePacket(id, &pubPkt)
			assert.NotNil(t, err)
			assert.Empty(t, replies)
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
			idGen.AssertExpectations(t)
		})
	}
}

func TestPublishHandlerHandlePacketSaveSessionError(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			idGen := &messageIDGenMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewPublishHandler(st, subMgr, idGen, &log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc,
				UnAckMessages: make(map[packet.ID]*Message)}

			pubPkt := packet.NewPublish(1, tc, "data", packet.QoS2,
				0, 0, []byte("data"), nil)
			msg := &Message{ID: 10, PacketID: pubPkt.PacketID, Packet: &pubPkt}

			st.On("ReadSession", id).Return(s, nil)
			st.On("SaveSession", s).Return(errors.New("failed"))
			idGen.On("NextID").Return(uint64(msg.ID))
			subMgr.On("Publish", msg).Return(nil)

			replies, err := h.HandlePacket(id, &pubPkt)
			assert.NotNil(t, err)
			assert.Empty(t, replies)
			st.AssertExpectations(t)
			idGen.AssertExpectations(t)
		})
	}
}
