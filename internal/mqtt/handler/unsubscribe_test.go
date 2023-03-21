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

func TestUnsubscribeHandlePacket(t *testing.T) {
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
			h := NewUnsubscribe(st, subMgr, log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc, Subscriptions: make(map[string]*Subscription)}

			sub := &Subscription{ClientID: id, TopicFilter: "data"}
			s.Subscriptions["data"] = sub
			unsubPkt := &packet.Unsubscribe{PacketID: 1, Version: tc, Topics: []string{"data"}}

			st.On("ReadSession", id).Return(s, nil)
			st.On("SaveSession", s).Return(nil)
			subMgr.On("Unsubscribe", id, sub.TopicFilter).Return(nil)

			replies, err := h.HandlePacket(id, unsubPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.UNSUBACK, reply.Type())

			unsubAckPkt := reply.(*packet.UnsubAck)
			assert.Equal(t, unsubPkt.PacketID, unsubAckPkt.PacketID)
			assert.Equal(t, unsubPkt.Version, unsubAckPkt.Version)

			require.Len(t, unsubAckPkt.ReasonCodes, 1)
			assert.Equal(t, packet.ReasonCodeV5Success, unsubAckPkt.ReasonCodes[0])

			assert.Empty(t, s.Subscriptions)
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
		})
	}
}

func TestUnsubscribeHandlePacketMultipleTopics(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			log := logger.New(&bytes.Buffer{}, nil, logger.LogFormatJson)
			h := NewUnsubscribe(st, subMgr, log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc, Subscriptions: make(map[string]*Subscription)}

			subs := []*Subscription{
				{ClientID: id, TopicFilter: "data/0", QoS: packet.QoS0},
				{ClientID: id, TopicFilter: "data/#", QoS: packet.QoS1},
				{ClientID: id, TopicFilter: "data/2", QoS: packet.QoS2},
			}

			s.Subscriptions[subs[0].TopicFilter] = subs[0]
			s.Subscriptions[subs[1].TopicFilter] = subs[1]
			s.Subscriptions[subs[2].TopicFilter] = subs[2]

			topicNames := []string{"data/0", "data/#"}
			unsubPkt := &packet.Unsubscribe{PacketID: 1, Version: tc, Topics: topicNames}

			st.On("ReadSession", id).Return(s, nil)
			st.On("SaveSession", s).Return(nil)
			subMgr.On("Unsubscribe", id, subs[0].TopicFilter).Once().Return(nil)
			subMgr.On("Unsubscribe", id, subs[1].TopicFilter).Once().Return(nil)

			replies, err := h.HandlePacket(id, unsubPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.UNSUBACK, reply.Type())

			unsubAckPkt := reply.(*packet.UnsubAck)
			assert.Equal(t, unsubPkt.PacketID, unsubAckPkt.PacketID)
			assert.Equal(t, unsubPkt.Version, unsubAckPkt.Version)

			codes := []packet.ReasonCode{packet.ReasonCodeV5Success, packet.ReasonCodeV5Success}
			assert.Equal(t, codes, unsubAckPkt.ReasonCodes)

			require.NotNil(t, s)
			assert.Len(t, s.Subscriptions, 1)
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
		})
	}
}

func TestUnsubscribeHandlePacketUnsubscribeError(t *testing.T) {
	testCases := []struct {
		err  error
		code packet.ReasonCode
	}{
		{ErrSubscriptionNotFound, packet.ReasonCodeV5NoSubscriptionExisted},
		{errors.New("failed"), packet.ReasonCodeV5UnspecifiedError},
	}

	for _, tc := range testCases {
		t.Run(tc.err.Error(), func(t *testing.T) {
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			log := logger.New(&bytes.Buffer{}, nil, logger.LogFormatJson)
			h := NewUnsubscribe(st, subMgr, log)

			id := packet.ClientID("a")
			s := &Session{
				ClientID:      id,
				Version:       packet.MQTT311,
				Subscriptions: make(map[string]*Subscription),
			}

			unsubPkt := &packet.Unsubscribe{
				PacketID: 1,
				Version:  packet.MQTT311,
				Topics:   []string{"data"},
			}

			st.On("ReadSession", id).Return(s, nil)
			subMgr.On("Unsubscribe", id, "data").Return(tc.err)

			replies, err := h.HandlePacket(id, unsubPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.UNSUBACK, reply.Type())

			unsubAckPkt := reply.(*packet.UnsubAck)
			assert.Equal(t, unsubPkt.PacketID, unsubAckPkt.PacketID)
			assert.Equal(t, unsubPkt.Version, unsubAckPkt.Version)

			require.Len(t, unsubAckPkt.ReasonCodes, 1)
			assert.Equal(t, tc.code, unsubAckPkt.ReasonCodes[0])
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
		})
	}
}

func TestUnsubscribeHandlePacketReadSessionError(t *testing.T) {
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
			h := NewUnsubscribe(st, subMgr, log)

			id := packet.ClientID("a")
			unsubPkt := &packet.Unsubscribe{PacketID: 1, Version: tc, Topics: []string{"data"}}
			st.On("ReadSession", id).Return(nil, errors.New("failed"))

			replies, err := h.HandlePacket(id, unsubPkt)
			assert.NotNil(t, err)
			assert.Empty(t, replies)
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
		})
	}
}

func TestUnsubscribeHandlePacketSaveSessionError(t *testing.T) {
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
			h := NewUnsubscribe(st, subMgr, log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc, Subscriptions: make(map[string]*Subscription)}

			subs := []*Subscription{
				{ClientID: id, TopicFilter: "data/0", QoS: packet.QoS0},
				{ClientID: id, TopicFilter: "data/1", QoS: packet.QoS1},
			}

			s.Subscriptions[subs[0].TopicFilter] = subs[0]
			s.Subscriptions[subs[1].TopicFilter] = subs[1]

			topicNames := []string{"data/0", "data/1"}
			unsubPkt := &packet.Unsubscribe{PacketID: 1, Version: tc, Topics: topicNames}

			st.On("ReadSession", id).Return(s, nil)
			st.On("SaveSession", s).Return(errors.New("failed"))
			subMgr.On("Unsubscribe", id, subs[0].TopicFilter).Return(nil)
			subMgr.On("Unsubscribe", id, subs[1].TopicFilter).Return(nil)

			replies, err := h.HandlePacket(id, unsubPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.UNSUBACK, reply.Type())

			unsubAckPkt := reply.(*packet.UnsubAck)
			assert.Equal(t, unsubPkt.PacketID, unsubAckPkt.PacketID)
			assert.Equal(t, unsubPkt.Version, unsubAckPkt.Version)

			codes := []packet.ReasonCode{packet.ReasonCodeV5UnspecifiedError, packet.ReasonCodeV5UnspecifiedError}
			assert.Equal(t, codes, unsubAckPkt.ReasonCodes)

			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
		})
	}
}
