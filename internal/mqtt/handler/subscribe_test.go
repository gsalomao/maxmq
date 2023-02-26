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

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscribeHandlerHandlePacket(t *testing.T) {
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
		{version: packet.MQTT31, qos: packet.QoS2},
		{version: packet.MQTT311, qos: packet.QoS2},
		{version: packet.MQTT50, qos: packet.QoS2},
	}

	for _, tc := range testCases {
		name := fmt.Sprintf("%v-%v", tc.version.String(), tc.qos)
		t.Run(name, func(t *testing.T) {
			conf := newDefaultConfiguration()
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewSubscribeHandler(&conf, st, subMgr, &log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc.version,
				Subscriptions: make(map[string]*Subscription)}
			sub := &Subscription{ID: 0, ClientID: id, TopicFilter: "data",
				QoS: tc.qos}

			st.On("ReadSession", id).Return(s, nil)
			st.On("SaveSession", s).Return(nil)
			subMgr.On("Subscribe", sub).Return(nil)

			subPkt := &packet.Subscribe{PacketID: 1, Version: tc.version,
				Topics: []packet.Topic{{Name: sub.TopicFilter, QoS: tc.qos}}}

			replies, err := h.HandlePacket(id, subPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.SUBACK, reply.Type())

			subAckPkt := reply.(*packet.SubAck)
			assert.Equal(t, subPkt.PacketID, subAckPkt.PacketID)
			assert.Equal(t, tc.version, subAckPkt.Version)

			require.Len(t, subAckPkt.ReasonCodes, 1)
			assert.Equal(t, packet.ReasonCode(tc.qos), subAckPkt.ReasonCodes[0])

			require.Len(t, s.Subscriptions, 1)
			assert.Equal(t, sub, s.Subscriptions["data"])
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
		})
	}
}

func TestSubscribeHandlerHandlePacketMultipleTopics(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newDefaultConfiguration()
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewSubscribeHandler(&conf, st, subMgr, &log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc,
				Subscriptions: make(map[string]*Subscription)}

			subs := []*Subscription{
				{ClientID: id, TopicFilter: "data/temp/0", QoS: packet.QoS0},
				{ClientID: id, TopicFilter: "data/temp/1", QoS: packet.QoS1},
				{ClientID: id, TopicFilter: "data/temp/2", QoS: packet.QoS2},
				{ClientID: id, TopicFilter: "data/temp/#", QoS: packet.QoS0},
			}

			st.On("ReadSession", id).Return(s, nil)
			st.On("SaveSession", s).Return(nil)
			subMgr.On("Subscribe", subs[0]).
				Return(nil).Once()
			subMgr.On("Subscribe", subs[1]).
				Return(nil).Once()
			subMgr.On("Subscribe", subs[2]).
				Return(nil).Once()
			subMgr.On("Subscribe", subs[3]).
				Return(errors.New("invalid")).Once()

			subPkt := &packet.Subscribe{PacketID: 5, Version: tc,
				Topics: []packet.Topic{
					{Name: subs[0].TopicFilter, QoS: subs[0].QoS},
					{Name: subs[1].TopicFilter, QoS: subs[1].QoS},
					{Name: subs[2].TopicFilter, QoS: subs[2].QoS},
					{Name: subs[3].TopicFilter, QoS: subs[3].QoS},
				},
			}

			replies, err := h.HandlePacket(id, subPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.SUBACK, reply.Type())

			subAckPkt := reply.(*packet.SubAck)
			assert.Equal(t, subPkt.PacketID, subAckPkt.PacketID)
			assert.Equal(t, subPkt.Version, subAckPkt.Version)

			require.Len(t, subAckPkt.ReasonCodes, 4)
			assert.Equal(t, packet.ReasonCode(packet.QoS0),
				subAckPkt.ReasonCodes[0])
			assert.Equal(t, packet.ReasonCode(packet.QoS1),
				subAckPkt.ReasonCodes[1])
			assert.Equal(t, packet.ReasonCode(packet.QoS2),
				subAckPkt.ReasonCodes[2])
			assert.Equal(t, packet.ReasonCodeV3Failure,
				subAckPkt.ReasonCodes[3])

			require.Len(t, s.Subscriptions, 3)
			assert.Equal(t, subs[0], s.Subscriptions[subPkt.Topics[0].Name])
			assert.Equal(t, subs[1], s.Subscriptions[subPkt.Topics[1].Name])
			assert.Equal(t, subs[2], s.Subscriptions[subPkt.Topics[2].Name])
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
		})
	}
}

func TestSubscribeHandlerHandlePacketError(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newDefaultConfiguration()
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewSubscribeHandler(&conf, st, subMgr, &log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc,
				Subscriptions: make(map[string]*Subscription)}
			sub := &Subscription{ID: 0, ClientID: id, TopicFilter: "data",
				QoS: packet.QoS1}

			st.On("ReadSession", id).Return(s, nil)
			subMgr.On("Subscribe", sub).Return(errors.New("failed"))

			subPkt := &packet.Subscribe{PacketID: 1, Version: tc,
				Topics: []packet.Topic{{Name: "data", QoS: packet.QoS1}}}

			replies, err := h.HandlePacket(id, subPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.SUBACK, reply.Type())

			subAckPkt := reply.(*packet.SubAck)
			assert.Equal(t, subPkt.PacketID, subAckPkt.PacketID)
			assert.Equal(t, tc, subAckPkt.Version)

			require.Len(t, subAckPkt.ReasonCodes, 1)
			assert.Equal(t, packet.ReasonCodeV3Failure,
				subAckPkt.ReasonCodes[0])

			assert.Empty(t, s.Subscriptions)
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
		})
	}
}

func TestSubscribeHandlerHandlePacketReadSessionError(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newDefaultConfiguration()
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewSubscribeHandler(&conf, st, subMgr, &log)

			id := packet.ClientID("a")
			st.On("ReadSession", id).
				Return(nil, ErrSessionNotFound)

			subPkt := &packet.Subscribe{PacketID: 1, Version: tc,
				Topics: []packet.Topic{{Name: "data", QoS: packet.QoS1}}}

			replies, err := h.HandlePacket(id, subPkt)
			assert.NotNil(t, err)
			assert.Empty(t, replies)
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
		})
	}
}

func TestSubscribeHandlerHandlePacketSaveSessionError(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newDefaultConfiguration()
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewSubscribeHandler(&conf, st, subMgr, &log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc,
				Subscriptions: make(map[string]*Subscription)}

			subs := []*Subscription{
				{ClientID: id, TopicFilter: "data/temp/0", QoS: packet.QoS0},
				{ClientID: id, TopicFilter: "data/temp/#", QoS: packet.QoS1},
				{ClientID: id, TopicFilter: "data/temp/2", QoS: packet.QoS2},
			}

			st.On("ReadSession", id).Return(s, nil)
			st.On("SaveSession", s).Return(errors.New("failed"))
			subMgr.On("Subscribe", subs[0]).
				Return(nil).Once()
			subMgr.On("Subscribe", subs[1]).
				Return(errors.New("invalid")).Once()
			subMgr.On("Subscribe", subs[2]).
				Return(nil).Once()
			subMgr.On("Unsubscribe", id, subs[0].TopicFilter).
				Once().Return(nil)
			subMgr.On("Unsubscribe", id, subs[2].TopicFilter).
				Once().Return(errors.New("failed"))

			subPkt := &packet.Subscribe{PacketID: 5, Version: tc,
				Topics: []packet.Topic{
					{Name: subs[0].TopicFilter, QoS: subs[0].QoS},
					{Name: subs[1].TopicFilter, QoS: subs[1].QoS},
					{Name: subs[2].TopicFilter, QoS: subs[2].QoS},
				},
			}

			replies, err := h.HandlePacket(id, subPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.SUBACK, reply.Type())

			subAckPkt := reply.(*packet.SubAck)
			assert.Equal(t, subPkt.PacketID, subAckPkt.PacketID)
			assert.Equal(t, subPkt.Version, subAckPkt.Version)

			require.Len(t, subAckPkt.ReasonCodes, 3)
			assert.Equal(t, packet.ReasonCodeV3Failure,
				subAckPkt.ReasonCodes[0])
			assert.Equal(t, packet.ReasonCodeV3Failure,
				subAckPkt.ReasonCodes[1])
			assert.Equal(t, packet.ReasonCodeV3Failure,
				subAckPkt.ReasonCodes[2])

			assert.Empty(t, s.Subscriptions)
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
		})
	}
}

func TestSubscribeHandlerHandlePacketV5WithSubID(t *testing.T) {
	conf := newDefaultConfiguration()
	conf.SubscriptionIDAvailable = true

	st := &sessionStoreMock{}
	subMgr := &subscriptionMgrMock{}
	log := logger.New(&bytes.Buffer{}, nil)
	h := NewSubscribeHandler(&conf, st, subMgr, &log)

	id := packet.ClientID("a")
	s := &Session{ClientID: id, Version: packet.MQTT50,
		Subscriptions: make(map[string]*Subscription)}

	sub := &Subscription{ID: 5, ClientID: id, TopicFilter: "data",
		QoS: packet.QoS0}

	st.On("ReadSession", id).Return(s, nil)
	st.On("SaveSession", s).Return(nil)
	subMgr.On("Subscribe", sub).Return(nil)

	props := &packet.Properties{}
	props.SubscriptionIdentifier = new(int)
	*props.SubscriptionIdentifier = sub.ID

	subPkt := &packet.Subscribe{PacketID: 2, Version: packet.MQTT50,
		Properties: props, Topics: []packet.Topic{{Name: "data"}}}

	replies, err := h.HandlePacket(id, subPkt)
	assert.Nil(t, err)
	require.Len(t, replies, 1)

	reply := replies[0]
	require.Equal(t, packet.SUBACK, reply.Type())
	st.AssertExpectations(t)
	subMgr.AssertExpectations(t)
}

func TestSubscribeHandlerHandlePacketV5WithSubIDError(t *testing.T) {
	conf := newDefaultConfiguration()
	conf.SubscriptionIDAvailable = false

	st := &sessionStoreMock{}
	subMgr := &subscriptionMgrMock{}
	log := logger.New(&bytes.Buffer{}, nil)
	h := NewSubscribeHandler(&conf, st, subMgr, &log)

	id := packet.ClientID("a")
	s := &Session{ClientID: id, Version: packet.MQTT50,
		Subscriptions: make(map[string]*Subscription)}

	st.On("ReadSession", id).Return(s, nil)

	props := &packet.Properties{}
	props.SubscriptionIdentifier = new(int)
	*props.SubscriptionIdentifier = 5

	subPkt := &packet.Subscribe{PacketID: 2, Version: packet.MQTT50,
		Properties: props, Topics: []packet.Topic{{Name: "topic"}}}

	replies, err := h.HandlePacket(id, subPkt)
	require.NotNil(t, err)
	require.Len(t, replies, 1)

	reply := replies[0]
	require.Equal(t, packet.DISCONNECT, reply.Type())

	discPkt := reply.(*packet.Disconnect)
	assert.Equal(t, packet.MQTT50, discPkt.Version)
	assert.Equal(t, packet.ReasonCodeV5SubscriptionIDNotSupported,
		discPkt.ReasonCode)
	st.AssertExpectations(t)
	subMgr.AssertExpectations(t)
}
