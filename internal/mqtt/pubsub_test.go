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
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/gsalomao/maxmq/internal/mqtt/handler"
	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestPubSubManagerStartStop(t *testing.T) {
	pd := &packetDelivererMock{}
	sm := &sessionStoreMock{}
	log := newLogger()
	mt := newMetrics(true, log)
	ps := newPubSubManager(pd, sm, mt, log)

	ps.start()
	<-time.After(10 * time.Millisecond)
	ps.stop()
}

func TestPubSubManagerRun(t *testing.T) {
	pd := &packetDelivererMock{}
	sm := &sessionStoreMock{}
	log := newLogger()
	mt := newMetrics(true, log)
	ps := newPubSubManager(pd, sm, mt, log)

	go ps.run()
	<-ps.action
	ps.action <- pubSubActionPublishMessage
	ps.action <- pubSubActionStop
	<-ps.action
}

func TestPubSubManagerSubscribe(t *testing.T) {
	testCases := []packet.Topic{
		{Name: "sensor/temp0", QoS: packet.QoS0, RetainHandling: 0, RetainAsPublished: false, NoLocal: false},
		{Name: "sensor/temp1", QoS: packet.QoS1, RetainHandling: 1, RetainAsPublished: false, NoLocal: true},
		{Name: "sensor/temp2", QoS: packet.QoS2, RetainHandling: 0, RetainAsPublished: true, NoLocal: false},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			pd := &packetDelivererMock{}
			sm := &sessionStoreMock{}
			log := newLogger()
			mt := newMetrics(true, log)
			ps := newPubSubManager(pd, sm, mt, log)

			sub := &handler.Subscription{
				ClientID:          "a",
				ID:                rand.Int(),
				TopicFilter:       tc.Name,
				QoS:               tc.QoS,
				RetainHandling:    tc.RetainHandling,
				RetainAsPublished: tc.RetainAsPublished,
				NoLocal:           tc.NoLocal,
			}

			err := ps.Subscribe(sub)
			assert.Nil(t, err)
		})
	}
}

func TestPubSubManagerSubscribeError(t *testing.T) {
	testCases := []packet.Topic{
		{Name: "tempQoS0#", QoS: packet.QoS0},
		{Name: "data/tempQoS1#", QoS: packet.QoS1},
		{Name: "data/temp/QoS2#", QoS: packet.QoS2},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			pd := &packetDelivererMock{}
			sm := &sessionStoreMock{}
			log := newLogger()
			mt := newMetrics(true, log)
			ps := newPubSubManager(pd, sm, mt, log)

			sub := &handler.Subscription{ClientID: "a", TopicFilter: tc.Name, QoS: tc.QoS}
			err := ps.Subscribe(sub)
			assert.NotNil(t, err)
		})
	}
}

func TestPubSubManagerUnsubscribeTopic(t *testing.T) {
	testCases := []packet.Topic{
		{Name: "sensor"},
		{Name: "sensor/temp"},
		{Name: "sensor/temp/2"},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			pd := &packetDelivererMock{}
			sm := &sessionStoreMock{}
			log := newLogger()
			mt := newMetrics(true, log)
			ps := newPubSubManager(pd, sm, mt, log)

			sub := &handler.Subscription{ClientID: "a", TopicFilter: tc.Name}
			err := ps.Subscribe(sub)
			require.Nil(t, err)

			err = ps.Unsubscribe(sub.ClientID, sub.TopicFilter)
			assert.Nil(t, err)
		})
	}
}

func TestPubSubManagerUnsubscribeSubscriptionNotFound(t *testing.T) {
	testCases := []packet.Topic{
		{Name: "sensor"},
		{Name: "sensor/temp"},
		{Name: "sensor/temp/2"},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			pd := &packetDelivererMock{}
			sm := &sessionStoreMock{}
			log := newLogger()
			mt := newMetrics(true, log)
			ps := newPubSubManager(pd, sm, mt, log)
			s := handler.Session{ClientID: "a"}

			err := ps.Unsubscribe(s.ClientID, tc.Name)
			assert.Equal(t, handler.ErrSubscriptionNotFound, err)
		})
	}
}

func TestPubSubManagerPublishMessage(t *testing.T) {
	pd := &packetDelivererMock{}
	sm := &sessionStoreMock{}
	log := newLogger()
	mt := newMetrics(true, log)
	ps := newPubSubManager(pd, sm, mt, log)

	pubPkt := packet.NewPublish(1, packet.MQTT311, "t", packet.QoS1, 0, 0, nil, nil)
	msg := &handler.Message{PacketID: pubPkt.PacketID, Packet: &pubPkt}

	err := ps.Publish(msg)
	require.Nil(t, err)

	act := <-ps.action
	assert.Equal(t, pubSubActionPublishMessage, act)

	assert.Equal(t, 1, ps.queue.Len())
	assert.Same(t, msg, ps.queue.Dequeue())
}

func TestPubSubManagerHandleQueuedMessagesNoSubscription(t *testing.T) {
	pd := &packetDelivererMock{}
	sm := &sessionStoreMock{}
	log := newLogger()
	mt := newMetrics(true, log)
	ps := newPubSubManager(pd, sm, mt, log)

	pubPkt := packet.NewPublish(1, packet.MQTT311, "t", packet.QoS0, 0, 0, nil, nil)
	msg := handler.Message{PacketID: pubPkt.PacketID, Packet: &pubPkt}
	ps.queue.Enqueue(&msg)

	ps.handleQueuedMessages()
	assert.Zero(t, ps.queue.Len())
}

func TestPubSubManagerHandleQueuedMessages(t *testing.T) {
	testCases := []struct {
		id    packet.ID
		topic string
		qos   packet.QoS
	}{
		{1, "a/0", packet.QoS0},
		{2, "a/0", packet.QoS1},
		{3, "a/0", packet.QoS2},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("ID-%v", tc.id), func(t *testing.T) {
			pd := &packetDelivererMock{}
			sm := &sessionStoreMock{}
			log := newLogger()
			mt := newMetrics(true, log)
			ps := newPubSubManager(pd, sm, mt, log)
			id := packet.ClientID("client")

			sub := handler.Subscription{ClientID: id, TopicFilter: tc.topic, QoS: tc.qos}
			_, err := ps.tree.Insert(sub)
			require.Nil(t, err)

			pubPkt := packet.NewPublish(tc.id, packet.MQTT311, tc.topic, tc.qos, 0, 0, nil, nil)
			msg := &handler.Message{PacketID: pubPkt.PacketID, Packet: &pubPkt}
			ps.queue.Enqueue(msg)

			s := &handler.Session{
				ClientID:     id,
				Version:      packet.MQTT311,
				Connected:    true,
				LastPacketID: uint32(tc.id + 9),
			}
			sm.On("ReadSession", id).Return(s, nil)
			pd.On("deliverPacket", id, mock.Anything).Return(nil)

			if tc.qos > packet.QoS0 {
				sm.On("SaveSession", s).Return(nil)
			}

			ps.handleQueuedMessages()
			assert.Zero(t, ps.queue.Len())
			if tc.qos > packet.QoS0 {
				require.Equal(t, 1, s.InflightMessages.Len())
				ifMsg := s.InflightMessages.Front().Value.(*handler.Message)
				assert.Equal(t, msg.ID, ifMsg.ID)
				assert.Equal(t, 1, ifMsg.Tries)
				assert.NotZero(t, ifMsg.LastSent)
			}
			sm.AssertExpectations(t)
			pd.AssertExpectations(t)
		})
	}
}

func TestPubSubManagerHandleQueuedMessagesMultipleSubscribers(t *testing.T) {
	testCases := []struct {
		topic string
		subs  []string
	}{
		{"a/0", []string{"a/0"}},
		{"b/1", []string{"b/1", "b/+"}},
		{"c/2", []string{"c/2", "c/#"}},
	}

	for _, tc := range testCases {
		t.Run(tc.topic, func(t *testing.T) {
			pd := &packetDelivererMock{}
			sm := &sessionStoreMock{}
			log := newLogger()
			mt := newMetrics(true, log)
			ps := newPubSubManager(pd, sm, mt, log)
			id := packet.ClientID("client")

			for _, topic := range tc.subs {
				sub := handler.Subscription{ClientID: id, TopicFilter: topic}
				_, err := ps.tree.Insert(sub)
				require.Nil(t, err)
			}

			pubPkt := packet.NewPublish(1, packet.MQTT311, tc.topic, packet.QoS0, 0, 0, nil, nil)
			msg := &handler.Message{PacketID: pubPkt.PacketID, Packet: &pubPkt}
			ps.queue.Enqueue(msg)

			s := &handler.Session{ClientID: id, Version: packet.MQTT311, Connected: true}
			sm.On("ReadSession", id).Return(s, nil)
			pd.On("deliverPacket", id, mock.Anything).Return(nil).Times(len(tc.subs))

			ps.handleQueuedMessages()
			assert.Zero(t, ps.queue.Len())
			sm.AssertExpectations(t)
			pd.AssertExpectations(t)
		})
	}
}

func TestPubSubManagerHandleQueuedMessagesNewPacketID(t *testing.T) {
	testCases := []packet.QoS{
		packet.QoS1,
		packet.QoS2,
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc), func(t *testing.T) {
			pd := &packetDelivererMock{}
			sm := &sessionStoreMock{}
			log := newLogger()
			mt := newMetrics(true, log)
			ps := newPubSubManager(pd, sm, mt, log)

			id := packet.ClientID("client")
			sub := handler.Subscription{ClientID: id, TopicFilter: "t", QoS: tc}
			_, err := ps.tree.Insert(sub)
			require.Nil(t, err)

			pubPkt := packet.NewPublish(10, packet.MQTT311, "t", tc, 0, 0, nil, nil)
			msg := &handler.Message{PacketID: pubPkt.PacketID, Packet: &pubPkt}
			ps.queue.Enqueue(msg)

			lastPktID := uint32(1)
			s := &handler.Session{
				ClientID:     id,
				Version:      packet.MQTT311,
				Connected:    true,
				LastPacketID: lastPktID,
			}
			sm.On("ReadSession", id).Return(s, nil)
			sm.On("SaveSession", s).Return(nil)
			pd.On("deliverPacket", id, mock.Anything).Return(nil)

			ps.handleQueuedMessages()
			assert.Zero(t, ps.queue.Len())

			pkt := pd.Calls[0].Arguments[1].(*packet.Publish)
			assert.Equal(t, packet.ID(lastPktID+1), pkt.PacketID)
			assert.Equal(t, packet.ID(s.LastPacketID), pkt.PacketID)
			assert.Equal(t, pubPkt.PacketID, msg.PacketID)
			assert.Equal(t, &pubPkt, msg.Packet)
			sm.AssertExpectations(t)
			pd.AssertExpectations(t)
		})
	}
}

func TestPubSubManagerHandleQueuedMessagesDifferentVersion(t *testing.T) {
	pd := &packetDelivererMock{}
	sm := &sessionStoreMock{}
	log := newLogger()
	mt := newMetrics(true, log)
	ps := newPubSubManager(pd, sm, mt, log)

	id := packet.ClientID("client")
	sub := handler.Subscription{ClientID: id, TopicFilter: "t", QoS: packet.QoS0}
	_, err := ps.tree.Insert(sub)
	require.Nil(t, err)

	pubPkt := packet.NewPublish(1, packet.MQTT311, "t", packet.QoS0, 0, 0, nil, nil)
	msg := &handler.Message{PacketID: pubPkt.PacketID, Packet: &pubPkt}
	ps.queue.Enqueue(msg)

	s := &handler.Session{ClientID: id, Version: packet.MQTT50, Connected: true}
	sm.On("ReadSession", id).Return(s, nil)
	pd.On("deliverPacket", id, mock.Anything).Return(nil)

	ps.handleQueuedMessages()
	assert.Zero(t, ps.queue.Len())

	pkt := pd.Calls[0].Arguments[1].(*packet.Publish)
	assert.Equal(t, s.Version, pkt.Version)
	sm.AssertExpectations(t)
	pd.AssertExpectations(t)
}

func TestPubSubManagerHandleQueuedMessagesDisconnected(t *testing.T) {
	testCases := []packet.QoS{
		packet.QoS0,
		packet.QoS1,
		packet.QoS2,
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc), func(t *testing.T) {
			pd := &packetDelivererMock{}
			sm := &sessionStoreMock{}
			log := newLogger()
			mt := newMetrics(true, log)
			ps := newPubSubManager(pd, sm, mt, log)

			id := packet.ClientID("client")
			sub := handler.Subscription{ClientID: id, TopicFilter: "t", QoS: tc}
			_, err := ps.tree.Insert(sub)
			require.Nil(t, err)

			pubPkt := packet.NewPublish(1, packet.MQTT311, "t", tc, 0, 0, nil, nil)
			msg := &handler.Message{PacketID: pubPkt.PacketID, Packet: &pubPkt}
			ps.queue.Enqueue(msg)

			s := &handler.Session{ClientID: id, Version: packet.MQTT50}
			sm.On("ReadSession", id).Return(s, nil)

			if tc > packet.QoS0 {
				sm.On("SaveSession", s).Return(nil)
			}

			ps.handleQueuedMessages()
			assert.Zero(t, ps.queue.Len())
			sm.AssertExpectations(t)
			pd.AssertExpectations(t)
		})
	}
}

func TestPubSubManagerHandleQueuedMessagesDeliverPacketError(t *testing.T) {
	testCases := []packet.QoS{
		packet.QoS0,
		packet.QoS1,
		packet.QoS2,
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc), func(t *testing.T) {
			pd := &packetDelivererMock{}
			sm := &sessionStoreMock{}
			log := newLogger()
			mt := newMetrics(true, log)
			ps := newPubSubManager(pd, sm, mt, log)

			id := packet.ClientID("client")
			sub := handler.Subscription{ClientID: id, TopicFilter: "t", QoS: tc}
			_, err := ps.tree.Insert(sub)
			require.Nil(t, err)

			pubPkt := packet.NewPublish(1, packet.MQTT311, "t", tc, 0, 0, nil, nil)
			msg := &handler.Message{PacketID: pubPkt.PacketID, Packet: &pubPkt}
			ps.queue.Enqueue(msg)

			s := &handler.Session{ClientID: id, Version: packet.MQTT311, Connected: true}
			sm.On("ReadSession", id).Return(s, nil)
			pd.On("deliverPacket", id, &pubPkt).Return(errors.New("failed"))

			if tc > packet.QoS0 {
				sm.On("SaveSession", s).Return(nil)
			}

			ps.handleQueuedMessages()
			assert.Zero(t, ps.queue.Len())
			sm.AssertExpectations(t)
			pd.AssertExpectations(t)
		})
	}
}

func TestPubSubManagerHandleQueuedMessagesReadSessionError(t *testing.T) {
	pd := &packetDelivererMock{}
	sm := &sessionStoreMock{}
	log := newLogger()
	mt := newMetrics(true, log)
	ps := newPubSubManager(pd, sm, mt, log)

	id := packet.ClientID("client")
	sub := handler.Subscription{ClientID: id, TopicFilter: "t", QoS: packet.QoS0}
	_, err := ps.tree.Insert(sub)
	require.Nil(t, err)

	pubPkt := packet.NewPublish(1, packet.MQTT311, "t", packet.QoS0, 0, 0, nil, nil)
	msg := &handler.Message{PacketID: pubPkt.PacketID, Packet: &pubPkt}
	ps.queue.Enqueue(msg)

	sm.On("ReadSession", id).Return(nil, handler.ErrSessionNotFound)

	ps.handleQueuedMessages()
	assert.Zero(t, ps.queue.Len())
	sm.AssertExpectations(t)
	pd.AssertExpectations(t)
}

func TestPubSubManagerHandleQueuedMessagesSaveSessionError(t *testing.T) {
	pd := &packetDelivererMock{}
	sm := &sessionStoreMock{}
	log := newLogger()
	mt := newMetrics(true, log)
	ps := newPubSubManager(pd, sm, mt, log)

	id := packet.ClientID("client")
	sub := handler.Subscription{ClientID: id, TopicFilter: "t", QoS: packet.QoS1}
	_, err := ps.tree.Insert(sub)
	require.Nil(t, err)

	pubPkt := packet.NewPublish(1, packet.MQTT311, "t", packet.QoS1, 0, 0, nil, nil)
	msg := &handler.Message{PacketID: pubPkt.PacketID, Packet: &pubPkt}
	ps.queue.Enqueue(msg)

	s := &handler.Session{ClientID: id, Version: packet.MQTT311}
	sm.On("ReadSession", id).Return(s, nil)
	sm.On("SaveSession", s).Return(errors.New("failed"))

	ps.handleQueuedMessages()
	assert.Zero(t, ps.queue.Len())
	sm.AssertExpectations(t)
	pd.AssertExpectations(t)
}
