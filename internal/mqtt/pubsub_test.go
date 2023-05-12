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

	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type packetDelivererMock struct {
	mock.Mock
	stream chan *packet.Publish
}

func newPacketDeliverMock() *packetDelivererMock {
	return &packetDelivererMock{
		stream: make(chan *packet.Publish, 1),
	}
}

func (p *packetDelivererMock) deliverPacket(id packet.ClientID, pkt *packet.Publish) error {
	args := p.Called(id, pkt)
	p.stream <- pkt
	if len(args) > 0 {
		return args.Error(0)
	}
	return nil
}

func TestPubSubSubscribe(t *testing.T) {
	testCases := []packet.Topic{
		{Name: "sensor/temp0", QoS: packet.QoS0, RetainHandling: 0, RetainAsPublished: false, NoLocal: false},
		{Name: "sensor/temp1", QoS: packet.QoS1, RetainHandling: 1, RetainAsPublished: false, NoLocal: true},
		{Name: "sensor/temp2", QoS: packet.QoS2, RetainHandling: 0, RetainAsPublished: true, NoLocal: false},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			gen := &idGeneratorMock{}
			pd := newPacketDeliverMock()
			log := newLogger()
			ss := newSessionStore(gen, log)
			mt := newMetrics(true, log)
			ps := newPubSub(pd, ss, mt, log)

			sub := &subscription{
				clientID:          "client-a",
				id:                subscriptionID(rand.Int()),
				topicFilter:       tc.Name,
				qos:               tc.QoS,
				retainHandling:    tc.RetainHandling,
				retainAsPublished: tc.RetainAsPublished,
				noLocal:           tc.NoLocal,
			}

			err := ps.subscribe(sub)
			assert.Nil(t, err)
		})
	}
}

func TestPubSubSubscribeError(t *testing.T) {
	testCases := []packet.Topic{
		{Name: "tempQoS0#", QoS: packet.QoS0},
		{Name: "data/tempQoS1#", QoS: packet.QoS1},
		{Name: "data/temp/QoS2#", QoS: packet.QoS2},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			gen := &idGeneratorMock{}
			pd := newPacketDeliverMock()
			log := newLogger()
			ss := newSessionStore(gen, log)
			mt := newMetrics(true, log)
			ps := newPubSub(pd, ss, mt, log)

			sub := &subscription{clientID: "client-a", topicFilter: tc.Name, qos: tc.QoS}
			err := ps.subscribe(sub)
			assert.NotNil(t, err)
		})
	}
}

func TestPubSubUnsubscribeTopic(t *testing.T) {
	testCases := []packet.Topic{
		{Name: "sensor"},
		{Name: "sensor/temp"},
		{Name: "sensor/temp/2"},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			gen := &idGeneratorMock{}
			pd := newPacketDeliverMock()
			log := newLogger()
			ss := newSessionStore(gen, log)
			mt := newMetrics(true, log)
			ps := newPubSub(pd, ss, mt, log)

			sub := &subscription{clientID: "client-a", topicFilter: tc.Name}
			err := ps.subscribe(sub)
			require.Nil(t, err)

			err = ps.unsubscribe(sub.clientID, sub.topicFilter)
			assert.Nil(t, err)
		})
	}
}

func TestPubSubUnsubscribeSubscriptionNotFound(t *testing.T) {
	testCases := []packet.Topic{
		{Name: "sensor"},
		{Name: "sensor/temp"},
		{Name: "sensor/temp/2"},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			gen := &idGeneratorMock{}
			pd := newPacketDeliverMock()
			log := newLogger()
			ss := newSessionStore(gen, log)
			mt := newMetrics(true, log)
			ps := newPubSub(pd, ss, mt, log)
			s := session{clientID: "client-a"}

			err := ps.unsubscribe(s.clientID, tc.Name)
			assert.Equal(t, errSubscriptionNotFound, err)
		})
	}
}

func TestPubSubPublishConnected(t *testing.T) {
	testCases := []packet.QoS{
		packet.QoS0,
		packet.QoS1,
		packet.QoS2,
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc), func(t *testing.T) {
			gen := &idGeneratorMock{}
			pd := newPacketDeliverMock()
			log := newLogger()
			ss := newSessionStore(gen, log)
			mt := newMetrics(true, log)
			ps := newPubSub(pd, ss, mt, log)

			s := &session{clientID: "client-a", connected: true, version: packet.MQTT311}
			err := ss.saveSession(s)
			require.Nil(t, err)

			sub := &subscription{clientID: s.clientID, topicFilter: "test"}
			err = ps.subscribe(sub)
			require.Nil(t, err)

			pubPkt := packet.NewPublish(1, s.version, sub.topicFilter, tc, 0, 0, nil, nil)
			msg := &message{packetID: pubPkt.PacketID, packet: &pubPkt}

			pd.On("deliverPacket", sub.clientID, mock.Anything)

			err = ps.publish(msg)
			require.Nil(t, err)

			pkt := <-pd.stream
			assert.NotNil(t, pkt)
			assert.Equal(t, msg.packet, pkt)
			pd.AssertExpectations(t)
		})
	}
}

func TestPubSubPublishNotConnected(t *testing.T) {
	gen := &idGeneratorMock{}
	pd := newPacketDeliverMock()
	log := newLogger()
	ss := newSessionStore(gen, log)
	mt := newMetrics(true, log)
	ps := newPubSub(pd, ss, mt, log)

	s := &session{clientID: "client-a", connected: false, version: packet.MQTT311}
	err := ss.saveSession(s)
	require.Nil(t, err)

	sub := &subscription{clientID: s.clientID, topicFilter: "test"}
	err = ps.subscribe(sub)
	require.Nil(t, err)

	pubPkt := packet.NewPublish(1, s.version, sub.topicFilter, packet.QoS1, 0, 0, nil, nil)
	msg := &message{packetID: pubPkt.PacketID, packet: &pubPkt}

	err = ps.publish(msg)
	require.Nil(t, err)
	pd.AssertExpectations(t)
}

func TestPubSubPublishWithoutSubscription(t *testing.T) {
	gen := &idGeneratorMock{}
	pd := newPacketDeliverMock()
	log := newLogger()
	ss := newSessionStore(gen, log)
	mt := newMetrics(true, log)
	ps := newPubSub(pd, ss, mt, log)

	pubPkt := packet.NewPublish(1, packet.MQTT311, "t", packet.QoS0, 0, 0, nil, nil)
	msg := &message{packetID: pubPkt.PacketID, packet: &pubPkt}

	err := ps.publish(msg)
	require.Nil(t, err)
	pd.AssertExpectations(t)
}

func TestPubSubPublishToMultipleSubscriptions(t *testing.T) {
	gen := &idGeneratorMock{}
	pd := newPacketDeliverMock()
	log := newLogger()
	ss := newSessionStore(gen, log)
	mt := newMetrics(true, log)
	ps := newPubSub(pd, ss, mt, log)

	version := packet.MQTT311
	topic := "test"

	s1 := &session{clientID: "client-a", connected: true, version: version}
	err := ss.saveSession(s1)
	require.Nil(t, err)

	s2 := &session{clientID: "client-b", connected: true, version: version}
	err = ss.saveSession(s2)
	require.Nil(t, err)

	sub1 := &subscription{clientID: s1.clientID, topicFilter: topic}
	err = ps.subscribe(sub1)
	require.Nil(t, err)

	sub2 := &subscription{clientID: s2.clientID, topicFilter: topic}
	err = ps.subscribe(sub2)
	require.Nil(t, err)

	pubPkt := packet.NewPublish(1, version, topic, packet.QoS1, 0, 0, nil, nil)
	msg := &message{packetID: pubPkt.PacketID, packet: &pubPkt}

	pd.On("deliverPacket", sub1.clientID, mock.Anything)
	pd.On("deliverPacket", sub2.clientID, mock.Anything)

	err = ps.publish(msg)
	require.Nil(t, err)

	pkt1 := <-pd.stream
	assert.NotNil(t, pkt1)
	assert.Equal(t, msg.packet, pkt1)

	pkt2 := <-pd.stream
	assert.NotNil(t, pkt2)
	assert.Equal(t, msg.packet, pkt2)
	pd.AssertExpectations(t)
}

func TestPubSubPublishAddsToInflightMessages(t *testing.T) {
	testCases := []struct {
		id    packet.ID
		topic string
		qos   packet.QoS
	}{
		{1, "a/0", packet.QoS1},
		{2, "a/0", packet.QoS2},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("ID-%v", tc.id), func(t *testing.T) {
			gen := &idGeneratorMock{}
			pd := newPacketDeliverMock()
			log := newLogger()
			ss := newSessionStore(gen, log)
			mt := newMetrics(true, log)
			ps := newPubSub(pd, ss, mt, log)

			s := &session{clientID: "client-a", connected: true, version: packet.MQTT311}
			err := ss.saveSession(s)
			require.Nil(t, err)

			sub := &subscription{clientID: s.clientID, topicFilter: tc.topic, qos: tc.qos}
			err = ps.subscribe(sub)
			require.Nil(t, err)

			pubPkt := packet.NewPublish(tc.id, s.version, tc.topic, tc.qos, 0, 0, nil, nil)
			msg := &message{packetID: pubPkt.PacketID, packet: &pubPkt}

			pd.On("deliverPacket", sub.clientID, mock.Anything)

			err = ps.publish(msg)
			require.Nil(t, err)
			<-pd.stream

			require.Equal(t, 1, s.inflightMessages.Len())
			ifMsg := s.inflightMessages.Front().Value.(*message)
			assert.Equal(t, msg.id, ifMsg.id)
			assert.Equal(t, 1, ifMsg.tries)
			assert.NotZero(t, ifMsg.lastSent)
			pd.AssertExpectations(t)
		})
	}
}

func TestPubSubPublishCreatesNewPacketID(t *testing.T) {
	testCases := []packet.QoS{
		packet.QoS1,
		packet.QoS2,
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc), func(t *testing.T) {
			gen := &idGeneratorMock{}
			pd := newPacketDeliverMock()
			log := newLogger()
			ss := newSessionStore(gen, log)
			mt := newMetrics(true, log)
			ps := newPubSub(pd, ss, mt, log)

			lastPktID := uint32(1)
			s := &session{clientID: "client-a", connected: true, version: packet.MQTT311, lastPacketID: lastPktID}
			err := ss.saveSession(s)
			require.Nil(t, err)

			sub := &subscription{clientID: s.clientID, topicFilter: "test", qos: tc}
			err = ps.subscribe(sub)
			require.Nil(t, err)

			pubPkt := packet.NewPublish(1, s.version, sub.topicFilter, tc, 0, 0, nil, nil)
			msg := &message{packetID: pubPkt.PacketID, packet: &pubPkt}

			pd.On("deliverPacket", sub.clientID, mock.Anything)

			err = ps.publish(msg)
			require.Nil(t, err)

			pkt := <-pd.stream
			require.NotNil(t, pkt)
			assert.Equal(t, packet.ID(lastPktID+1), pkt.PacketID)

			s, err = ss.readSession(s.clientID)
			require.Nil(t, err)

			assert.Equal(t, uint32(pkt.PacketID), s.lastPacketID)
			pd.AssertExpectations(t)
		})
	}
}

func TestPubSubPublishDifferentVersion(t *testing.T) {
	gen := &idGeneratorMock{}
	pd := newPacketDeliverMock()
	log := newLogger()
	ss := newSessionStore(gen, log)
	mt := newMetrics(true, log)
	ps := newPubSub(pd, ss, mt, log)

	s := &session{clientID: "client-a", connected: true, version: packet.MQTT311}
	err := ss.saveSession(s)
	require.Nil(t, err)

	sub := &subscription{clientID: s.clientID, topicFilter: "test"}
	err = ps.subscribe(sub)
	require.Nil(t, err)

	pubPkt := packet.NewPublish(1, packet.MQTT50, sub.topicFilter, packet.QoS1, 0, 0, nil, nil)
	msg := &message{packetID: pubPkt.PacketID, packet: &pubPkt}

	pd.On("deliverPacket", sub.clientID, mock.Anything)

	err = ps.publish(msg)
	require.Nil(t, err)

	pkt := <-pd.stream
	assert.NotNil(t, pkt)
	assert.Equal(t, s.version, pkt.Version)
	pd.AssertExpectations(t)
}

func TestPubSubPublishDeliverPacketError(t *testing.T) {
	gen := &idGeneratorMock{}
	pd := newPacketDeliverMock()
	log := newLogger()
	ss := newSessionStore(gen, log)
	mt := newMetrics(true, log)
	ps := newPubSub(pd, ss, mt, log)

	s := &session{clientID: "client-a", connected: true, version: packet.MQTT311}
	err := ss.saveSession(s)
	require.Nil(t, err)

	sub := &subscription{clientID: s.clientID, topicFilter: "test"}
	err = ps.subscribe(sub)
	require.Nil(t, err)

	pubPkt := packet.NewPublish(1, s.version, sub.topicFilter, packet.QoS0, 0, 0, nil, nil)
	msg := &message{packetID: pubPkt.PacketID, packet: &pubPkt}

	pd.On("deliverPacket", sub.clientID, mock.Anything).Return(errors.New("failed"))

	err = ps.publish(msg)
	require.Nil(t, err)

	<-pd.stream
	pd.AssertExpectations(t)
}

func TestPubSubPublishToClientReadSessionError(t *testing.T) {
	gen := &idGeneratorMock{}
	pd := newPacketDeliverMock()
	log := newLogger()
	ss := newSessionStore(gen, log)
	mt := newMetrics(true, log)
	ps := newPubSub(pd, ss, mt, log)

	sub := &subscription{clientID: "client-a", topicFilter: "test"}
	err := ps.subscribe(sub)
	require.Nil(t, err)

	pubPkt := packet.NewPublish(1, packet.MQTT311, sub.topicFilter, packet.QoS0, 0, 0, nil, nil)
	msg := &message{packetID: pubPkt.PacketID, packet: &pubPkt}

	ps.publishToClient(sub.clientID, msg)
	require.Nil(t, err)
	pd.AssertExpectations(t)
}
