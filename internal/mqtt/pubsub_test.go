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
	"math/rand"
	"testing"
	"time"

	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type messagePublisherMock struct {
	mock.Mock
}

func (d *messagePublisherMock) publishMessage(id clientID, msg *message) error {
	args := d.Called(id, msg)
	return args.Error(0)
}

func createPubSubManager() *pubSubManager {
	log := newLogger()
	pubMock := &messagePublisherMock{}

	m := newMetrics(true, &log)
	ps := newPubSubManager(m, &log)
	ps.publisher = pubMock
	return ps
}

func TestPubSubManagerStartStop(t *testing.T) {
	ps := createPubSubManager()
	ps.start()
	<-time.After(10 * time.Millisecond)
	ps.stop()
}

func TestPubSubManagerRun(t *testing.T) {
	ps := createPubSubManager()
	go ps.run()
	<-ps.action
	ps.action <- pubSubActionPublishMessage
	ps.action <- pubSubActionStop
	<-ps.action
}

func TestPubSubManagerSubscribeTopic(t *testing.T) {
	testCases := []packet.Topic{
		{Name: "sensor/temp0", QoS: packet.QoS0, RetainHandling: 0,
			RetainAsPublished: false, NoLocal: false},
		{Name: "sensor/temp1", QoS: packet.QoS1, RetainHandling: 1,
			RetainAsPublished: false, NoLocal: true},
		{Name: "sensor/temp2", QoS: packet.QoS2, RetainHandling: 0,
			RetainAsPublished: true, NoLocal: false},
	}

	for _, test := range testCases {
		t.Run(test.Name, func(t *testing.T) {
			s := &session{clientID: "a"}
			ps := createPubSubManager()
			subscriptionID := rand.Int()

			sub, err := ps.subscribe(s, test, subscriptionID)
			assert.Nil(t, err)
			assert.Equal(t, subscriptionID, sub.id)
			assert.Equal(t, test.Name, sub.topicFilter)
			assert.Equal(t, test.QoS, sub.qos)
			assert.Equal(t, test.RetainHandling, sub.retainHandling)
			assert.Equal(t, test.RetainAsPublished, sub.retainAsPublished)
			assert.Equal(t, test.NoLocal, sub.noLocal)
		})
	}
}

func TestPubSubManagerSubscribeError(t *testing.T) {
	s := &session{clientID: "a"}
	topic := packet.Topic{Name: "sensor/temp#", QoS: packet.QoS0}
	ps := createPubSubManager()

	_, err := ps.subscribe(s, topic, 0)
	assert.NotNil(t, err)
}

func TestPubSubManagerUnsubscribeTopic(t *testing.T) {
	testCases := []packet.Topic{
		{Name: "sensor"},
		{Name: "sensor/temp"},
		{Name: "sensor/temp/2"},
	}

	for _, test := range testCases {
		t.Run(test.Name, func(t *testing.T) {
			s := &session{clientID: "a"}
			ps := createPubSubManager()

			sub, err := ps.subscribe(s, test, 0)
			require.Nil(t, err)

			err = ps.unsubscribe(s.clientID, sub.topicFilter)
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

	for _, test := range testCases {
		t.Run(test.Name, func(t *testing.T) {
			s := session{clientID: "a"}
			ps := createPubSubManager()

			err := ps.unsubscribe(s.clientID, test.Name)
			assert.Equal(t, errSubscriptionNotFound, err)
		})
	}
}

func TestPubSubManagerPublishMessage(t *testing.T) {
	ps := createPubSubManager()
	require.Zero(t, ps.queue.len())

	pkt := packet.NewPublish(1, packet.MQTT311, "test", packet.QoS1,
		0, 0, nil, nil)
	msg := &message{packetID: pkt.PacketID, packet: &pkt}

	ps.publish(msg)

	act := <-ps.action
	assert.Equal(t, pubSubActionPublishMessage, act)

	assert.Equal(t, 1, ps.queue.len())
	assert.Same(t, msg, ps.queue.list.Front().Value.(*message))
}

func TestPubSubManagerPublishQueuedMessagesNoSubscription(t *testing.T) {
	ps := createPubSubManager()
	pkt := packet.NewPublish(1, packet.MQTT311, "test", packet.QoS0,
		0, 0, nil, nil)
	msg := message{packetID: pkt.PacketID, packet: &pkt}
	ps.queue.enqueue(&msg)

	ps.publishQueuedMessages()
	assert.Zero(t, ps.queue.len())
}

func TestPubSubManagerPublishQueuedQoS0Message(t *testing.T) {
	testCases := []struct {
		name  string
		id    packet.ID
		subs  []string
		topic string
	}{
		{name: "SingleSubscriber", id: 1, subs: []string{"temp"},
			topic: "temp"},
		{name: "MultipleSubscriber", id: 2,
			subs: []string{"temp/1", "temp/+", "temp/#"}, topic: "temp/1"},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			ps := createPubSubManager()
			id := clientID("client-a")

			for _, topic := range test.subs {
				sub := subscription{clientID: id, topicFilter: topic,
					qos: packet.QoS0}

				_, err := ps.tree.insert(sub)
				require.Nil(t, err)
			}

			pkt := packet.NewPublish(test.id, packet.MQTT311, test.topic,
				packet.QoS0, 0, 0, nil, nil)
			msg := &message{packetID: pkt.PacketID, packet: &pkt}

			pubMock := ps.publisher.(*messagePublisherMock)
			pubMock.On("publishMessage", id, msg).Return(nil)

			ps.queue.enqueue(msg)
			ps.publishQueuedMessages()

			assert.Zero(t, ps.queue.len())
			pubMock.AssertNumberOfCalls(t, "publishMessage", len(test.subs))
			pubMock.AssertExpectations(t)
		})
	}
}

func TestPubSubManagerProcessQueuedMessagesFailedToDeliver(t *testing.T) {
	ps := createPubSubManager()
	id := clientID("client-1")
	sub := subscription{clientID: id, topicFilter: "data", qos: packet.QoS0}

	_, err := ps.tree.insert(sub)
	require.Nil(t, err)

	pkt := packet.NewPublish(1, packet.MQTT311, "data", packet.QoS0, 0, 0,
		nil, nil)
	msg := &message{packetID: pkt.PacketID, packet: &pkt}

	pubMock := ps.publisher.(*messagePublisherMock)
	pubMock.On("publishMessage", id, msg).
		Return(errors.New("failed to publish message"))

	ps.queue.enqueue(msg)
	ps.publishQueuedMessages()

	assert.Zero(t, ps.queue.len())
	pubMock.AssertExpectations(t)
}
