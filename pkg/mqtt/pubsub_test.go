// Copyright 2022 The MaxMQ Authors
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

	"github.com/gsalomao/maxmq/mocks"
	packet "github.com/gsalomao/maxmq/pkg/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type messagePublisherMock struct {
	mock.Mock
}

func (d *messagePublisherMock) publishMessage(session *Session,
	msg *message) error {
	args := d.Called(session, msg)
	return args.Error(0)
}

func createPubSub() pubSub {
	logger := mocks.NewLoggerStub()
	pubMock := messagePublisherMock{}
	idGen := &idGeneratorMock{}

	m := newMetrics(true, logger.Logger())
	return newPubSub(&pubMock, idGen, m, logger.Logger())
}

func TestPubSub_StartStop(t *testing.T) {
	ps := createPubSub()
	ps.start()
	ps.stop()
}

func TestPubSub_Run(t *testing.T) {
	ps := createPubSub()
	go ps.run()
	<-ps.action
	ps.action <- pubSubActionPublishMessage
	ps.action <- pubSubActionStop
	<-ps.action
}

func TestPubSub_Subscribe(t *testing.T) {
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
			session := Session{ClientID: ClientID("a")}
			ps := createPubSub()
			subscriptionID := rand.Uint32()

			sub, err := ps.subscribe(&session, test, subscriptionID)
			assert.Nil(t, err)
			assert.Equal(t, subscriptionID, sub.ID)
			assert.Equal(t, test.Name, sub.TopicFilter)
			assert.Equal(t, test.QoS, sub.QoS)
			assert.Equal(t, test.RetainHandling, sub.RetainHandling)
			assert.Equal(t, test.RetainAsPublished, sub.RetainAsPublished)
			assert.Equal(t, test.NoLocal, sub.NoLocal)
		})
	}
}

func TestPubSub_SubscribeError(t *testing.T) {
	session := Session{ClientID: ClientID("a")}
	topic := packet.Topic{Name: "sensor/temp#", QoS: packet.QoS0}
	ps := createPubSub()

	_, err := ps.subscribe(&session, topic, 0)
	assert.NotNil(t, err)
}

func TestPubSub_Unsubscribe(t *testing.T) {
	testCases := []packet.Topic{
		{Name: "sensor"},
		{Name: "sensor/temp"},
		{Name: "sensor/temp/2"},
	}

	for _, test := range testCases {
		t.Run(test.Name, func(t *testing.T) {
			session := Session{ClientID: ClientID("a")}
			ps := createPubSub()

			sub, err := ps.subscribe(&session, test, 0)
			require.Nil(t, err)

			err = ps.unsubscribe(session.ClientID, sub.TopicFilter)
			assert.Nil(t, err)
		})
	}
}

func TestPubSub_UnsubscribeSubscriptionNotFound(t *testing.T) {
	testCases := []packet.Topic{
		{Name: "sensor"},
		{Name: "sensor/temp"},
		{Name: "sensor/temp/2"},
	}

	for _, test := range testCases {
		t.Run(test.Name, func(t *testing.T) {
			session := Session{ClientID: ClientID("a")}
			ps := createPubSub()

			err := ps.unsubscribe(session.ClientID, test.Name)
			assert.Equal(t, ErrSubscriptionNotFound, err)
		})
	}
}

func TestPubSub_PublishQoS0(t *testing.T) {
	ps := createPubSub()
	require.Zero(t, ps.queue.len())

	msgID := 10
	idGen := ps.idGen.(*idGeneratorMock)
	idGen.On("NextID").Return(msgID)

	pkt := packet.NewPublish(1, packet.MQTT311, "test", packet.QoS1,
		0, 0, nil, nil)

	msg := ps.publish(&pkt)
	assert.Equal(t, messageID(msgID), msg.id)
	assert.Equal(t, &pkt, msg.packet)
	assert.Equal(t, 1, ps.queue.len())

	act := <-ps.action
	assert.Equal(t, pubSubActionPublishMessage, act)
}

func TestPubSub_PublishQueuedMessagesNoSubscription(t *testing.T) {
	ps := createPubSub()
	pkt := packet.NewPublish(1, packet.MQTT311, "test", packet.QoS0,
		0, 0, nil, nil)
	msg := message{id: 1, packet: &pkt}
	ps.queue.enqueue(&msg)

	ps.publishQueuedMessages()
	assert.Zero(t, ps.queue.len())
}

func TestPubSub_PublishQueuedMessagesQoS0(t *testing.T) {
	testCases := []struct {
		id    packet.ID
		subs  []string
		topic string
	}{
		{id: 1, subs: []string{"temp"}, topic: "temp"},
		{id: 2, subs: []string{"temp/1", "temp/+", "temp/#"}, topic: "temp/1"},
	}

	for _, test := range testCases {
		name := fmt.Sprintf("%v-%v", test.id, test.topic)
		t.Run(name, func(t *testing.T) {
			ps := createPubSub()
			session := Session{ClientID: ClientID("a")}

			for _, topic := range test.subs {
				sub := Subscription{
					Session:     &session,
					TopicFilter: topic,
					QoS:         packet.QoS0,
				}

				_, err := ps.tree.insert(sub)
				require.Nil(t, err)
			}

			pkt := packet.NewPublish(test.id, packet.MQTT311, test.topic,
				packet.QoS0, 0, 0, nil, nil)
			msg := &message{id: messageID(test.id), packet: &pkt}

			pubMock := ps.publisher.(*messagePublisherMock)
			pubMock.On("publishMessage", &session, msg).Return(nil)

			ps.queue.enqueue(msg)
			ps.publishQueuedMessages()

			assert.Zero(t, ps.queue.len())
			pubMock.AssertNumberOfCalls(t, "publishMessage", len(test.subs))
			pubMock.AssertExpectations(t)
		})
	}
}

func TestPubSub_ProcessQueuedMessagesFailedToDeliver(t *testing.T) {
	ps := createPubSub()
	session := Session{ClientID: ClientID("a")}

	sub := Subscription{
		Session:     &session,
		TopicFilter: "data",
		QoS:         packet.QoS0,
	}

	_, err := ps.tree.insert(sub)
	require.Nil(t, err)

	pkt := packet.NewPublish(1, packet.MQTT311, "data",
		packet.QoS0, 0, 0, nil, nil)
	msg := &message{id: 1, packet: &pkt}

	pubMock := ps.publisher.(*messagePublisherMock)
	pubMock.On("publishMessage", &session, msg).
		Return(errors.New("failed to publish message"))

	ps.queue.enqueue(msg)
	ps.publishQueuedMessages()

	assert.Zero(t, ps.queue.len())
	pubMock.AssertExpectations(t)
}
