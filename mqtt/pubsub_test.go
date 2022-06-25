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
	"testing"

	"github.com/gsalomao/maxmq/mocks"
	"github.com/gsalomao/maxmq/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createPubSub() pubSub {
	logger := mocks.NewLoggerStub()
	m := newMetrics(true, logger.Logger())
	return newPubSub(m, logger.Logger())
}

func TestPubSub_Subscribe(t *testing.T) {
	testCases := []packet.Topic{
		{Name: []byte("sensor/temp0"), QoS: packet.QoS0,
			RetainHandling: 0, RetainAsPublished: false, NoLocal: false},
		{Name: []byte("sensor/temp1"), QoS: packet.QoS1,
			RetainHandling: 1, RetainAsPublished: false, NoLocal: true},
		{Name: []byte("sensor/temp2"), QoS: packet.QoS2,
			RetainHandling: 0, RetainAsPublished: true, NoLocal: false},
	}

	for _, test := range testCases {
		t.Run(string(test.Name), func(t *testing.T) {
			session := Session{ClientID: ClientID("a")}
			ps := createPubSub()

			sub, err := ps.subscribe(&session, test)
			assert.Nil(t, err)
			assert.Equal(t, string(test.Name), sub.TopicFilter)
			assert.Equal(t, test.QoS, sub.QoS)
			assert.Equal(t, test.RetainHandling, sub.RetainHandling)
			assert.Equal(t, test.RetainAsPublished, sub.RetainAsPublished)
			assert.Equal(t, test.NoLocal, sub.NoLocal)
		})
	}
}

func TestPubSub_SubscribeError(t *testing.T) {
	session := Session{ClientID: ClientID("a")}
	topic := packet.Topic{Name: []byte("sensor/temp#"), QoS: packet.QoS0}
	ps := createPubSub()

	_, err := ps.subscribe(&session, topic)
	assert.NotNil(t, err)
}

func TestPubSub_Unsubscribe(t *testing.T) {
	testCases := []packet.Topic{
		{Name: []byte("sensor")},
		{Name: []byte("sensor/temp")},
		{Name: []byte("sensor/temp/2")},
	}

	for _, test := range testCases {
		t.Run(string(test.Name), func(t *testing.T) {
			session := Session{ClientID: ClientID("a")}
			ps := createPubSub()

			sub, err := ps.subscribe(&session, test)
			require.Nil(t, err)

			err = ps.unsubscribe(session.ClientID, sub.TopicFilter)
			assert.Nil(t, err)
		})
	}
}

func TestPubSub_UnsubscribeSubscriptionNotFound(t *testing.T) {
	testCases := []packet.Topic{
		{Name: []byte("sensor")},
		{Name: []byte("sensor/temp")},
		{Name: []byte("sensor/temp/2")},
	}

	for _, test := range testCases {
		t.Run(string(test.Name), func(t *testing.T) {
			session := Session{ClientID: ClientID("a")}
			ps := createPubSub()

			err := ps.unsubscribe(session.ClientID, string(test.Name))
			assert.Equal(t, ErrSubscriptionNotFound, err)
		})
	}
}
