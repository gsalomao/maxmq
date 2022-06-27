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
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/gsalomao/maxmq/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertSubscription(t *testing.T, tree *subscriptionTree, sub Subscription,
	id ClientID) {

	words := strings.Split(sub.TopicFilter, "/")
	nodes := tree.root.children

	for i, word := range words {
		n, ok := nodes[word]
		require.True(t, ok)

		if i == len(words)-1 {
			assert.Empty(t, n.children)
			require.NotNil(t, n.subscription)
			subscription := n.subscription

			for {
				require.NotNil(t, subscription.Session)

				if bytes.Equal(id, subscription.Session.ClientID) {
					assert.Equal(t, sub.QoS, subscription.QoS)
					assert.Equal(t, sub.RetainHandling,
						subscription.RetainHandling)
					assert.Equal(t, sub.RetainAsPublished,
						subscription.RetainAsPublished)
					assert.Equal(t, sub.NoLocal, subscription.NoLocal)
					break
				} else {
					assert.NotNil(t, subscription.next)
					subscription = subscription.next
				}
			}
		} else {
			assert.Nil(t, n.subscription)
			require.NotEmpty(t, n.children)
			nodes = n.children
		}
	}
}

func TestSubscriptionTree_Insert(t *testing.T) {
	testCases := []string{"a", "/topic", "topic/level", "topic/level/3",
		"topic//test"}
	session := Session{ClientID: ClientID("id")}

	for _, test := range testCases {
		t.Run(test, func(t *testing.T) {
			sub := Subscription{Session: &session, TopicFilter: test,
				QoS: packet.QoS0}

			tree := newSubscriptionTree()
			err := tree.insert(sub)
			assert.Nil(t, err)
			require.Len(t, tree.root.children, 1)
			assertSubscription(t, &tree, sub, session.ClientID)
		})
	}
}

func BenchmarkSubscriptionTree_Insert(b *testing.B) {
	b.ReportAllocs()
	session := Session{ClientID: ClientID("id")}
	tree := newSubscriptionTree()

	for i := 0; i < b.N; i++ {
		sub := Subscription{Session: &session}
		sub.TopicFilter = "sensor/temp/" + fmt.Sprint(i)

		err := tree.insert(sub)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestSubscriptionTree_InsertMultipleSubscriptions(t *testing.T) {
	session := Session{ClientID: ClientID("id")}
	subscriptions := []Subscription{
		{Session: &session, TopicFilter: "topic/0", QoS: packet.QoS0},
		{Session: &session, TopicFilter: "topic/1", QoS: packet.QoS1},
		{Session: &session, TopicFilter: "topic/2", QoS: packet.QoS2},
	}
	tree := newSubscriptionTree()

	err := tree.insert(subscriptions[0])
	require.Nil(t, err)
	err = tree.insert(subscriptions[1])
	require.Nil(t, err)
	err = tree.insert(subscriptions[2])
	require.Nil(t, err)

	assertSubscription(t, &tree, subscriptions[0], session.ClientID)
	assertSubscription(t, &tree, subscriptions[1], session.ClientID)
	assertSubscription(t, &tree, subscriptions[2], session.ClientID)
}

func TestSubscriptionTree_InsertSubscriptionsSameTopic(t *testing.T) {
	subscriptions := []Subscription{
		{Session: &Session{ClientID: ClientID("0")}, RetainHandling: 0,
			RetainAsPublished: false, NoLocal: false,
			TopicFilter: "topic", QoS: packet.QoS0},
		{Session: &Session{ClientID: ClientID("1")}, RetainHandling: 1,
			RetainAsPublished: false, NoLocal: true,
			TopicFilter: "topic", QoS: packet.QoS1},
		{Session: &Session{ClientID: ClientID("2")}, RetainHandling: 2,
			RetainAsPublished: true, NoLocal: false,
			TopicFilter: "topic", QoS: packet.QoS2},
	}

	tree := newSubscriptionTree()

	for _, sub := range subscriptions {
		err := tree.insert(sub)
		require.Nil(t, err)
	}
	for _, sub := range subscriptions {
		assertSubscription(t, &tree, sub, sub.Session.ClientID)
	}
}

func TestSubscriptionTree_InsertTopicFilterWithWildcard(t *testing.T) {
	testCases := []string{"+", "#", "a/+", "a/#", "a/b/+", "a/b/#", "/a/+/c/+",
		"/a/b/c/#"}

	for _, test := range testCases {
		t.Run(test, func(t *testing.T) {
			session := Session{ClientID: ClientID("id")}
			sub := Subscription{Session: &session, TopicFilter: test,
				QoS: packet.QoS0}

			tree := newSubscriptionTree()
			err := tree.insert(sub)
			assert.Nil(t, err)
			assertSubscription(t, &tree, sub, sub.Session.ClientID)
		})
	}
}

func TestSubscriptionTree_InsertInvalidTopicFilter(t *testing.T) {
	testCases := []string{"", "sensor#", "sensor/room#", "sensor/#/temp",
		"sensor+"}

	for _, test := range testCases {
		t.Run(test, func(t *testing.T) {
			session := Session{ClientID: ClientID("id")}
			sub := Subscription{Session: &session, TopicFilter: test,
				QoS: packet.QoS0}

			tree := newSubscriptionTree()
			err := tree.insert(sub)
			assert.NotNil(t, err)
		})
	}
}

func TestSubscriptionTree_InsertSameTopicFilter(t *testing.T) {
	session := Session{ClientID: ClientID("id")}
	subscriptions := []Subscription{
		{Session: &session, TopicFilter: "data", QoS: packet.QoS0},
		{Session: &session, TopicFilter: "data", QoS: packet.QoS1},
		{Session: &session, TopicFilter: "data", QoS: packet.QoS2},
	}
	tree := newSubscriptionTree()

	err := tree.insert(subscriptions[0])
	require.Nil(t, err)
	err = tree.insert(subscriptions[1])
	require.Nil(t, err)
	err = tree.insert(subscriptions[2])
	require.Nil(t, err)

	require.Len(t, tree.root.children, 1)
	node := tree.root.children["data"]
	require.NotNil(t, node.subscription)

	sub := node.subscription
	require.NotNil(t, sub)
	assert.Equal(t, subscriptions[0].TopicFilter, sub.TopicFilter)
	assert.Equal(t, subscriptions[2].QoS, sub.QoS)
	assert.Nil(t, sub.next)
	assert.Empty(t, node.children)
}

func TestSubscriptionTree_Remove(t *testing.T) {
	testCases := []string{"a", "/topic", "topic/level", "topic/level/3",
		"topic//test"}
	session := Session{ClientID: ClientID("id")}

	for _, test := range testCases {
		t.Run(test, func(t *testing.T) {
			sub := Subscription{Session: &session, TopicFilter: test,
				QoS: packet.QoS0}

			tree := newSubscriptionTree()
			err := tree.insert(sub)
			require.Nil(t, err)

			err = tree.remove(session.ClientID, sub.TopicFilter)
			assert.Nil(t, err)
			assert.Empty(t, tree.root.children)
		})
	}
}

func BenchmarkSubscriptionTree_Remove(b *testing.B) {
	b.ReportAllocs()
	session := Session{ClientID: ClientID("id")}
	tree := newSubscriptionTree()

	for i := 0; i < b.N; i++ {
		sub := Subscription{Session: &session}
		sub.TopicFilter = "sensor/temp/" + fmt.Sprint(i)

		err := tree.insert(sub)
		if err != nil {
			b.Fatal(err)
		}
	}

	for i := 0; i < b.N; i++ {
		err := tree.remove(session.ClientID, "sensor/temp/"+fmt.Sprint(i))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestSubscriptionTree_RemoveNoExisting(t *testing.T) {
	testCases := []struct {
		topics []string
	}{
		{topics: []string{"a", "b"}},
		{topics: []string{"/topic", "/data"}},
		{topics: []string{"/topic/level", "/topic/data"}},
		{topics: []string{"/topic/level/#", "/topic/level/2"}},
	}
	session := Session{ClientID: ClientID("id")}

	for _, test := range testCases {
		t.Run(fmt.Sprintf("%s-%s", test.topics[0], test.topics[1]),
			func(t *testing.T) {
				tree := newSubscriptionTree()

				sub1 := Subscription{Session: &session,
					TopicFilter: test.topics[0], QoS: packet.QoS0}
				err := tree.insert(sub1)
				require.Nil(t, err)

				err = tree.remove(session.ClientID, test.topics[1])
				assert.Equal(t, ErrSubscriptionNotFound, err)

				assertSubscription(t, &tree, sub1, session.ClientID)
			})
	}
}

func TestSubscriptionTree_RemoveAllChildren(t *testing.T) {
	testCases := []struct {
		topics []string
	}{
		{topics: []string{"a", "b"}},
		{topics: []string{"/topic", "/data"}},
		{topics: []string{"/topic/level", "/topic/data"}},
		{topics: []string{"/topic/+/1", "/topic/+/2"}},
	}
	session := Session{ClientID: ClientID("id")}

	for _, test := range testCases {
		t.Run(fmt.Sprintf("%s-%s", test.topics[0], test.topics[1]),
			func(t *testing.T) {
				tree := newSubscriptionTree()

				sub1 := Subscription{Session: &session,
					TopicFilter: test.topics[0], QoS: packet.QoS0}
				err := tree.insert(sub1)
				require.Nil(t, err)

				sub2 := Subscription{Session: &session,
					TopicFilter: test.topics[1], QoS: packet.QoS0}
				err = tree.insert(sub2)
				require.Nil(t, err)

				err = tree.remove(session.ClientID, sub2.TopicFilter)
				assert.Nil(t, err)

				assertSubscription(t, &tree, sub1, session.ClientID)
			})
	}
}

func TestSubscriptionTree_RemoveSameTopicFilter(t *testing.T) {
	sessions := []Session{
		{ClientID: ClientID("id-0")},
		{ClientID: ClientID("id-1")},
		{ClientID: ClientID("id-2")},
	}
	subscriptions := []Subscription{
		{Session: &sessions[0], TopicFilter: "data/#", QoS: packet.QoS0},
		{Session: &sessions[1], TopicFilter: "data/#", QoS: packet.QoS1},
		{Session: &sessions[2], TopicFilter: "data/#", QoS: packet.QoS2},
	}
	tree := newSubscriptionTree()

	err := tree.insert(subscriptions[0])
	require.Nil(t, err)
	err = tree.insert(subscriptions[1])
	require.Nil(t, err)
	err = tree.insert(subscriptions[2])
	require.Nil(t, err)
	require.Len(t, tree.root.children, 1)

	err = tree.remove(sessions[1].ClientID, subscriptions[1].TopicFilter)
	assert.Nil(t, err)
	assert.Len(t, tree.root.children, 1)

	err = tree.remove(sessions[0].ClientID, subscriptions[0].TopicFilter)
	assert.Nil(t, err)
	assert.Len(t, tree.root.children, 1)

	err = tree.remove(sessions[2].ClientID, subscriptions[2].TopicFilter)
	assert.Nil(t, err)
	assert.Empty(t, tree.root.children)
}

func TestSubscriptionTree_RemoveSameTopicDifferentSession(t *testing.T) {
	session := Session{ClientID: ClientID("id-0")}
	sub := Subscription{Session: &session, TopicFilter: "data"}
	tree := newSubscriptionTree()

	err := tree.insert(sub)
	require.Nil(t, err)

	err = tree.remove(ClientID("id-1"), sub.TopicFilter)
	assert.Equal(t, ErrSubscriptionNotFound, err)

	assertSubscription(t, &tree, sub, session.ClientID)
}
