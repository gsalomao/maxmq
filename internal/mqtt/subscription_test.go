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
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/gsalomao/maxmq/internal/mqtt/packet"
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
				if id == subscription.ClientID {
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

func TestSubscriptionTreeInsertSubscription(t *testing.T) {
	testCases := []string{"a", "/topic", "topic/level", "topic/level/a",
		"topic//test"}
	id := ClientID("client-1")

	for _, test := range testCases {
		t.Run(test, func(t *testing.T) {
			sub := Subscription{ClientID: id, TopicFilter: test,
				QoS: packet.QoS0}

			tree := newSubscriptionTree()
			exists, err := tree.insert(sub)
			assert.Nil(t, err)
			assert.False(t, exists)
			require.Len(t, tree.root.children, 1)
			assertSubscription(t, &tree, sub, id)
		})
	}
}

func BenchmarkSubscriptionTreeInsertSubscription(b *testing.B) {
	b.ReportAllocs()
	id := ClientID("client-1")
	tree := newSubscriptionTree()

	for i := 0; i < b.N; i++ {
		sub := Subscription{ClientID: id}
		sub.TopicFilter = "sensor/temp/" + fmt.Sprint(i)

		_, err := tree.insert(sub)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestSubscriptionTreeInsertMultipleSubscriptions(t *testing.T) {
	id := ClientID("client-1")
	subscriptions := []Subscription{
		{ClientID: id, TopicFilter: "topic/0", QoS: packet.QoS0},
		{ClientID: id, TopicFilter: "topic/1", QoS: packet.QoS1},
		{ClientID: id, TopicFilter: "topic/2", QoS: packet.QoS2},
	}
	tree := newSubscriptionTree()

	exists, err := tree.insert(subscriptions[0])
	require.Nil(t, err)
	require.False(t, exists)
	exists, err = tree.insert(subscriptions[1])
	require.Nil(t, err)
	require.False(t, exists)
	exists, err = tree.insert(subscriptions[2])
	require.Nil(t, err)
	require.False(t, exists)

	assertSubscription(t, &tree, subscriptions[0], id)
	assertSubscription(t, &tree, subscriptions[1], id)
	assertSubscription(t, &tree, subscriptions[2], id)
}

func TestSubscriptionTreeInsertSubscriptionsSameTopic(t *testing.T) {
	subscriptions := []Subscription{
		{ClientID: ClientID("0"), TopicFilter: "topic"},
		{ClientID: ClientID("1"), TopicFilter: "topic"},
		{ClientID: ClientID("2"), TopicFilter: "topic"},
		{ClientID: ClientID("3"), TopicFilter: "raw/#"},
		{ClientID: ClientID("4"), TopicFilter: "raw/#"},
		{ClientID: ClientID("5"), TopicFilter: "raw/#"},
	}

	tree := newSubscriptionTree()

	for _, sub := range subscriptions {
		exists, err := tree.insert(sub)
		require.Nil(t, err)
		require.False(t, exists)
	}
	for _, sub := range subscriptions {
		assertSubscription(t, &tree, sub, sub.ClientID)
	}
}

func TestSubscriptionTreeInsertTopicFilterWithWildcard(t *testing.T) {
	testCases := []string{"+", "#", "a/+", "a/#", "a/b/+", "a/b/#", "/a/+/c/+",
		"/a/b/c/#"}
	id := ClientID("client-1")

	for _, test := range testCases {
		t.Run(test, func(t *testing.T) {
			sub := Subscription{ClientID: id, TopicFilter: test,
				QoS: packet.QoS0}

			tree := newSubscriptionTree()
			exists, err := tree.insert(sub)
			assert.Nil(t, err)
			assert.False(t, exists)
			assertSubscription(t, &tree, sub, sub.ClientID)
		})
	}
}

func TestSubscriptionTreeInsertInvalidTopicFilter(t *testing.T) {
	testCases := []string{"", "sensor#", "sensor/room#", "sensor/#/temp",
		"sensor+"}
	id := ClientID("client-1")

	for _, test := range testCases {
		t.Run(test, func(t *testing.T) {
			sub := Subscription{ClientID: id, TopicFilter: test,
				QoS: packet.QoS0}

			tree := newSubscriptionTree()
			_, err := tree.insert(sub)
			assert.NotNil(t, err)
		})
	}
}

func TestSubscriptionTreeInsertSameTopicFilter(t *testing.T) {
	id := ClientID("client-1")
	subscriptions := []Subscription{
		{ClientID: id, TopicFilter: "data", QoS: packet.QoS0},
		{ClientID: id, TopicFilter: "data", QoS: packet.QoS1},
		{ClientID: id, TopicFilter: "data", QoS: packet.QoS2},
	}
	tree := newSubscriptionTree()

	exists, err := tree.insert(subscriptions[0])
	require.Nil(t, err)
	require.False(t, exists)
	exists, err = tree.insert(subscriptions[1])
	require.Nil(t, err)
	require.True(t, exists)
	exists, err = tree.insert(subscriptions[2])
	require.Nil(t, err)
	require.True(t, exists)

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

func TestSubscriptionTreeInsertWithoutLooseNext(t *testing.T) {
	subscriptions := []Subscription{
		{ClientID: "id-0", TopicFilter: "data", QoS: packet.QoS0},
		{ClientID: "id-1", TopicFilter: "data", QoS: packet.QoS1},
	}
	tree := newSubscriptionTree()

	_, err := tree.insert(subscriptions[0])
	require.Nil(t, err)
	_, err = tree.insert(subscriptions[1])
	require.Nil(t, err)
	_, err = tree.insert(subscriptions[0])
	require.Nil(t, err)

	require.Len(t, tree.root.children, 1)
	node := tree.root.children["data"]
	require.NotNil(t, node.subscription)

	sub := node.subscription
	require.NotNil(t, sub)
	assert.Equal(t, subscriptions[0].TopicFilter, sub.TopicFilter)
	assert.Equal(t, subscriptions[0].QoS, sub.QoS)
	assert.Empty(t, node.children)
	assert.NotNil(t, sub.next)

	sub = sub.next
	require.NotNil(t, sub)
	assert.Equal(t, subscriptions[1].TopicFilter, sub.TopicFilter)
	assert.Equal(t, subscriptions[1].QoS, sub.QoS)
	assert.Empty(t, node.children)
	assert.Nil(t, sub.next)
}

func TestSubscriptionTreeRemoveSubscription(t *testing.T) {
	testCases := []string{"a", "/topic", "topic/level", "topic/level/3",
		"topic//test"}
	id := ClientID("client-1")

	for _, test := range testCases {
		t.Run(test, func(t *testing.T) {
			sub := Subscription{ClientID: id, TopicFilter: test,
				QoS: packet.QoS0}

			tree := newSubscriptionTree()
			_, err := tree.insert(sub)
			require.Nil(t, err)

			err = tree.remove(id, sub.TopicFilter)
			assert.Nil(t, err)
			assert.Empty(t, tree.root.children)
		})
	}
}

func BenchmarkSubscriptionTreeRemoveSubscription(b *testing.B) {
	b.ReportAllocs()
	id := ClientID("client-1")
	tree := newSubscriptionTree()

	for i := 0; i < b.N; i++ {
		sub := Subscription{ClientID: id}
		sub.TopicFilter = "sensor/temp/" + fmt.Sprint(i)

		_, err := tree.insert(sub)
		if err != nil {
			b.Fatal(err)
		}
	}

	for i := 0; i < b.N; i++ {
		err := tree.remove(id, "sensor/temp/"+fmt.Sprint(i))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestSubscriptionTreeRemoveUnknownSubscription(t *testing.T) {
	testCases := []struct {
		topics []string
	}{
		{topics: []string{"a", "b"}},
		{topics: []string{"/topic", "/data"}},
		{topics: []string{"/topic/level", "/topic/data"}},
		{topics: []string{"/topic/level/#", "/topic/level/2"}},
	}
	id := ClientID("client-1")

	for _, test := range testCases {
		t.Run(fmt.Sprintf("%s-%s", test.topics[0], test.topics[1]),
			func(t *testing.T) {
				tree := newSubscriptionTree()

				sub1 := Subscription{ClientID: id, TopicFilter: test.topics[0],
					QoS: packet.QoS0}

				_, err := tree.insert(sub1)
				require.Nil(t, err)

				err = tree.remove(id, test.topics[1])
				assert.Equal(t, ErrSubscriptionNotFound, err)
				assertSubscription(t, &tree, sub1, id)
			})
	}
}

func TestSubscriptionTreeRemoveSiblingSubscription(t *testing.T) {
	testCases := []struct {
		topics []string
	}{
		{topics: []string{"a", "b"}},
		{topics: []string{"/topic", "/data"}},
		{topics: []string{"/topic/level", "/topic/data"}},
		{topics: []string{"/topic/+/1", "/topic/+/2"}},
	}
	id := ClientID("client-1")

	for _, test := range testCases {
		t.Run(fmt.Sprintf("%s-%s", test.topics[0], test.topics[1]),
			func(t *testing.T) {
				tree := newSubscriptionTree()

				sub1 := Subscription{ClientID: id, TopicFilter: test.topics[0],
					QoS: packet.QoS0}
				_, err := tree.insert(sub1)
				require.Nil(t, err)

				sub2 := Subscription{ClientID: id, TopicFilter: test.topics[1],
					QoS: packet.QoS0}
				_, err = tree.insert(sub2)
				require.Nil(t, err)

				err = tree.remove(id, sub2.TopicFilter)
				assert.Nil(t, err)
				assertSubscription(t, &tree, sub1, id)
			})
	}
}

func TestSubscriptionTreeRemoveSameTopicFilter(t *testing.T) {
	ids := []ClientID{ClientID("id-0"), ClientID("id-1"), ClientID("id-2")}
	subscriptions := []Subscription{
		{ClientID: ids[0], TopicFilter: "data/#", QoS: packet.QoS0},
		{ClientID: ids[1], TopicFilter: "data/#", QoS: packet.QoS1},
		{ClientID: ids[2], TopicFilter: "data/#", QoS: packet.QoS2},
	}
	tree := newSubscriptionTree()

	_, err := tree.insert(subscriptions[0])
	require.Nil(t, err)
	_, err = tree.insert(subscriptions[1])
	require.Nil(t, err)
	_, err = tree.insert(subscriptions[2])
	require.Nil(t, err)
	require.Len(t, tree.root.children, 1)

	err = tree.remove(ids[1], subscriptions[1].TopicFilter)
	assert.Nil(t, err)
	assert.Len(t, tree.root.children, 1)

	err = tree.remove(ids[0], subscriptions[0].TopicFilter)
	assert.Nil(t, err)
	assert.Len(t, tree.root.children, 1)

	err = tree.remove(ids[2], subscriptions[2].TopicFilter)
	assert.Nil(t, err)
	assert.Empty(t, tree.root.children)
}

func TestSubscriptionTreeRemoveSameTopicDifferentClientID(t *testing.T) {
	id := ClientID("id-0")
	sub := Subscription{ClientID: id, TopicFilter: "data"}
	tree := newSubscriptionTree()

	_, err := tree.insert(sub)
	require.Nil(t, err)

	err = tree.remove("id-1", sub.TopicFilter)
	assert.Equal(t, ErrSubscriptionNotFound, err)

	assertSubscription(t, &tree, sub, id)
}

func TestSubscriptionTreeFindMatches(t *testing.T) {
	testCases := []struct {
		subs    []string
		topic   string
		matches int
	}{
		{subs: []string{}, topic: "data", matches: 0},
		{subs: []string{"data1", "data2", "data3"}, topic: "data2", matches: 1},
		{subs: []string{"data1", "data2", "data3"}, topic: "data4", matches: 0},
		{subs: []string{"raw/1", "raw/2", "raw/3"}, topic: "raw/2", matches: 1},
		{subs: []string{"raw/1", "raw/2", "raw/3"}, topic: "raw/4", matches: 0},
		{subs: []string{"raw/#", "raw/2", "raw/3"}, topic: "raw/3", matches: 2},
		{subs: []string{"raw/1", "raw/#", "raw/+"}, topic: "raw/1", matches: 3},
		{subs: []string{"raw/+/1", "raw/temp/1"}, topic: "raw/temp/1",
			matches: 2},
		{subs: []string{"data/+/+", "raw/+/2", "raw/+/3"}, topic: "raw/temp/2",
			matches: 1},
		{subs: []string{"raw/+", "raw/temp/+", "raw/temp/3"},
			topic: "raw/temp/3", matches: 2},
		{subs: []string{"raw/#", "raw/temp/+", "+/+/+"}, topic: "raw/temp/4",
			matches: 3},
		{subs: []string{"raw", "raw/temp", "raw/temp/5"}, topic: "raw/temp/5",
			matches: 1},
	}
	id := ClientID("id-0")

	for _, test := range testCases {
		t.Run(test.topic, func(t *testing.T) {
			tree := newSubscriptionTree()

			for _, topic := range test.subs {
				sub := Subscription{ClientID: id, TopicFilter: topic}
				_, err := tree.insert(sub)
				require.Nil(t, err)
			}

			subs := tree.findMatches(test.topic)
			assert.Equal(t, test.matches, len(subs))
		})
	}
}

func TestSubscriptionTreeFindMatchesSameTopicFilter(t *testing.T) {
	testCases := []struct {
		topic       string
		clients     int
		topicFilter string
	}{
		{topic: "raw/temp/1", clients: 2, topicFilter: "raw/#"},
		{topic: "raw/1", clients: 3, topicFilter: "raw/1"},
		{topic: "raw/temp", clients: 5, topicFilter: "raw/+"},
	}

	for _, test := range testCases {
		t.Run(test.topic, func(t *testing.T) {
			tree := newSubscriptionTree()

			for i := 0; i < test.clients; i++ {
				id := ClientID(strconv.Itoa(i))
				sub := Subscription{ClientID: id, TopicFilter: test.topicFilter}

				_, err := tree.insert(sub)
				require.Nil(t, err)
			}

			subs := tree.findMatches(test.topic)
			assert.Len(t, subs, test.clients)
		})
	}
}
