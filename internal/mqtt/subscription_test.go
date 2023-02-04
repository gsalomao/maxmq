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

func assertSubscription(t *testing.T, tree *subscriptionTree, sub subscription,
	id clientID) {

	words := strings.Split(sub.topicFilter, "/")
	nodes := tree.root.children

	for i, word := range words {
		n, ok := nodes[word]
		require.True(t, ok)

		if i == len(words)-1 {
			assert.Empty(t, n.children)
			require.NotNil(t, n.subscription)
			sub2 := n.subscription

			for {
				if id == sub2.clientID {
					assert.Equal(t, sub.qos, sub2.qos)
					assert.Equal(t, sub.retainHandling, sub2.retainHandling)
					assert.Equal(t, sub.noLocal, sub2.noLocal)
					assert.Equal(t, sub.retainAsPublished,
						sub2.retainAsPublished)
					break
				} else {
					assert.NotNil(t, sub2.next)
					sub2 = sub2.next
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
	id := clientID("client-1")

	for _, test := range testCases {
		t.Run(test, func(t *testing.T) {
			sub := subscription{clientID: id, topicFilter: test,
				qos: packet.QoS0}

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
	id := clientID("client-1")
	tree := newSubscriptionTree()

	for i := 0; i < b.N; i++ {
		sub := subscription{clientID: id}
		sub.topicFilter = "sensor/temp/" + fmt.Sprint(i)

		_, err := tree.insert(sub)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestSubscriptionTreeInsertMultipleSubscriptions(t *testing.T) {
	id := clientID("client-1")
	subs := []subscription{
		{clientID: id, topicFilter: "topic/0", qos: packet.QoS0},
		{clientID: id, topicFilter: "topic/1", qos: packet.QoS1},
		{clientID: id, topicFilter: "topic/2", qos: packet.QoS2},
	}
	tree := newSubscriptionTree()

	exists, err := tree.insert(subs[0])
	require.Nil(t, err)
	require.False(t, exists)
	exists, err = tree.insert(subs[1])
	require.Nil(t, err)
	require.False(t, exists)
	exists, err = tree.insert(subs[2])
	require.Nil(t, err)
	require.False(t, exists)

	assertSubscription(t, &tree, subs[0], id)
	assertSubscription(t, &tree, subs[1], id)
	assertSubscription(t, &tree, subs[2], id)
}

func TestSubscriptionTreeInsertSubscriptionsSameTopic(t *testing.T) {
	subs := []subscription{
		{clientID: clientID("0"), topicFilter: "topic"},
		{clientID: clientID("1"), topicFilter: "topic"},
		{clientID: clientID("2"), topicFilter: "topic"},
		{clientID: clientID("3"), topicFilter: "raw/#"},
		{clientID: clientID("4"), topicFilter: "raw/#"},
		{clientID: clientID("5"), topicFilter: "raw/#"},
	}

	tree := newSubscriptionTree()

	for _, sub := range subs {
		exists, err := tree.insert(sub)
		require.Nil(t, err)
		require.False(t, exists)
	}
	for _, sub := range subs {
		assertSubscription(t, &tree, sub, sub.clientID)
	}
}

func TestSubscriptionTreeInsertTopicFilterWithWildcard(t *testing.T) {
	testCases := []string{"+", "#", "a/+", "a/#", "a/b/+", "a/b/#", "/a/+/c/+",
		"/a/b/c/#"}
	id := clientID("client-1")

	for _, test := range testCases {
		t.Run(test, func(t *testing.T) {
			sub := subscription{clientID: id, topicFilter: test,
				qos: packet.QoS0}

			tree := newSubscriptionTree()
			exists, err := tree.insert(sub)
			assert.Nil(t, err)
			assert.False(t, exists)
			assertSubscription(t, &tree, sub, sub.clientID)
		})
	}
}

func TestSubscriptionTreeInsertInvalidTopicFilter(t *testing.T) {
	testCases := []string{"", "sensor#", "sensor/room#", "sensor/#/temp",
		"sensor+"}
	id := clientID("client-1")

	for _, test := range testCases {
		t.Run(test, func(t *testing.T) {
			sub := subscription{clientID: id, topicFilter: test,
				qos: packet.QoS0}

			tree := newSubscriptionTree()
			_, err := tree.insert(sub)
			assert.NotNil(t, err)
		})
	}
}

func TestSubscriptionTreeInsertSameTopicFilter(t *testing.T) {
	id := clientID("client-1")
	subs := []subscription{
		{clientID: id, topicFilter: "data", qos: packet.QoS0},
		{clientID: id, topicFilter: "data", qos: packet.QoS1},
		{clientID: id, topicFilter: "data", qos: packet.QoS2},
	}
	tree := newSubscriptionTree()

	exists, err := tree.insert(subs[0])
	require.Nil(t, err)
	require.False(t, exists)
	exists, err = tree.insert(subs[1])
	require.Nil(t, err)
	require.True(t, exists)
	exists, err = tree.insert(subs[2])
	require.Nil(t, err)
	require.True(t, exists)

	require.Len(t, tree.root.children, 1)
	node := tree.root.children["data"]
	require.NotNil(t, node.subscription)

	sub := node.subscription
	require.NotNil(t, sub)
	assert.Equal(t, subs[0].topicFilter, sub.topicFilter)
	assert.Equal(t, subs[2].qos, sub.qos)
	assert.Nil(t, sub.next)
	assert.Empty(t, node.children)
}

func TestSubscriptionTreeInsertWithoutLooseNext(t *testing.T) {
	subs := []subscription{
		{clientID: "id-0", topicFilter: "data", qos: packet.QoS0},
		{clientID: "id-1", topicFilter: "data", qos: packet.QoS1},
	}
	tree := newSubscriptionTree()

	_, err := tree.insert(subs[0])
	require.Nil(t, err)
	_, err = tree.insert(subs[1])
	require.Nil(t, err)
	_, err = tree.insert(subs[0])
	require.Nil(t, err)

	require.Len(t, tree.root.children, 1)
	node := tree.root.children["data"]
	require.NotNil(t, node.subscription)

	sub := node.subscription
	require.NotNil(t, sub)
	assert.Equal(t, subs[0].topicFilter, sub.topicFilter)
	assert.Equal(t, subs[0].qos, sub.qos)
	assert.Empty(t, node.children)
	assert.NotNil(t, sub.next)

	sub = sub.next
	require.NotNil(t, sub)
	assert.Equal(t, subs[1].topicFilter, sub.topicFilter)
	assert.Equal(t, subs[1].qos, sub.qos)
	assert.Empty(t, node.children)
	assert.Nil(t, sub.next)
}

func TestSubscriptionTreeRemoveSubscription(t *testing.T) {
	testCases := []string{"a", "/topic", "topic/level", "topic/level/3",
		"topic//test"}
	id := clientID("client-1")

	for _, test := range testCases {
		t.Run(test, func(t *testing.T) {
			sub := subscription{clientID: id, topicFilter: test,
				qos: packet.QoS0}

			tree := newSubscriptionTree()
			_, err := tree.insert(sub)
			require.Nil(t, err)

			err = tree.remove(id, sub.topicFilter)
			assert.Nil(t, err)
			assert.Empty(t, tree.root.children)
		})
	}
}

func BenchmarkSubscriptionTreeRemoveSubscription(b *testing.B) {
	b.ReportAllocs()
	id := clientID("client-1")
	tree := newSubscriptionTree()

	for i := 0; i < b.N; i++ {
		sub := subscription{clientID: id}
		sub.topicFilter = "sensor/temp/" + fmt.Sprint(i)

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
	id := clientID("client-1")

	for _, test := range testCases {
		t.Run(fmt.Sprintf("%s-%s", test.topics[0], test.topics[1]),
			func(t *testing.T) {
				tree := newSubscriptionTree()

				sub := subscription{clientID: id, topicFilter: test.topics[0],
					qos: packet.QoS0}

				_, err := tree.insert(sub)
				require.Nil(t, err)

				err = tree.remove(id, test.topics[1])
				assert.Equal(t, errSubscriptionNotFound, err)
				assertSubscription(t, &tree, sub, id)
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
	id := clientID("client-1")

	for _, test := range testCases {
		t.Run(fmt.Sprintf("%s-%s", test.topics[0], test.topics[1]),
			func(t *testing.T) {
				tree := newSubscriptionTree()

				sub1 := subscription{clientID: id, topicFilter: test.topics[0],
					qos: packet.QoS0}
				_, err := tree.insert(sub1)
				require.Nil(t, err)

				sub2 := subscription{clientID: id, topicFilter: test.topics[1],
					qos: packet.QoS0}
				_, err = tree.insert(sub2)
				require.Nil(t, err)

				err = tree.remove(id, sub2.topicFilter)
				assert.Nil(t, err)
				assertSubscription(t, &tree, sub1, id)
			})
	}
}

func TestSubscriptionTreeRemoveSameTopicFilter(t *testing.T) {
	ids := []clientID{clientID("id-0"), clientID("id-1"), clientID("id-2")}
	subs := []subscription{
		{clientID: ids[0], topicFilter: "data/#", qos: packet.QoS0},
		{clientID: ids[1], topicFilter: "data/#", qos: packet.QoS1},
		{clientID: ids[2], topicFilter: "data/#", qos: packet.QoS2},
	}
	tree := newSubscriptionTree()

	_, err := tree.insert(subs[0])
	require.Nil(t, err)
	_, err = tree.insert(subs[1])
	require.Nil(t, err)
	_, err = tree.insert(subs[2])
	require.Nil(t, err)
	require.Len(t, tree.root.children, 1)

	err = tree.remove(ids[1], subs[1].topicFilter)
	assert.Nil(t, err)
	assert.Len(t, tree.root.children, 1)

	err = tree.remove(ids[0], subs[0].topicFilter)
	assert.Nil(t, err)
	assert.Len(t, tree.root.children, 1)

	err = tree.remove(ids[2], subs[2].topicFilter)
	assert.Nil(t, err)
	assert.Empty(t, tree.root.children)
}

func TestSubscriptionTreeRemoveSameTopicDifferentClientID(t *testing.T) {
	id := clientID("id-0")
	sub := subscription{clientID: id, topicFilter: "data"}
	tree := newSubscriptionTree()

	_, err := tree.insert(sub)
	require.Nil(t, err)

	err = tree.remove("id-1", sub.topicFilter)
	assert.Equal(t, errSubscriptionNotFound, err)

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
	id := clientID("id-0")

	for _, test := range testCases {
		t.Run(test.topic, func(t *testing.T) {
			tree := newSubscriptionTree()

			for _, topic := range test.subs {
				sub := subscription{clientID: id, topicFilter: topic}
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
				id := clientID(strconv.Itoa(i))
				sub := subscription{clientID: id, topicFilter: test.topicFilter}

				_, err := tree.insert(sub)
				require.Nil(t, err)
			}

			subs := tree.findMatches(test.topic)
			assert.Len(t, subs, test.clients)
		})
	}
}
