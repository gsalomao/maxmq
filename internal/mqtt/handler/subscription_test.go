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
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type subscriptionMgrMock struct {
	mock.Mock
}

func (m *subscriptionMgrMock) Subscribe(s *Subscription) error {
	args := m.Called(s)
	return args.Error(0)
}

func (m *subscriptionMgrMock) Unsubscribe(id packet.ClientID, topic string) error {
	args := m.Called(id, topic)
	return args.Error(0)
}

func (m *subscriptionMgrMock) Publish(msg *Message) error {
	args := m.Called(msg)
	return args.Error(0)
}

func assertSubscription(t *testing.T, tree *SubscriptionTree, s Subscription, id packet.ClientID) {
	words := strings.Split(s.TopicFilter, "/")
	nodes := tree.root.children

	for i, word := range words {
		n, ok := nodes[word]
		require.True(t, ok)

		if i == len(words)-1 {
			assert.Empty(t, n.children)
			require.NotNil(t, n.subscription)
			sub2 := n.subscription

			for {
				if id == sub2.ClientID {
					assert.Equal(t, s.QoS, sub2.QoS)
					assert.Equal(t, s.RetainHandling, sub2.RetainHandling)
					assert.Equal(t, s.NoLocal, sub2.NoLocal)
					assert.Equal(t, s.RetainAsPublished, sub2.RetainAsPublished)
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
	testCases := []string{"a", "/topic", "topic/level", "topic/level/a", "topic//test"}
	id := packet.ClientID("client-1")

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			tree := NewSubscriptionTree()

			sub := Subscription{ClientID: id, TopicFilter: tc, QoS: packet.QoS0}
			exists, err := tree.Insert(sub)
			assert.Nil(t, err)
			assert.False(t, exists)
			require.Len(t, tree.root.children, 1)
			assertSubscription(t, &tree, sub, id)
		})
	}
}

func BenchmarkSubscriptionTreeInsertSubscription(b *testing.B) {
	b.ReportAllocs()
	id := packet.ClientID("client-1")
	tree := NewSubscriptionTree()

	for i := 0; i < b.N; i++ {
		sub := Subscription{ClientID: id}
		sub.TopicFilter = "sensor/temp/" + fmt.Sprint(i)

		_, err := tree.Insert(sub)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestSubscriptionTreeInsertMultipleSubscriptions(t *testing.T) {
	id := packet.ClientID("client-1")
	subs := []Subscription{
		{ClientID: id, TopicFilter: "topic/0", QoS: packet.QoS0},
		{ClientID: id, TopicFilter: "topic/1", QoS: packet.QoS1},
		{ClientID: id, TopicFilter: "topic/2", QoS: packet.QoS2},
	}
	tree := NewSubscriptionTree()

	exists, err := tree.Insert(subs[0])
	require.Nil(t, err)
	require.False(t, exists)
	exists, err = tree.Insert(subs[1])
	require.Nil(t, err)
	require.False(t, exists)
	exists, err = tree.Insert(subs[2])
	require.Nil(t, err)
	require.False(t, exists)

	assertSubscription(t, &tree, subs[0], id)
	assertSubscription(t, &tree, subs[1], id)
	assertSubscription(t, &tree, subs[2], id)
}

func TestSubscriptionTreeInsertSubscriptionsSameTopic(t *testing.T) {
	subs := []Subscription{
		{ClientID: packet.ClientID("0"), TopicFilter: "topic"},
		{ClientID: packet.ClientID("1"), TopicFilter: "topic"},
		{ClientID: packet.ClientID("2"), TopicFilter: "topic"},
		{ClientID: packet.ClientID("3"), TopicFilter: "raw/#"},
		{ClientID: packet.ClientID("4"), TopicFilter: "raw/#"},
		{ClientID: packet.ClientID("5"), TopicFilter: "raw/#"},
	}

	tree := NewSubscriptionTree()

	for _, sub := range subs {
		exists, err := tree.Insert(sub)
		require.Nil(t, err)
		require.False(t, exists)
	}
	for _, sub := range subs {
		assertSubscription(t, &tree, sub, sub.ClientID)
	}
}

func TestSubscriptionTreeInsertTopicFilterWithWildcard(t *testing.T) {
	testCases := []string{"+", "#", "a/+", "a/#", "a/b/+", "a/b/#", "/a/+/c/+", "/a/b/c/#"}
	id := packet.ClientID("client-1")

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			tree := NewSubscriptionTree()

			sub := Subscription{ClientID: id, TopicFilter: tc, QoS: packet.QoS0}
			exists, err := tree.Insert(sub)
			assert.Nil(t, err)
			assert.False(t, exists)
			assertSubscription(t, &tree, sub, sub.ClientID)
		})
	}
}

func TestSubscriptionTreeInsertInvalidTopicFilter(t *testing.T) {
	testCases := []string{"", "sensor#", "sensor/room#", "sensor/#/temp", "sensor+"}
	id := packet.ClientID("client-1")

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			tree := NewSubscriptionTree()

			sub := Subscription{ClientID: id, TopicFilter: tc, QoS: packet.QoS0}
			_, err := tree.Insert(sub)
			assert.NotNil(t, err)
		})
	}
}

func TestSubscriptionTreeInsertSameTopicFilter(t *testing.T) {
	id := packet.ClientID("client-1")
	subs := []Subscription{
		{ClientID: id, TopicFilter: "data", QoS: packet.QoS0},
		{ClientID: id, TopicFilter: "data", QoS: packet.QoS1},
		{ClientID: id, TopicFilter: "data", QoS: packet.QoS2},
	}
	tree := NewSubscriptionTree()

	exists, err := tree.Insert(subs[0])
	require.Nil(t, err)
	require.False(t, exists)
	exists, err = tree.Insert(subs[1])
	require.Nil(t, err)
	require.True(t, exists)
	exists, err = tree.Insert(subs[2])
	require.Nil(t, err)
	require.True(t, exists)

	require.Len(t, tree.root.children, 1)
	node := tree.root.children["data"]
	require.NotNil(t, node.subscription)

	sub := node.subscription
	require.NotNil(t, sub)
	assert.Equal(t, subs[0].TopicFilter, sub.TopicFilter)
	assert.Equal(t, subs[2].QoS, sub.QoS)
	assert.Nil(t, sub.next)
	assert.Empty(t, node.children)
}

func TestSubscriptionTreeInsertWithoutLooseNext(t *testing.T) {
	subs := []Subscription{
		{ClientID: "id-0", TopicFilter: "data", QoS: packet.QoS0},
		{ClientID: "id-1", TopicFilter: "data", QoS: packet.QoS1},
	}
	tree := NewSubscriptionTree()

	_, err := tree.Insert(subs[0])
	require.Nil(t, err)
	_, err = tree.Insert(subs[1])
	require.Nil(t, err)
	_, err = tree.Insert(subs[0])
	require.Nil(t, err)

	require.Len(t, tree.root.children, 1)
	node := tree.root.children["data"]
	require.NotNil(t, node.subscription)

	sub := node.subscription
	require.NotNil(t, sub)
	assert.Equal(t, subs[0].TopicFilter, sub.TopicFilter)
	assert.Equal(t, subs[0].QoS, sub.QoS)
	assert.Empty(t, node.children)
	assert.NotNil(t, sub.next)

	sub = sub.next
	require.NotNil(t, sub)
	assert.Equal(t, subs[1].TopicFilter, sub.TopicFilter)
	assert.Equal(t, subs[1].QoS, sub.QoS)
	assert.Empty(t, node.children)
	assert.Nil(t, sub.next)
}

func TestSubscriptionTreeRemoveSubscription(t *testing.T) {
	testCases := []string{"a", "/topic", "topic/level", "topic/level/3", "topic//test"}
	id := packet.ClientID("client-1")

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			tree := NewSubscriptionTree()

			sub := Subscription{ClientID: id, TopicFilter: tc, QoS: packet.QoS0}
			_, err := tree.Insert(sub)
			require.Nil(t, err)

			err = tree.Remove(id, sub.TopicFilter)
			assert.Nil(t, err)
			assert.Empty(t, tree.root.children)
		})
	}
}

func BenchmarkSubscriptionTreeRemoveSubscription(b *testing.B) {
	b.ReportAllocs()
	id := packet.ClientID("client-1")
	tree := NewSubscriptionTree()

	for i := 0; i < b.N; i++ {
		sub := Subscription{ClientID: id}
		sub.TopicFilter = "sensor/temp/" + fmt.Sprint(i)

		_, err := tree.Insert(sub)
		if err != nil {
			b.Fatal(err)
		}
	}

	for i := 0; i < b.N; i++ {
		err := tree.Remove(id, "sensor/temp/"+fmt.Sprint(i))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestSubscriptionTreeRemoveUnknownSubscription(t *testing.T) {
	testCases := []struct {
		topics []string
	}{
		{[]string{"a", "b"}},
		{[]string{"/topic", "/data"}},
		{[]string{"/topic/level", "/topic/data"}},
		{[]string{"/topic/level/#", "/topic/level/2"}},
	}
	id := packet.ClientID("client-1")

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s-%s", tc.topics[0], tc.topics[1]), func(t *testing.T) {
			tree := NewSubscriptionTree()

			sub := Subscription{ClientID: id, TopicFilter: tc.topics[0], QoS: packet.QoS0}
			_, err := tree.Insert(sub)
			require.Nil(t, err)

			err = tree.Remove(id, tc.topics[1])
			assert.Equal(t, ErrSubscriptionNotFound, err)
			assertSubscription(t, &tree, sub, id)
		})
	}
}

func TestSubscriptionTreeRemoveSiblingSubscription(t *testing.T) {
	testCases := []struct {
		topics []string
	}{
		{[]string{"a", "b"}},
		{[]string{"/topic", "/data"}},
		{[]string{"/topic/level", "/topic/data"}},
		{[]string{"/topic/+/1", "/topic/+/2"}},
	}
	id := packet.ClientID("client-1")

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s-%s", tc.topics[0], tc.topics[1]),
			func(t *testing.T) {
				tree := NewSubscriptionTree()

				sub1 := Subscription{ClientID: id, TopicFilter: tc.topics[0], QoS: packet.QoS0}
				_, err := tree.Insert(sub1)
				require.Nil(t, err)

				sub2 := Subscription{ClientID: id, TopicFilter: tc.topics[1], QoS: packet.QoS0}
				_, err = tree.Insert(sub2)
				require.Nil(t, err)

				err = tree.Remove(id, sub2.TopicFilter)
				assert.Nil(t, err)
				assertSubscription(t, &tree, sub1, id)
			})
	}
}

func TestSubscriptionTreeRemoveSameTopicFilter(t *testing.T) {
	ids := []packet.ClientID{
		packet.ClientID("id-0"),
		packet.ClientID("id-1"),
		packet.ClientID("id-2"),
	}
	subs := []Subscription{
		{ClientID: ids[0], TopicFilter: "data/#", QoS: packet.QoS0},
		{ClientID: ids[1], TopicFilter: "data/#", QoS: packet.QoS1},
		{ClientID: ids[2], TopicFilter: "data/#", QoS: packet.QoS2},
	}
	tree := NewSubscriptionTree()

	_, err := tree.Insert(subs[0])
	require.Nil(t, err)
	_, err = tree.Insert(subs[1])
	require.Nil(t, err)
	_, err = tree.Insert(subs[2])
	require.Nil(t, err)
	require.Len(t, tree.root.children, 1)

	err = tree.Remove(ids[1], subs[1].TopicFilter)
	assert.Nil(t, err)
	assert.Len(t, tree.root.children, 1)

	err = tree.Remove(ids[0], subs[0].TopicFilter)
	assert.Nil(t, err)
	assert.Len(t, tree.root.children, 1)

	err = tree.Remove(ids[2], subs[2].TopicFilter)
	assert.Nil(t, err)
	assert.Empty(t, tree.root.children)
}

func TestSubscriptionTreeRemoveSameTopicDifferentClientID(t *testing.T) {
	id := packet.ClientID("id-0")
	tree := NewSubscriptionTree()

	sub := Subscription{ClientID: id, TopicFilter: "data"}
	_, err := tree.Insert(sub)
	require.Nil(t, err)

	err = tree.Remove("id-1", sub.TopicFilter)
	assert.Equal(t, ErrSubscriptionNotFound, err)

	assertSubscription(t, &tree, sub, id)
}

func TestSubscriptionTreeFindMatches(t *testing.T) {
	testCases := []struct {
		subs    []string
		topic   string
		matches int
	}{
		{[]string{}, "data", 0},
		{[]string{"data1", "data2", "data3"}, "data2", 1},
		{[]string{"data1", "data2", "data3"}, "data4", 0},
		{[]string{"raw/1", "raw/2", "raw/3"}, "raw/2", 1},
		{[]string{"raw/1", "raw/2", "raw/3"}, "raw/4", 0},
		{[]string{"raw/#", "raw/2", "raw/3"}, "raw/3", 2},
		{[]string{"raw/1", "raw/#", "raw/+"}, "raw/1", 3},
		{[]string{"raw/+/1", "raw/temp/1"}, "raw/temp/1", 2},
		{[]string{"data/+/+", "raw/+/2", "raw/+/3"}, "raw/temp/2", 1},
		{[]string{"raw/+", "raw/temp/+", "raw/temp/3"}, "raw/temp/3", 2},
		{[]string{"raw/#", "raw/temp/+", "+/+/+"}, "raw/temp/4", 3},
		{[]string{"raw", "raw/temp", "raw/temp/5"}, "raw/temp/5",
			1},
	}
	id := packet.ClientID("id-0")

	for _, tc := range testCases {
		t.Run(tc.topic, func(t *testing.T) {
			tree := NewSubscriptionTree()

			for _, topic := range tc.subs {
				sub := Subscription{ClientID: id, TopicFilter: topic}
				_, err := tree.Insert(sub)
				require.Nil(t, err)
			}

			subs := tree.FindMatches(tc.topic)
			assert.Equal(t, tc.matches, len(subs))
		})
	}
}

func TestSubscriptionTreeFindMatchesSameTopicFilter(t *testing.T) {
	testCases := []struct {
		topic       string
		clients     int
		topicFilter string
	}{
		{"raw/temp/1", 2, "raw/#"},
		{"raw/1", 3, "raw/1"},
		{"raw/temp", 5, "raw/+"},
	}

	for _, tc := range testCases {
		t.Run(tc.topic, func(t *testing.T) {
			tree := NewSubscriptionTree()

			for i := 0; i < tc.clients; i++ {
				id := packet.ClientID(strconv.Itoa(i))
				sub := Subscription{ClientID: id, TopicFilter: tc.topicFilter}

				_, err := tree.Insert(sub)
				require.Nil(t, err)
			}

			subs := tree.FindMatches(tc.topic)
			assert.Len(t, subs, tc.clients)
		})
	}
}
