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

func addSubscriptions(trie *subscriptionTrie, session *Session, prefix string,
	numOfTopics int) error {

	sub := Subscription{Session: session}
	for i := 0; i < numOfTopics; i++ {
		topic := prefix + fmt.Sprint(i)
		sub.TopicFilter = []byte(topic)

		err := trie.insert(sub)
		if err != nil {
			return err
		}
	}
	return nil
}

func checkTrie(t *testing.T, trie *subscriptionTrie, sub Subscription,
	id ClientID) {

	words := strings.Split(string(sub.TopicFilter), "/")
	nodes := trie.nodes

	for i, word := range words {
		n, ok := nodes[word]
		require.True(t, ok)

		if i == len(words)-1 {
			for {
				require.NotNil(t, n.subscription)
				require.NotNil(t, n.subscription.Session)
				assert.Empty(t, n.children)

				if bytes.Equal(id, n.subscription.Session.ClientID) {
					assert.Equal(t, sub.QoS, n.subscription.QoS)
					assert.Equal(t, sub.RetainHandling,
						n.subscription.RetainHandling)
					assert.Equal(t, sub.RetainAsPublished,
						n.subscription.RetainAsPublished)
					assert.Equal(t, sub.NoLocal, n.subscription.NoLocal)
					break
				} else {
					assert.NotNil(t, n.next)
					n = n.next
				}
			}
		} else {
			assert.Nil(t, n.subscription)
			assert.Empty(t, n.next)
			require.NotEmpty(t, n.children)
			nodes = n.children
		}
	}
}

func TestSubscriptionTrie_Insert(t *testing.T) {
	testCases := []string{"a", "/topic", "topic/level", "topic/level/3",
		"topic//test"}
	session := Session{ClientID: ClientID("id")}

	for _, test := range testCases {
		t.Run(test, func(t *testing.T) {
			sub := Subscription{Session: &session, TopicFilter: []byte(test),
				QoS: packet.QoS0}

			trie := newSubscriptionTrie()
			err := trie.insert(sub)
			assert.Nil(t, err)
			require.Len(t, trie.nodes, 1)
			checkTrie(t, &trie, sub, session.ClientID)
		})
	}
}

func BenchmarkSubscriptionTrie_Insert(b *testing.B) {
	session := Session{ClientID: ClientID("id")}
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		trie := newSubscriptionTrie()

		err := addSubscriptions(&trie, &session, "sensor/temp/", 1000)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestSubscriptionTrie_InsertMultipleSubscriptions(t *testing.T) {
	session := Session{ClientID: ClientID("id")}
	subscriptions := []Subscription{
		{Session: &session, TopicFilter: []byte("topic/0"), QoS: packet.QoS0},
		{Session: &session, TopicFilter: []byte("topic/1"), QoS: packet.QoS1},
		{Session: &session, TopicFilter: []byte("topic/2"), QoS: packet.QoS2},
	}
	trie := newSubscriptionTrie()

	err := trie.insert(subscriptions[0])
	require.Nil(t, err)
	err = trie.insert(subscriptions[1])
	require.Nil(t, err)
	err = trie.insert(subscriptions[2])
	require.Nil(t, err)

	checkTrie(t, &trie, subscriptions[0], session.ClientID)
	checkTrie(t, &trie, subscriptions[1], session.ClientID)
	checkTrie(t, &trie, subscriptions[2], session.ClientID)
}

func TestSubscriptionTrie_InsertSubscriptionsSameTopic(t *testing.T) {
	subscriptions := []Subscription{
		{Session: &Session{ClientID: ClientID("0")}, RetainHandling: 0,
			RetainAsPublished: false, NoLocal: false,
			TopicFilter: []byte("topic"), QoS: packet.QoS0},
		{Session: &Session{ClientID: ClientID("1")}, RetainHandling: 1,
			RetainAsPublished: false, NoLocal: true,
			TopicFilter: []byte("topic"), QoS: packet.QoS1},
		{Session: &Session{ClientID: ClientID("2")}, RetainHandling: 2,
			RetainAsPublished: true, NoLocal: false,
			TopicFilter: []byte("topic"), QoS: packet.QoS2},
	}

	trie := newSubscriptionTrie()

	for _, sub := range subscriptions {
		err := trie.insert(sub)
		require.Nil(t, err)
	}
	for _, sub := range subscriptions {
		checkTrie(t, &trie, sub, sub.Session.ClientID)
	}
}

func TestSubscriptionTrie_InsertTopicFilterWithWildcard(t *testing.T) {
	testCases := []string{"+", "#", "a/+", "a/#", "a/b/+", "a/b/#", "/a/+/c/+",
		"/a/b/c/#"}

	for _, test := range testCases {
		t.Run(test, func(t *testing.T) {
			session := Session{ClientID: ClientID("id")}
			sub := Subscription{Session: &session, TopicFilter: []byte(test),
				QoS: packet.QoS0}

			trie := newSubscriptionTrie()
			err := trie.insert(sub)
			assert.Nil(t, err)
			checkTrie(t, &trie, sub, sub.Session.ClientID)
		})
	}
}

func TestSubscriptionTrie_InsertInvalidTopicFilter(t *testing.T) {
	testCases := []string{"", "sensor#", "sensor/room#", "sensor/#/temp",
		"sensor+"}

	for _, test := range testCases {
		t.Run(test, func(t *testing.T) {
			session := Session{ClientID: ClientID("id")}
			sub := Subscription{Session: &session, TopicFilter: []byte(test),
				QoS: packet.QoS0}

			trie := newSubscriptionTrie()
			err := trie.insert(sub)
			assert.NotNil(t, err)
		})
	}
}
