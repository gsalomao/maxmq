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
	"errors"
	"strings"
	"sync"

	"github.com/gsalomao/maxmq/mqtt/packet"
)

// ErrSubscriptionInvalidWildcard indicates that the topic filter in the
// Subscription has an invalid wildcard.
var ErrSubscriptionInvalidWildcard = errors.New("invalid wildcard")

// Subscription represents a MQTT subscription.
type Subscription struct {
	// Session is the session which subscribed.
	Session *Session

	// TopicFilter is the MQTT Topic Filter.
	TopicFilter string

	// QoS is the Quality of Service level of the subscription.
	QoS packet.QoS

	// RetainHandling is the MQTT Retain Handling option.
	RetainHandling byte

	// RetainAsPublished is the MQTT Retain As Published option.
	RetainAsPublished bool

	// NoLocal is the MQTT No Local option.
	NoLocal bool
}

type subscriptionNode struct {
	subscription *Subscription
	next         *subscriptionNode
	children     map[string]*subscriptionNode
}

func newSubscriptionNode() *subscriptionNode {
	return &subscriptionNode{children: make(map[string]*subscriptionNode)}
}

type subscriptionTrie struct {
	nodes map[string]*subscriptionNode
	mutex sync.RWMutex
}

func newSubscriptionTrie() subscriptionTrie {
	return subscriptionTrie{nodes: make(map[string]*subscriptionNode)}
}

func (t *subscriptionTrie) insert(sub Subscription) error {
	if len(sub.TopicFilter) == 0 {
		return errors.New("empty topic filter")
	}

	words := strings.Split(sub.TopicFilter, "/")
	nodes := t.nodes
	var node *subscriptionNode

	t.mutex.Lock()
	defer t.mutex.Unlock()

	for i, word := range words {
		lastLevel := i == len(words)-1

		if err := validateTopicWord(word, lastLevel); err != nil {
			return err
		}

		child, ok := nodes[word]
		if !ok {
			child = newSubscriptionNode()
			nodes[word] = child
		}
		node = child
		nodes = child.children
	}

	for node.subscription != nil {
		if sameSubscription(node.subscription, &sub) {
			break
		}

		if node.next == nil {
			node.next = newSubscriptionNode()
		}
		node = node.next
	}

	node.subscription = &sub
	return nil
}

func validateTopicWord(word string, isLastWord bool) error {
	wl := len(word)

	if (wl > 1 || !isLastWord) && strings.Contains(word, "#") {
		return ErrSubscriptionInvalidWildcard
	}
	if wl > 1 && strings.Contains(word, "+") {
		return ErrSubscriptionInvalidWildcard
	}

	return nil
}

func sameSubscription(sub1 *Subscription, sub2 *Subscription) bool {
	if bytes.Equal(sub1.Session.ClientID, sub2.Session.ClientID) &&
		sub1.TopicFilter == sub2.TopicFilter {
		return true
	}

	return false
}
