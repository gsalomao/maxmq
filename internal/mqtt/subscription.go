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
	"strings"
	"sync"

	"github.com/gsalomao/maxmq/internal/mqtt/packet"
)

// ErrSubscriptionInvalidWildcard indicates that the topic filter in the
// Subscription has an invalid wildcard.
var ErrSubscriptionInvalidWildcard = errors.New("invalid wildcard")

// ErrSubscriptionNotFound indicates that the subscription was not found.
var ErrSubscriptionNotFound = errors.New("subscription not found")

// Subscription represents a MQTT subscription.
type Subscription struct {
	next *Subscription

	// ClientID is the MQTT Client Identifier which created the subscription.
	ClientID ClientID

	// TopicFilter is the MQTT Topic Filter.
	TopicFilter string

	// ID is the subscription identifier.
	ID int

	// QoS is the Quality of Service level of the subscription.
	QoS packet.QoS

	// RetainHandling is the MQTT Retain Handling option.
	RetainHandling byte

	// RetainAsPublished is the MQTT Retain As Published option.
	RetainAsPublished bool

	// NoLocal is the MQTT No Local option.
	NoLocal bool
}

type subscriptionTree struct {
	root  *subscriptionNode
	mutex sync.RWMutex
}

func newSubscriptionTree() subscriptionTree {
	return subscriptionTree{root: newSubscriptionNode()}
}

func (t *subscriptionTree) insert(sub Subscription) (exists bool, err error) {
	if len(sub.TopicFilter) == 0 {
		return false, errors.New("empty topic filter")
	}

	words := strings.Split(sub.TopicFilter, "/")
	t.mutex.Lock()
	defer t.mutex.Unlock()

	nodes := t.root.children
	var node *subscriptionNode

	for i, word := range words {
		lastLevel := i == len(words)-1

		if err = validateTopicWord(word, lastLevel); err != nil {
			return false, err
		}

		child, ok := nodes[word]
		if !ok {
			child = newSubscriptionNode()
			nodes[word] = child
		}
		node = child
		nodes = child.children
	}

	exists = node.insert(&sub)
	return
}

func (t *subscriptionTree) remove(id ClientID, topic string) error {
	words := strings.Split(topic, "/")
	t.mutex.Lock()
	defer t.mutex.Unlock()

	nodes := t.root.children
	stack := make([]*subscriptionNode, 0, len(words)+1) // +1 (root node)
	stack = append(stack, t.root)

	for i, word := range words {
		node, ok := nodes[word]
		if !ok {
			return ErrSubscriptionNotFound
		}

		if i == len(words)-1 {
			err := node.remove(id)
			if err != nil {
				return err
			}
		}

		stack = append(stack, node)
		nodes = node.children
	}

	for len(stack) > 1 {
		n := stack[len(stack)-1]
		parent := stack[len(stack)-2]
		stack = stack[:len(stack)-1]

		if n.subscription == nil && len(n.children) == 0 {
			delete(parent.children, words[len(words)-1])
			words = words[:len(words)-1]
		}
	}

	return nil
}

func (t *subscriptionTree) findMatches(topic string) []Subscription {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	subscriptions := make([]Subscription, 0)
	words := strings.Split(topic, "/")
	t.root.findMatches(&subscriptions, words)

	return subscriptions
}

type subscriptionNode struct {
	subscription *Subscription
	children     map[string]*subscriptionNode
}

func newSubscriptionNode() *subscriptionNode {
	return &subscriptionNode{children: make(map[string]*subscriptionNode)}
}

func (n *subscriptionNode) insert(sub *Subscription) bool {
	var parent *Subscription
	var exists bool

	subscription := n.subscription

	for subscription != nil {
		if sameSubscription(sub, subscription) {
			break
		}

		parent = subscription
		subscription = subscription.next
	}

	if subscription != nil {
		exists = true
	}

	if parent != nil {
		parent.next = sub
	} else {
		n.subscription = sub
	}

	return exists
}

func (n *subscriptionNode) remove(id ClientID) error {
	var parent *Subscription
	sub := n.subscription

	for sub != nil {
		if id == sub.ClientID {
			break
		}

		parent = sub
		sub = sub.next
	}

	if sub == nil {
		return ErrSubscriptionNotFound
	}

	if parent == nil {
		if sub.next == nil {
			n.subscription = nil
		} else {
			n.subscription = sub.next
		}
	} else {
		parent.next = sub.next
	}

	return nil
}

func (n *subscriptionNode) findMatches(subs *[]Subscription, topic []string) {
	topicLen := len(topic)

	node, ok := n.children[topic[0]]
	if ok {
		if topicLen == 1 {
			node.getAllSubscriptions(subs)
		} else if topicLen > 1 {
			node.findMatches(subs, topic[1:])
		}
	}

	node, ok = n.children["+"]
	if ok {
		if topicLen == 1 {
			node.getAllSubscriptions(subs)
		} else if topicLen > 1 {
			node.findMatches(subs, topic[1:])
		}
	}

	node, ok = n.children["#"]
	if ok {
		node.getAllSubscriptions(subs)
	}
}

func (n *subscriptionNode) getAllSubscriptions(subs *[]Subscription) {
	sub := n.subscription
	for sub != nil {
		*subs = append(*subs, *sub)
		sub = sub.next
	}
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
	if sub1.ClientID == sub2.ClientID &&
		sub1.TopicFilter == sub2.TopicFilter {
		return true
	}

	return false
}