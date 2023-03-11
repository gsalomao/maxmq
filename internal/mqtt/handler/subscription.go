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
	"errors"
	"strings"
	"sync"

	"github.com/gsalomao/maxmq/internal/mqtt/packet"
)

// ErrSubscriptionInvalidWildcard indicates that the Subscription has invalid wildcard in its topic
// filter.
var ErrSubscriptionInvalidWildcard = errors.New("invalid wildcard")

// ErrSubscriptionNotFound indicates that the Subscription has not been found.
var ErrSubscriptionNotFound = errors.New("subscription not found")

// SubscriptionID represents the Subscription identifier.
type SubscriptionID int

// SubscriptionManager is responsible for managing subscriptions.
type SubscriptionManager interface {
	// Subscribe adds the given Subscription.
	Subscribe(s *Subscription) error

	// Unsubscribe removes the Subscription for the given client identifier and topic.
	Unsubscribe(id packet.ClientID, topic string) error

	// Publish publishes the given message to all subscriptions.
	Publish(msg *Message) error
}

// Subscription represents a subscription of given client to a given topic.
type Subscription struct {
	// ClientID represents the identifier of the client which subscribed.
	ClientID packet.ClientID

	// TopicFilter represents the topic which the client subscribed.
	TopicFilter string

	// ID represents the subscription identifier
	ID int

	// QoS represents the quality-of-service level of the subscription.
	QoS packet.QoS

	// RetainHandling indicates whether the retained message are sent when the subscription is
	// established or not.
	RetainHandling byte

	// RetainAsPublished indicates whether the RETAIN flag is kept when messages are forwarded using
	// this subscription or not.
	RetainAsPublished bool

	// NoLocal indicates whether the messages must not be forwarded to a connection with a client ID
	// equal to the client ID of the publishing connection or not.
	NoLocal bool

	// Unexported fields
	next *Subscription
}

// SubscriptionTree stores subscriptions using a tree.
type SubscriptionTree struct {
	root  *subscriptionNode
	mutex sync.RWMutex
}

// NewSubscriptionTree creates a new SubscriptionTree.
func NewSubscriptionTree() SubscriptionTree {
	return SubscriptionTree{
		root: newSubscriptionNode(),
	}
}

// Insert inserts the given subscription into the SubscriptionTree.
func (t *SubscriptionTree) Insert(sub Subscription) (exists bool, err error) {
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

// Remove removes the Subscription for a given client identifier and topic.
func (t *SubscriptionTree) Remove(id packet.ClientID, topic string) error {
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

// FindMatches finds all subscriptions which match with the given topic.
func (t *SubscriptionTree) FindMatches(topic string) []Subscription {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	subs := make([]Subscription, 0)
	words := strings.Split(topic, "/")
	t.root.findMatches(&subs, words)

	return subs
}

type subscriptionNode struct {
	subscription *Subscription
	children     map[string]*subscriptionNode
}

func newSubscriptionNode() *subscriptionNode {
	return &subscriptionNode{
		children: make(map[string]*subscriptionNode),
	}
}

func (n *subscriptionNode) insert(sub *Subscription) bool {
	var exists bool
	sub2 := &n.subscription

	for *sub2 != nil {
		if sameSubscription(*sub2, sub) {
			exists = true
			sub.next = (*sub2).next
			break
		}

		sub2 = &(*sub2).next
	}

	*sub2 = sub
	return exists
}

func (n *subscriptionNode) remove(id packet.ClientID) error {
	sub := &n.subscription

	for *sub != nil {
		if id == (*sub).ClientID {
			break
		}
		sub = &(*sub).next
	}

	if *sub == nil {
		return ErrSubscriptionNotFound
	}

	*sub = (*sub).next
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
	if sub1.ClientID == sub2.ClientID && sub1.TopicFilter == sub2.TopicFilter {
		return true
	}

	return false
}
