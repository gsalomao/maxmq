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

package mqtt

import (
	"errors"
	"strings"
	"sync"

	"github.com/gsalomao/maxmq/internal/mqtt/packet"
)

var errSubscriptionInvalidWildcard = errors.New("invalid wildcard")
var errSubscriptionNotFound = errors.New("subscription not found")

type subscriptionID int

type subscription struct {
	next              *subscription
	clientID          packet.ClientID
	topicFilter       string
	id                subscriptionID
	qos               packet.QoS
	retainHandling    byte
	retainAsPublished bool
	noLocal           bool
}

type subscriptionTree struct {
	root  *subscriptionNode
	mutex sync.RWMutex
}

func newSubscriptionTree() subscriptionTree {
	return subscriptionTree{root: newSubscriptionNode()}
}

func (t *subscriptionTree) insert(sub subscription) (exists bool, err error) {
	if len(sub.topicFilter) == 0 {
		return false, errors.New("empty topic filter")
	}

	words := strings.Split(sub.topicFilter, "/")
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

func (t *subscriptionTree) remove(id packet.ClientID, topic string) error {
	words := strings.Split(topic, "/")
	t.mutex.Lock()
	defer t.mutex.Unlock()

	nodes := t.root.children
	stack := make([]*subscriptionNode, 0, len(words)+1) // +1 (root node)
	stack = append(stack, t.root)

	for i, word := range words {
		node, ok := nodes[word]
		if !ok {
			return errSubscriptionNotFound
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

func (t *subscriptionTree) findMatches(topic string) []subscription {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	subs := make([]subscription, 0)
	words := strings.Split(topic, "/")
	t.root.findMatches(&subs, words)

	return subs
}

type subscriptionNode struct {
	subscription *subscription
	children     map[string]*subscriptionNode
}

func newSubscriptionNode() *subscriptionNode {
	return &subscriptionNode{
		children: make(map[string]*subscriptionNode),
	}
}

func (n *subscriptionNode) insert(sub *subscription) bool {
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
		if id == (*sub).clientID {
			break
		}
		sub = &(*sub).next
	}

	if *sub == nil {
		return errSubscriptionNotFound
	}

	*sub = (*sub).next
	return nil
}

func (n *subscriptionNode) findMatches(subs *[]subscription, topic []string) {
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

func (n *subscriptionNode) getAllSubscriptions(subs *[]subscription) {
	sub := n.subscription
	for sub != nil {
		*subs = append(*subs, *sub)
		sub = sub.next
	}
}

func validateTopicWord(word string, isLastWord bool) error {
	wl := len(word)

	if (wl > 1 || !isLastWord) && strings.Contains(word, "#") {
		return errSubscriptionInvalidWildcard
	}
	if wl > 1 && strings.Contains(word, "+") {
		return errSubscriptionInvalidWildcard
	}

	return nil
}

func sameSubscription(sub1 *subscription, sub2 *subscription) bool {
	if sub1.clientID == sub2.clientID && sub1.topicFilter == sub2.topicFilter {
		return true
	}

	return false
}
