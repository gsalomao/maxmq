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
	"container/list"
	"sync"

	"github.com/gsalomao/maxmq/internal/mqtt/packet"
)

type messageID uint64

type message struct {
	id     messageID
	packet *packet.Publish
}

type messageQueue struct {
	mutex sync.RWMutex
	list  list.List
}

func (q *messageQueue) enqueue(msg *message) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	q.list.PushBack(msg)
}

func (q *messageQueue) dequeue() *message {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	elem := q.list.Front()
	q.list.Remove(elem)

	return elem.Value.(*message)
}

func (q *messageQueue) len() int {
	q.mutex.RLock()
	defer q.mutex.RUnlock()

	return q.list.Len()
}

type inflightMessage struct {
	next      *inflightMessage
	packet    *packet.Publish
	lastSent  int64
	messageID messageID
	tries     int
	packetID  packet.ID
}

type inflightMessagesList struct {
	root  inflightMessage
	tail  *inflightMessage
	mutex sync.RWMutex
	size  int
}

func (l *inflightMessagesList) add(msg *inflightMessage) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	tail := l.tail
	if tail == nil {
		tail = &l.root
	}
	tail.next = msg
	l.tail = msg
	l.size++
}

func (l *inflightMessagesList) remove(id packet.ID) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	elem := &l.root
	for elem.next != nil {
		if elem.next.packetID == id {
			if elem.next == l.tail {
				l.tail = elem
			}

			elem.next = elem.next.next
			l.size--
			break
		}
		elem = elem.next
	}
}

func (l *inflightMessagesList) find(id packet.ID) *inflightMessage {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	elem := &l.root
	for elem.next != nil {
		if elem.next.packetID == id {
			return elem.next
		}
		elem = elem.next
	}

	return nil
}

func (l *inflightMessagesList) front() *inflightMessage {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	return l.root.next
}

func (l *inflightMessagesList) len() int {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	return l.size
}
