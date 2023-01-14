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
	id       messageID
	packet   *packet.Publish
	lastSent int64
	tries    int
	packetID packet.ID
}

func (m *message) clone() *message {
	return &message{id: m.id, packetID: m.packetID, packet: m.packet.Clone(),
		lastSent: m.lastSent, tries: m.tries}
}

type messageQueue struct {
	mutex sync.RWMutex
	list  list.List
}

func (mq *messageQueue) enqueue(msg *message) {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	mq.list.PushBack(msg)
}

func (mq *messageQueue) dequeue() *message {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	elem := mq.list.Front()
	mq.list.Remove(elem)

	return elem.Value.(*message)
}

func (mq *messageQueue) len() int {
	mq.mutex.RLock()
	defer mq.mutex.RUnlock()

	return mq.list.Len()
}
