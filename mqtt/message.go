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
	"time"

	"github.com/gsalomao/maxmq/mqtt/packet"
)

const (
	messageIDTimestampMask      = 0xFFFFFFFFFF
	messageIDTimestampBit       = 24
	messageIDNodeIDMask         = 0x3FF
	messageIDNodeIDBit          = 14
	messageIDSequenceNumberMask = 0x1FFF
)

func newMessageIDGenerator(nodeID uint16) messageIDGenerator {
	startTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).
		UnixMilli()

	return messageIDGenerator{
		startTime:      startTime,
		elapsedTime:    time.Now().UnixMilli() - startTime,
		nodeID:         nodeID,
		sequenceNumber: messageIDSequenceNumberMask,
	}
}

// Generated messages IDs are composed of:
//  - 40 bits for timestamp with milliseconds of precision (epoch 01/01/2020)
//  - 10 bits for node ID (1024 nodes)
//  - 13 bits for sequence number (8192 IDs)
type messageIDGenerator struct {
	mutex          sync.Mutex
	startTime      int64
	elapsedTime    int64
	nodeID         uint16
	sequenceNumber int
}

func (g *messageIDGenerator) nextID() uint64 {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	currentElapsedTime := g.currentElapsedTime()

	if currentElapsedTime > g.elapsedTime {
		g.elapsedTime = currentElapsedTime
		g.sequenceNumber = 0
	} else {
		g.sequenceNumber = (g.sequenceNumber + 1) & messageIDSequenceNumberMask
	}

	var id uint64

	id = uint64(g.elapsedTime&messageIDTimestampMask) << messageIDTimestampBit
	id |= uint64(g.nodeID&messageIDNodeIDMask) << messageIDNodeIDBit
	id |= uint64(g.sequenceNumber & messageIDSequenceNumberMask)

	return id
}

func (g *messageIDGenerator) currentElapsedTime() int64 {
	return time.Now().UnixMilli() - g.startTime
}

type message struct {
	id     uint64
	packet *packet.Publish
}

type messageQueue struct {
	mutex sync.RWMutex
	list  list.List
}

func (q *messageQueue) enqueue(msg message) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	q.list.PushBack(msg)
}

func (q *messageQueue) dequeue() message {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	elem := q.list.Front()
	q.list.Remove(elem)

	return elem.Value.(message)
}

func (q *messageQueue) len() int {
	q.mutex.RLock()
	defer q.mutex.RUnlock()

	return q.list.Len()
}
