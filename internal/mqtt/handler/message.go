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
	"container/list"
	"sync"

	"github.com/gsalomao/maxmq/internal/mqtt/packet"
)

// MessageID represents the message identifier.
type MessageID uint64

// MessageIDGenerator is responsible for generating MessageID.
type MessageIDGenerator interface {
	// NextID creates a new identifier.
	NextID() uint64
}

// Message is a MQTT message which was published.
type Message struct {
	// ID represents the message identifier.
	ID MessageID

	// Packet is the MQTT PUBLISH packet.
	Packet *packet.Publish

	// LastSent is the timestamp of the last time the packet was sent to client.
	LastSent int64

	// Tries is the number of times that the message was sent to client.
	Tries int

	// PacketID is the MQTT PUBLISH packet identifier.
	PacketID packet.ID
}

// Clone clones the message.
func (m *Message) Clone() *Message {
	return &Message{
		ID:       m.ID,
		PacketID: m.PacketID,
		Packet:   m.Packet.Clone(),
		LastSent: m.LastSent,
		Tries:    m.Tries,
	}
}

// MessageQueue is a queue for Message.
type MessageQueue struct {
	// Unexported fields
	mutex sync.RWMutex
	list  list.List
}

// Enqueue adds the given message in the end of the queue.
func (mq *MessageQueue) Enqueue(msg *Message) {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	mq.list.PushBack(msg)
}

// Dequeue removes the first message from the queue.
func (mq *MessageQueue) Dequeue() *Message {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	elem := mq.list.Front()
	mq.list.Remove(elem)

	return elem.Value.(*Message)
}

// Len returns the length of the queue.
func (mq *MessageQueue) Len() int {
	mq.mutex.RLock()
	defer mq.mutex.RUnlock()

	return mq.list.Len()
}
