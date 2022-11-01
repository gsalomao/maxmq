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
	"testing"

	"github.com/gsalomao/maxmq/pkg/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessageQueue_Enqueue(t *testing.T) {
	mq := messageQueue{}
	require.Zero(t, mq.list.Len())

	msg := message{id: 1}
	mq.enqueue(&msg)
	require.Equal(t, 1, mq.list.Len())
}

func TestMessageQueue_Dequeue(t *testing.T) {
	msg1 := message{id: 1}
	mq := messageQueue{}
	mq.enqueue(&msg1)

	msg2 := mq.dequeue()
	require.Zero(t, mq.list.Len())
	assert.Equal(t, msg1.id, msg2.id)
}

func TestMessageQueue_Len(t *testing.T) {
	mq := messageQueue{}
	assert.Zero(t, mq.len())

	msg := message{id: 1}
	mq.enqueue(&msg)
	assert.Equal(t, 1, mq.len())

	_ = mq.dequeue()
	assert.Zero(t, mq.len())
}

func TestInflightMessagesList_Add(t *testing.T) {
	var inflightMessages inflightMessagesList
	size := inflightMessages.size
	require.Zero(t, size)

	for i := 0; i < 10; i++ {
		msg := &inflightMessage{packetID: packet.ID(i)}
		inflightMessages.add(msg)
		assert.Equal(t, size+i+1, inflightMessages.size)
	}
}

func BenchmarkInflightMessagesList_Add(b *testing.B) {
	var inflightMessages inflightMessagesList
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		msg := &inflightMessage{packetID: packet.ID(i)}
		inflightMessages.add(msg)
	}
}

func TestInflightMessagesList_Remove(t *testing.T) {
	var inflightMessages inflightMessagesList

	for i := 0; i < 10; i++ {
		msg := &inflightMessage{packetID: packet.ID(i)}
		inflightMessages.add(msg)
	}

	size := inflightMessages.size
	for i := 0; i < 10; i++ {
		inflightMessages.remove(packet.ID(i))
		assert.Equal(t, size-i-1, inflightMessages.size)
	}

	for i := 0; i < 10; i++ {
		msg := &inflightMessage{packetID: packet.ID(i)}
		inflightMessages.add(msg)
	}

	size = inflightMessages.size
	for i := size; i > 0; i-- {
		inflightMessages.remove(packet.ID(i - 1))
		assert.Equal(t, size-(size-i)-1, inflightMessages.size)
	}

	assert.Nil(t, inflightMessages.root.next)
	assert.Equal(t, &inflightMessages.root, inflightMessages.tail)
}

func BenchmarkInflightMessagesList_Remove(b *testing.B) {
	var inflightMessages inflightMessagesList
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		msg := &inflightMessage{packetID: packet.ID(i)}
		inflightMessages.add(msg)
	}

	for i := 0; i < b.N; i++ {
		inflightMessages.remove(packet.ID(i))
	}
}

func TestInflightMessagesList_Find(t *testing.T) {
	var inflightMessages inflightMessagesList

	for i := 0; i < 10; i++ {
		msg := &inflightMessage{packetID: packet.ID(i)}
		inflightMessages.add(msg)
	}

	for i := 0; i < 10; i++ {
		msg := inflightMessages.find(packet.ID(i))
		assert.NotNil(t, msg)
		assert.Equal(t, packet.ID(i), msg.packetID)
	}
}

func BenchmarkInflightMessagesList_Find(b *testing.B) {
	var inflightMessages inflightMessagesList
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		msg := &inflightMessage{packetID: packet.ID(i)}
		inflightMessages.add(msg)

		msg = inflightMessages.find(packet.ID(i))
		if msg == nil {
			b.Fatal("message not found")
		}
	}
}

func TestInflightMessagesList_Front(t *testing.T) {
	var inflightMessages inflightMessagesList

	for i := 0; i < 10; i++ {
		msg := &inflightMessage{packetID: packet.ID(i)}
		inflightMessages.add(msg)
	}

	for i := 0; i < 10; i++ {
		msg := inflightMessages.front()
		assert.NotNil(t, msg)
		assert.Equal(t, packet.ID(i), msg.packetID)

		inflightMessages.remove(packet.ID(i))
	}
}

func BenchmarkInflightMessagesList_Front(b *testing.B) {
	var inflightMessages inflightMessagesList
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		msg := &inflightMessage{packetID: packet.ID(i)}
		inflightMessages.add(msg)

		msg = inflightMessages.front()
		if msg == nil {
			b.Fatal("message not found")
		} else if msg.packetID != packet.ID(0) {
			b.Fatal("invalid message")
		}
	}
}

func TestInflightMessagesList_Len(t *testing.T) {
	var inflightMessages inflightMessagesList

	for i := 0; i < 10; i++ {
		msg := &inflightMessage{packetID: packet.ID(i)}
		inflightMessages.add(msg)
		assert.Equal(t, i+1, inflightMessages.len())
	}
}
