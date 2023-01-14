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
	"time"

	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessageClone(t *testing.T) {
	pkt := packet.NewPublish(5, packet.MQTT311, "topic", packet.QoS1,
		0, 0, []byte("data"), nil)
	msg1 := &message{id: 100, packetID: pkt.PacketID, packet: &pkt,
		lastSent: time.Now().Unix(), tries: 3}

	msg2 := msg1.clone()
	require.NotNil(t, msg2)
	assert.NotSame(t, msg1, msg2)
	assert.Equal(t, msg1, msg2)
	assert.NotSame(t, msg1.packet, msg2.packet)
	assert.Equal(t, msg1.packet, msg2.packet)
}

func TestMessageQueueEnqueueMessage(t *testing.T) {
	mq := messageQueue{}
	require.Zero(t, mq.list.Len())

	msg := message{id: 1}
	mq.enqueue(&msg)
	require.Equal(t, 1, mq.list.Len())
}

func TestMessageQueueDequeueMessage(t *testing.T) {
	msg1 := message{id: 1}
	mq := messageQueue{}
	mq.enqueue(&msg1)

	msg2 := mq.dequeue()
	require.Zero(t, mq.list.Len())
	assert.Equal(t, msg1.id, msg2.id)
}

func TestMessageQueueGetQueueLen(t *testing.T) {
	mq := messageQueue{}
	assert.Zero(t, mq.len())

	msg := message{id: 1}
	mq.enqueue(&msg)
	assert.Equal(t, 1, mq.len())

	_ = mq.dequeue()
	assert.Zero(t, mq.len())
}
