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
	"testing"
	"time"

	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type messageIDGenMock struct {
	mock.Mock
}

func (g *messageIDGenMock) NextID() uint64 {
	args := g.Called()
	return args.Get(0).(uint64)
}

func TestMessageClone(t *testing.T) {
	pkt := packet.NewPublish(
		5, /*id*/
		packet.MQTT311,
		"topic", /*topic*/
		packet.QoS1,
		0,              /*dup*/
		0,              /*retain*/
		[]byte("data"), /*payload*/
		nil,            /*props*/
	)
	msg1 := &Message{
		ID:       100,
		PacketID: pkt.PacketID, Packet: &pkt, LastSent: time.Now().Unix(),
		Tries: 3,
	}

	msg2 := msg1.Clone()
	require.NotNil(t, msg2)
	assert.NotSame(t, msg1, msg2)
	assert.Equal(t, msg1, msg2)
	assert.NotSame(t, msg1.Packet, msg2.Packet)
	assert.Equal(t, msg1.Packet, msg2.Packet)
}

func TestMessageQueueEnqueueMessage(t *testing.T) {
	mq := MessageQueue{}
	require.Zero(t, mq.list.Len())

	msg := Message{ID: 1}
	mq.Enqueue(&msg)
	require.Equal(t, 1, mq.list.Len())
}

func TestMessageQueueDequeueMessage(t *testing.T) {
	msg1 := Message{ID: 1}
	mq := MessageQueue{}
	mq.Enqueue(&msg1)

	msg2 := mq.Dequeue()
	require.Zero(t, mq.list.Len())
	assert.Equal(t, msg1.ID, msg2.ID)
}

func TestMessageQueueGetQueueLen(t *testing.T) {
	mq := MessageQueue{}
	assert.Zero(t, mq.Len())

	msg := Message{ID: 1}
	mq.Enqueue(&msg)
	assert.Equal(t, 1, mq.Len())

	_ = mq.Dequeue()
	assert.Zero(t, mq.Len())
}
