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
	"math"
	"testing"

	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSessionNextClientID(t *testing.T) {
	session := Session{}

	for i := 0; i < math.MaxUint16; i++ {
		id := session.nextClientID()
		assert.Equal(t, packet.ID(i+1), id)
	}

	id := session.nextClientID()
	assert.Equal(t, packet.ID(1), id)
}

func TestSessionFindInflightMessage(t *testing.T) {
	session := &Session{}
	numOfMessages := 10

	for i := 0; i < numOfMessages; i++ {
		msg := &message{id: messageID(i), packetID: packet.ID(i)}
		session.inflightMessages.PushBack(msg)
	}

	for i := 0; i < numOfMessages; i++ {
		inflightMsg := session.findInflightMessage(packet.ID(i))
		require.NotNil(t, inflightMsg)

		msg := inflightMsg.Value.(*message)
		require.Equal(t, messageID(i), msg.id)
		require.Equal(t, packet.ID(i), msg.packetID)
	}
}

func TestSessionFindInflightMessageNotFound(t *testing.T) {
	session := &Session{}

	for i := 0; i < 10; i++ {
		inflightMsg := session.findInflightMessage(packet.ID(i))
		require.Nil(t, inflightMsg)
	}
}

func TestSessionFindUnAckPubMessage(t *testing.T) {
	session := &Session{}
	numOfMessages := 10
	messages := make([]*message, 0, numOfMessages)

	for i := 0; i < numOfMessages; i++ {
		pkt := packet.NewPublish(packet.ID(i), packet.MQTT311, "topic",
			packet.QoS2, 0, 0, []byte("data"), nil)

		msg := &message{packetID: pkt.PacketID, packet: &pkt}
		session.unAckPubMessages.PushBack(msg)
		messages = append(messages, msg)
	}

	for i := 0; i < numOfMessages; i++ {
		unAckMsg := session.findUnAckPubMessage(packet.ID(i))
		require.NotNil(t, unAckMsg)

		msg := unAckMsg.Value.(*message)
		require.Same(t, messages[i], msg)
	}
}

func TestSessionFindUnAckPubPacketNotFound(t *testing.T) {
	session := &Session{}

	for i := 0; i < 10; i++ {
		pkt := session.findUnAckPubMessage(packet.ID(i))
		require.Nil(t, pkt)
	}
}
