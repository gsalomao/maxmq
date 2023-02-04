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
	s := session{}

	for i := 0; i < math.MaxUint16; i++ {
		id := s.nextClientID()
		assert.Equal(t, packet.ID(i+1), id)
	}

	id := s.nextClientID()
	assert.Equal(t, packet.ID(1), id)
}

func TestSessionFindInflightMessage(t *testing.T) {
	s := &session{}
	numOfMessages := 10

	for i := 0; i < numOfMessages; i++ {
		msg := &message{id: messageID(i), packetID: packet.ID(i)}
		s.inflightMessages.PushBack(msg)
	}

	for i := 0; i < numOfMessages; i++ {
		inflightMsg := s.findInflightMessage(packet.ID(i))
		require.NotNil(t, inflightMsg)

		msg := inflightMsg.Value.(*message)
		require.Equal(t, messageID(i), msg.id)
		require.Equal(t, packet.ID(i), msg.packetID)
	}
}

func TestSessionFindInflightMessageNotFound(t *testing.T) {
	s := &session{}

	for i := 0; i < 10; i++ {
		inflightMsg := s.findInflightMessage(packet.ID(i))
		require.Nil(t, inflightMsg)
	}
}
