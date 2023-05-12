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
	"testing"
	"time"

	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPubAckHandlePacket(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			gen := newIDGeneratorMock()
			log := newLogger()
			ss := newSessionStore(gen, log)
			h := newPubAckHandler(ss, log)

			id := packet.ClientID("client-a")
			s := &session{clientID: id, version: tc}

			err := ss.saveSession(s)
			require.Nil(t, err)

			pID := packet.ID(2)
			pubPkt := packet.NewPublish(pID, tc, "t", packet.QoS1, 0, 0, nil, nil)
			msg := &message{id: 1, packetID: pID, packet: &pubPkt, tries: 1, lastSent: time.Now().UnixMicro()}

			s.inflightMessages.PushBack(&message{id: 1, packetID: 1})
			s.inflightMessages.PushBack(msg)

			var replies []packet.Packet
			pubAckPkt := packet.NewPubAck(pID, pubPkt.Version, packet.ReasonCodeV5Success, nil)

			replies, err = h.handlePacket(id, &pubAckPkt)
			require.Nil(t, err)
			assert.Empty(t, replies)
			assert.Equal(t, 1, s.inflightMessages.Len())
		})
	}
}

func TestPubAckHandlePacketUnknownMessage(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			gen := newIDGeneratorMock()
			log := newLogger()
			ss := newSessionStore(gen, log)
			h := newPubAckHandler(ss, log)

			id := packet.ClientID("client-a")
			s := &session{clientID: id, version: tc}

			err := ss.saveSession(s)
			require.Nil(t, err)

			var replies []packet.Packet
			pubAckPkt := packet.NewPubAck(1, tc, packet.ReasonCodeV5Success, nil)

			replies, err = h.handlePacket(id, &pubAckPkt)
			assert.NotNil(t, err)
			assert.Empty(t, replies)
		})
	}
}

func TestPubAckHandlePacketReadSessionError(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			gen := newIDGeneratorMock()
			log := newLogger()
			ss := newSessionStore(gen, log)
			h := newPubAckHandler(ss, log)

			id := packet.ClientID("client-a")
			pubAckPkt := packet.NewPubAck(1, tc, packet.ReasonCodeV5Success, nil)

			replies, err := h.handlePacket(id, &pubAckPkt)
			assert.NotNil(t, err)
			assert.Empty(t, replies)
		})
	}
}
