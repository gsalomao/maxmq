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

func TestPubCompHandlePacket(t *testing.T) {
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
			h := newPubCompHandler(ss, log)

			id := packet.ClientID("client-a")
			s := &session{clientID: id, version: tc}

			err := ss.saveSession(s)
			require.Nil(t, err)

			msg := &message{packetID: 1, tries: 1, lastSent: time.Now().UnixMicro()}
			s.inflightMessages.PushBack(msg)

			var replies []packet.Packet
			pubCompPkt := packet.NewPubComp(msg.packetID, tc, packet.ReasonCodeV5Success, nil)

			replies, err = h.handlePacket(id, &pubCompPkt)
			require.Nil(t, err)
			require.Empty(t, replies)
			require.Equal(t, 0, s.inflightMessages.Len())
		})
	}
}

func TestPubCompHandlePacketPacketNotFound(t *testing.T) {
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
			h := newPubCompHandler(ss, log)

			id := packet.ClientID("client-a")
			s := &session{clientID: id, version: tc}

			err := ss.saveSession(s)
			require.Nil(t, err)

			pubCompPkt := packet.NewPubComp(2, tc, packet.ReasonCodeV5Success, nil)
			replies, err := h.handlePacket(id, &pubCompPkt)
			require.NotNil(t, err)
			require.Empty(t, replies)
		})
	}
}

func TestPubCompHandlePacketReadSessionError(t *testing.T) {
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
			h := newPubCompHandler(ss, log)

			id := packet.ClientID("client-a")
			pubCompPkt := packet.NewPubComp(1, tc, packet.ReasonCodeV5Success, nil)

			replies, err := h.handlePacket(id, &pubCompPkt)
			assert.NotNil(t, err)
			assert.Empty(t, replies)
		})
	}
}
