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

	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPingReqHandlePacket(t *testing.T) {
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
			h := newPingReqHandler(ss, log)

			id := packet.ClientID('a')
			s := &session{clientID: id}

			err := ss.saveSession(s)
			require.Nil(t, err)

			var replies []packet.Packet
			pingReqPkt := &packet.PingReq{}

			replies, err = h.handlePacket(id, pingReqPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			assert.Equal(t, packet.PINGRESP, reply.Type())
		})
	}
}

func TestPingReqHandlePacketReadSessionError(t *testing.T) {
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
			h := newPingReqHandler(ss, log)

			id := packet.ClientID('a')
			pingReqPkt := &packet.PingReq{}

			replies, err := h.handlePacket(id, pingReqPkt)
			assert.NotNil(t, err)
			assert.Empty(t, replies)
		})
	}
}