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

func TestDisconnectHandlePacket(t *testing.T) {
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
			h := newDisconnectHandler(ss, nil, log)

			id := packet.ClientID("client-a")
			s := &session{
				clientID:      id,
				version:       tc,
				connected:     true,
				subscriptions: make(map[string]*subscription),
			}

			sub := &subscription{id: 1, topicFilter: "test", clientID: id}
			s.subscriptions["test"] = sub

			err := ss.saveSession(s)
			require.Nil(t, err)

			var replies []packet.Packet
			discPkt := packet.NewDisconnect(tc, packet.ReasonCodeV5Success, nil)

			replies, err = h.handlePacket(id, &discPkt)
			assert.Nil(t, err)
			assert.Empty(t, replies)
			assert.False(t, s.connected)
			assert.NotEmpty(t, s.subscriptions)
		})
	}
}

func TestDisconnectHandlePacketCleanSession(t *testing.T) {
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
			ps := newPubSub(nil, ss, nil, log)
			h := newDisconnectHandler(ss, ps, log)

			id := packet.ClientID("client-a")
			s := &session{
				clientID:      id,
				version:       tc,
				connected:     true,
				cleanSession:  true,
				subscriptions: make(map[string]*subscription),
			}

			sub := &subscription{id: 1, topicFilter: "test", clientID: id}
			s.subscriptions["test"] = sub

			err := ss.saveSession(s)
			require.Nil(t, err)

			var replies []packet.Packet
			discPkt := packet.NewDisconnect(tc, packet.ReasonCodeV5Success, nil)

			replies, err = h.handlePacket(id, &discPkt)
			assert.Nil(t, err)
			require.Empty(t, replies)
			assert.False(t, s.connected)
			assert.Empty(t, s.subscriptions)
		})
	}
}

func TestDisconnectHandlePacketV5ExpiryInterval(t *testing.T) {
	gen := newIDGeneratorMock()
	log := newLogger()
	ss := newSessionStore(gen, log)
	h := newDisconnectHandler(ss, nil, log)

	id := packet.ClientID("client-a")
	s := &session{clientID: id, version: packet.MQTT50, connected: true, expiryInterval: 600}

	err := ss.saveSession(s)
	require.Nil(t, err)

	props := &packet.Properties{}
	props.SessionExpiryInterval = new(uint32)
	*props.SessionExpiryInterval = 300

	var replies []packet.Packet
	discPkt := packet.NewDisconnect(packet.MQTT50, packet.ReasonCodeV5Success, props)

	replies, err = h.handlePacket(id, &discPkt)
	assert.Nil(t, err)
	assert.Empty(t, replies)
	assert.Equal(t, uint32(300), s.expiryInterval)
}

func TestDisconnectHandlePacketV5CleanSessionExpInterval(t *testing.T) {
	gen := newIDGeneratorMock()
	log := newLogger()
	ss := newSessionStore(gen, log)
	h := newDisconnectHandler(ss, nil, log)

	id := packet.ClientID("client-a")
	s := &session{clientID: id, version: packet.MQTT50, connected: true, cleanSession: true, expiryInterval: 600}

	err := ss.saveSession(s)
	require.Nil(t, err)

	var replies []packet.Packet
	discPkt := packet.NewDisconnect(packet.MQTT50, packet.ReasonCodeV5Success, nil)

	replies, err = h.handlePacket(id, &discPkt)
	assert.Nil(t, err)
	assert.Empty(t, replies)
	assert.Equal(t, uint32(600), s.expiryInterval)
}

func TestDisconnectHandlePacketV5InvalidExpiryInterval(t *testing.T) {
	gen := newIDGeneratorMock()
	log := newLogger()
	ss := newSessionStore(gen, log)
	h := newDisconnectHandler(ss, nil, log)

	id := packet.ClientID("client-a")
	s := &session{clientID: id, version: packet.MQTT50, connected: true}

	err := ss.saveSession(s)
	require.Nil(t, err)

	props := &packet.Properties{}
	props.SessionExpiryInterval = new(uint32)
	*props.SessionExpiryInterval = 300

	var replies []packet.Packet
	discPkt := packet.NewDisconnect(packet.MQTT50, packet.ReasonCodeV5Success, props)

	replies, err = h.handlePacket(id, &discPkt)
	require.NotNil(t, err)
	require.Len(t, replies, 1)

	reply := replies[0]
	assert.Equal(t, packet.DISCONNECT, reply.Type())

	discReplyPkt := reply.(*packet.Disconnect)
	assert.Equal(t, packet.MQTT50, discReplyPkt.Version)
	assert.Equal(t, packet.ReasonCodeV5ProtocolError, discReplyPkt.ReasonCode)
}

func TestDisconnectHandlePacketReadSessionError(t *testing.T) {
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
			h := newDisconnectHandler(ss, nil, log)

			id := packet.ClientID("client-a")
			discPkt := packet.NewDisconnect(tc, packet.ReasonCodeV5Success, nil)

			replies, err := h.handlePacket(id, &discPkt)
			assert.NotNil(t, err)
			assert.Empty(t, replies)
		})
	}
}
