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

func TestUnsubscribeHandlePacket(t *testing.T) {
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
			mt := newMetrics(true, log)
			ps := newPubSub(nil, ss, mt, log)
			h := newUnsubscribeHandler(ss, ps, log)

			id := packet.ClientID("client-a")
			s := &session{clientID: id, version: tc, subscriptions: make(map[string]*subscription)}
			sub := &subscription{clientID: id, topicFilter: "data"}

			err := ps.subscribe(sub)
			require.Nil(t, err)

			s.subscriptions["data"] = sub
			unsubPkt := &packet.Unsubscribe{PacketID: 1, Version: tc, Topics: []string{"data"}}

			err = ss.saveSession(s)
			require.Nil(t, err)

			var replies []packet.Packet

			replies, err = h.handlePacket(id, unsubPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.UNSUBACK, reply.Type())

			unsubAckPkt := reply.(*packet.UnsubAck)
			assert.Equal(t, unsubPkt.PacketID, unsubAckPkt.PacketID)
			assert.Equal(t, unsubPkt.Version, unsubAckPkt.Version)

			require.Len(t, unsubAckPkt.ReasonCodes, 1)
			assert.Equal(t, packet.ReasonCodeV5Success, unsubAckPkt.ReasonCodes[0])

			assert.Empty(t, s.subscriptions)
		})
	}
}

func TestUnsubscribeHandlePacketMultipleTopics(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			gen := newIDGeneratorMock()
			log := newLogger()
			ss := newSessionStore(gen, log)
			mt := newMetrics(true, log)
			ps := newPubSub(nil, ss, mt, log)
			h := newUnsubscribeHandler(ss, ps, log)

			id := packet.ClientID("client-a")
			s := &session{clientID: id, version: tc, subscriptions: make(map[string]*subscription)}

			subs := []*subscription{
				{clientID: id, topicFilter: "data/0", qos: packet.QoS0},
				{clientID: id, topicFilter: "data/#", qos: packet.QoS1},
				{clientID: id, topicFilter: "data/2", qos: packet.QoS2},
			}

			for _, sub := range subs {
				err := ps.subscribe(sub)
				require.Nil(t, err)

				s.subscriptions[sub.topicFilter] = sub
			}

			topicNames := []string{"data/0", "data/#"}
			unsubPkt := &packet.Unsubscribe{PacketID: 1, Version: tc, Topics: topicNames}

			err := ss.saveSession(s)
			require.Nil(t, err)

			var replies []packet.Packet

			replies, err = h.handlePacket(id, unsubPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.UNSUBACK, reply.Type())

			unsubAckPkt := reply.(*packet.UnsubAck)
			assert.Equal(t, unsubPkt.PacketID, unsubAckPkt.PacketID)
			assert.Equal(t, unsubPkt.Version, unsubAckPkt.Version)

			codes := []packet.ReasonCode{packet.ReasonCodeV5Success, packet.ReasonCodeV5Success}
			assert.Equal(t, codes, unsubAckPkt.ReasonCodes)

			require.NotNil(t, s)
			assert.Len(t, s.subscriptions, 1)
		})
	}
}

func TestUnsubscribeHandlePacketUnsubscribeError(t *testing.T) {
	gen := newIDGeneratorMock()
	log := newLogger()
	ss := newSessionStore(gen, log)
	mt := newMetrics(true, log)
	ps := newPubSub(nil, ss, mt, log)
	h := newUnsubscribeHandler(ss, ps, log)

	id := packet.ClientID("client-a")
	s := &session{
		clientID:      id,
		version:       packet.MQTT311,
		subscriptions: make(map[string]*subscription),
	}
	unsubPkt := &packet.Unsubscribe{
		PacketID: 1,
		Version:  packet.MQTT311,
		Topics:   []string{"data"},
	}

	err := ss.saveSession(s)
	require.Nil(t, err)

	var replies []packet.Packet

	replies, err = h.handlePacket(id, unsubPkt)
	require.Nil(t, err)
	require.Len(t, replies, 1)

	reply := replies[0]
	require.Equal(t, packet.UNSUBACK, reply.Type())

	unsubAckPkt := reply.(*packet.UnsubAck)
	assert.Equal(t, unsubPkt.PacketID, unsubAckPkt.PacketID)
	assert.Equal(t, unsubPkt.Version, unsubAckPkt.Version)

	require.Len(t, unsubAckPkt.ReasonCodes, 1)
	assert.Equal(t, packet.ReasonCodeV5NoSubscriptionExisted, unsubAckPkt.ReasonCodes[0])
}

func TestUnsubscribeHandlePacketReadSessionError(t *testing.T) {
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
			h := newUnsubscribeHandler(ss, ps, log)

			id := packet.ClientID("client-a")
			unsubPkt := &packet.Unsubscribe{PacketID: 1, Version: tc, Topics: []string{"data"}}

			replies, err := h.handlePacket(id, unsubPkt)
			assert.NotNil(t, err)
			assert.Empty(t, replies)
		})
	}
}
