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
	"fmt"
	"testing"

	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscribeHandlePacket(t *testing.T) {
	testCases := []struct {
		version packet.Version
		qos     packet.QoS
	}{
		{packet.MQTT31, packet.QoS0},
		{packet.MQTT311, packet.QoS0},
		{packet.MQTT50, packet.QoS0},
		{packet.MQTT31, packet.QoS1},
		{packet.MQTT311, packet.QoS1},
		{packet.MQTT50, packet.QoS1},
		{packet.MQTT31, packet.QoS2},
		{packet.MQTT311, packet.QoS2},
		{packet.MQTT50, packet.QoS2},
	}

	for _, tc := range testCases {
		name := fmt.Sprintf("%v-%v", tc.version.String(), tc.qos)
		t.Run(name, func(t *testing.T) {
			conf := newDefaultConfig()
			gen := newIDGeneratorMock()
			log := newLogger()
			ss := newSessionStore(gen, log)
			mt := newMetrics(true, log)
			ps := newPubSub(nil, ss, mt, log)
			h := newSubscribeHandler(conf, ss, ps, log)

			id := packet.ClientID("client-a")
			s := &session{
				clientID:      id,
				version:       tc.version,
				subscriptions: make(map[string]*subscription),
			}

			err := ss.saveSession(s)
			require.Nil(t, err)

			subPkt := &packet.Subscribe{
				PacketID: 1,
				Version:  tc.version,
				Topics:   []packet.Topic{{Name: "data", QoS: tc.qos}},
			}
			var replies []packet.Packet

			replies, err = h.handlePacket(id, subPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.SUBACK, reply.Type())

			subAckPkt := reply.(*packet.SubAck)
			assert.Equal(t, subPkt.PacketID, subAckPkt.PacketID)
			assert.Equal(t, tc.version, subAckPkt.Version)

			require.Len(t, subAckPkt.ReasonCodes, 1)
			assert.Equal(t, packet.ReasonCode(tc.qos), subAckPkt.ReasonCodes[0])

			s, err = ss.readSession(id)
			require.Nil(t, err)

			require.Len(t, s.subscriptions, 1)
			assert.Equal(t, subPkt.Topics[0].Name, s.subscriptions["data"].topicFilter)
			assert.Equal(t, subPkt.Topics[0].QoS, s.subscriptions["data"].qos)
			assert.Equal(t, subPkt.Topics[0].RetainHandling, s.subscriptions["data"].retainHandling)
			assert.Equal(t, subPkt.Topics[0].RetainAsPublished, s.subscriptions["data"].retainAsPublished)
			assert.Equal(t, subPkt.Topics[0].NoLocal, s.subscriptions["data"].noLocal)
		})
	}
}

func TestSubscribeHandlePacketMultipleTopics(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newDefaultConfig()
			gen := newIDGeneratorMock()
			log := newLogger()
			ss := newSessionStore(gen, log)
			mt := newMetrics(true, log)
			ps := newPubSub(nil, ss, mt, log)
			h := newSubscribeHandler(conf, ss, ps, log)

			id := packet.ClientID("client-a")
			s := &session{clientID: id, version: tc, subscriptions: make(map[string]*subscription)}

			err := ss.saveSession(s)
			require.Nil(t, err)

			subs := []*subscription{
				{clientID: id, topicFilter: "data/temp/0", qos: packet.QoS0},
				{clientID: id, topicFilter: "data/temp/1", qos: packet.QoS1},
				{clientID: id, topicFilter: "data/temp/2", qos: packet.QoS2},
				{clientID: id, topicFilter: "", qos: packet.QoS0},
			}
			subPkt := &packet.Subscribe{
				PacketID: 5,
				Version:  tc,
				Topics: []packet.Topic{
					{Name: subs[0].topicFilter, QoS: subs[0].qos},
					{Name: subs[1].topicFilter, QoS: subs[1].qos},
					{Name: subs[2].topicFilter, QoS: subs[2].qos},
					{Name: subs[3].topicFilter, QoS: subs[3].qos},
				},
			}
			var replies []packet.Packet

			replies, err = h.handlePacket(id, subPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.SUBACK, reply.Type())

			subAckPkt := reply.(*packet.SubAck)
			assert.Equal(t, subPkt.PacketID, subAckPkt.PacketID)
			assert.Equal(t, subPkt.Version, subAckPkt.Version)

			require.Len(t, subAckPkt.ReasonCodes, 4)
			assert.Equal(t, packet.ReasonCode(packet.QoS0), subAckPkt.ReasonCodes[0])
			assert.Equal(t, packet.ReasonCode(packet.QoS1), subAckPkt.ReasonCodes[1])
			assert.Equal(t, packet.ReasonCode(packet.QoS2), subAckPkt.ReasonCodes[2])
			assert.Equal(t, packet.ReasonCodeV3Failure, subAckPkt.ReasonCodes[3])

			s, err = ss.readSession(id)
			require.Nil(t, err)

			require.Len(t, s.subscriptions, 3)
			assert.Equal(t, subs[0], s.subscriptions[subPkt.Topics[0].Name])
			assert.Equal(t, subs[1], s.subscriptions[subPkt.Topics[1].Name])
			assert.Equal(t, subs[2], s.subscriptions[subPkt.Topics[2].Name])
		})
	}
}

func TestSubscribeHandlePacketError(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newDefaultConfig()
			gen := newIDGeneratorMock()
			log := newLogger()
			ss := newSessionStore(gen, log)
			ps := newPubSub(nil, ss, nil, log)
			h := newSubscribeHandler(conf, ss, ps, log)

			id := packet.ClientID("client-a")
			s := &session{clientID: id, version: tc, subscriptions: make(map[string]*subscription)}

			err := ss.saveSession(s)
			require.Nil(t, err)

			subPkt := &packet.Subscribe{
				PacketID: 1,
				Version:  tc,
				Topics:   []packet.Topic{{Name: "", QoS: packet.QoS1}},
			}
			var replies []packet.Packet

			replies, err = h.handlePacket(id, subPkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.SUBACK, reply.Type())

			subAckPkt := reply.(*packet.SubAck)
			assert.Equal(t, subPkt.PacketID, subAckPkt.PacketID)
			assert.Equal(t, tc, subAckPkt.Version)

			require.Len(t, subAckPkt.ReasonCodes, 1)
			assert.Equal(t, packet.ReasonCodeV3Failure, subAckPkt.ReasonCodes[0])

			s, err = ss.readSession(id)
			require.Nil(t, err)
			assert.Empty(t, s.subscriptions)
		})
	}
}

func TestSubscribeHandlePacketReadSessionError(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newDefaultConfig()
			gen := newIDGeneratorMock()
			log := newLogger()
			ss := newSessionStore(gen, log)
			h := newSubscribeHandler(conf, ss, nil, log)

			id := packet.ClientID("client-a")
			subPkt := &packet.Subscribe{
				PacketID: 1,
				Version:  tc,
				Topics:   []packet.Topic{{Name: "data", QoS: packet.QoS1}},
			}

			replies, err := h.handlePacket(id, subPkt)
			assert.NotNil(t, err)
			assert.Empty(t, replies)
		})
	}
}

func TestSubscribeHandlePacketV5WithSubID(t *testing.T) {
	conf := newDefaultConfig()
	conf.SubscriptionIDAvailable = true

	gen := newIDGeneratorMock()
	log := newLogger()
	ss := newSessionStore(gen, log)
	mt := newMetrics(true, log)
	ps := newPubSub(nil, ss, mt, log)
	h := newSubscribeHandler(conf, ss, ps, log)

	id := packet.ClientID("client-a")
	s := &session{
		clientID:      id,
		version:       packet.MQTT50,
		subscriptions: make(map[string]*subscription),
	}

	err := ss.saveSession(s)
	require.Nil(t, err)

	props := &packet.Properties{}
	props.SubscriptionIdentifier = new(int)
	*props.SubscriptionIdentifier = 10

	subPkt := &packet.Subscribe{
		PacketID:   2,
		Version:    packet.MQTT50,
		Properties: props,
		Topics:     []packet.Topic{{Name: "data"}},
	}
	var replies []packet.Packet

	replies, err = h.handlePacket(id, subPkt)
	assert.Nil(t, err)
	require.Len(t, replies, 1)

	reply := replies[0]
	require.Equal(t, packet.SUBACK, reply.Type())
}

func TestSubscribeHandlePacketV5WithSubIDError(t *testing.T) {
	conf := newDefaultConfig()
	conf.SubscriptionIDAvailable = false

	gen := newIDGeneratorMock()
	log := newLogger()
	ss := newSessionStore(gen, log)
	ps := newPubSub(nil, ss, nil, log)
	h := newSubscribeHandler(conf, ss, ps, log)

	id := packet.ClientID("client-a")
	s := &session{
		clientID:      id,
		version:       packet.MQTT50,
		subscriptions: make(map[string]*subscription),
	}

	err := ss.saveSession(s)
	require.Nil(t, err)

	props := &packet.Properties{}
	props.SubscriptionIdentifier = new(int)
	*props.SubscriptionIdentifier = 5

	subPkt := &packet.Subscribe{
		PacketID:   2,
		Version:    packet.MQTT50,
		Properties: props,
		Topics:     []packet.Topic{{Name: "topic"}},
	}
	var replies []packet.Packet

	replies, err = h.handlePacket(id, subPkt)
	require.NotNil(t, err)
	require.Len(t, replies, 1)

	reply := replies[0]
	require.Equal(t, packet.DISCONNECT, reply.Type())

	discPkt := reply.(*packet.Disconnect)
	assert.Equal(t, packet.MQTT50, discPkt.Version)
	assert.Equal(t, packet.ReasonCodeV5SubscriptionIDNotSupported, discPkt.ReasonCode)
}
