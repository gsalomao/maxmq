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
	"time"

	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectHandlePacketNewSession(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newDefaultConfig()
			gen := newIDGeneratorMock(10)
			log := newLogger()
			ss := newSessionStore(gen, log)
			h := newConnectHandler(conf, ss, log)

			id := packet.ClientID("client-a")
			s := &session{clientID: id}
			connPkt := &packet.Connect{ClientID: []byte(id), Version: tc}

			replies, err := h.handlePacket("", connPkt)
			assert.Nil(t, err)

			s, err = ss.readSession(s.clientID)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.False(t, connAckPkt.SessionPresent)
			assert.Equal(t, s.clientID, connAckPkt.ClientID)
			assert.Equal(t, s.keepAlive, connAckPkt.KeepAlive)
			assert.Equal(t, tc, connAckPkt.Version)

			assert.Equal(t, tc, s.version)
			assert.True(t, s.connected)
			assert.NotZero(t, s.connectedAt)
			assert.Equal(t, int(connPkt.KeepAlive), s.keepAlive)
		})
	}
}

func TestConnectHandlePacketExistingSession(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newDefaultConfig()
			gen := newIDGeneratorMock(10)
			log := newLogger()
			ss := newSessionStore(gen, log)
			h := newConnectHandler(conf, ss, log)

			id := packet.ClientID("client-a")
			connectedAt := time.Now().Add(-1 * time.Minute).Unix()
			s := &session{
				clientID:       id,
				sessionID:      1,
				version:        tc,
				connectedAt:    connectedAt,
				expiryInterval: conf.MaxSessionExpiryInterval,
				subscriptions:  make(map[string]*subscription),
			}
			connPkt := &packet.Connect{ClientID: []byte(id), Version: tc}

			replies, err := h.handlePacket("", connPkt)
			assert.Nil(t, err)

			s, err = ss.readSession(s.clientID)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.False(t, connAckPkt.SessionPresent)

			assert.Equal(t, tc, s.version)
			assert.True(t, s.connected)
			assert.NotZero(t, s.connectedAt)
			assert.NotEqual(t, connectedAt, s.connectedAt)
		})
	}
}

func TestConnectHandlePacketExistingWithCleanSession(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newDefaultConfig()
			gen := newIDGeneratorMock(10)
			log := newLogger()
			ss := newSessionStore(gen, log)
			h := newConnectHandler(conf, ss, log)

			id := packet.ClientID("client-a")
			s := &session{
				clientID:       id,
				sessionID:      1,
				version:        tc,
				connectedAt:    time.Now().Add(-1 * time.Minute).Unix(),
				expiryInterval: conf.MaxSessionExpiryInterval,
				subscriptions:  make(map[string]*subscription),
			}
			connPkt := &packet.Connect{ClientID: []byte(id), Version: tc, CleanSession: true}

			replies, err := h.handlePacket("", connPkt)
			assert.Nil(t, err)

			s, err = ss.readSession(s.clientID)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.False(t, connAckPkt.SessionPresent)

			assert.Equal(t, tc, s.version)
			assert.True(t, s.connected)
			assert.NotZero(t, s.connectedAt)
		})
	}
}

func TestConnectHandlePacketWithInflightMessages(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newDefaultConfig()

			gen := newIDGeneratorMock(10)
			log := newLogger()
			ss := newSessionStore(gen, log)
			h := newConnectHandler(conf, ss, log)

			s := &session{clientID: "client-a"}
			inflightMsgList := make([]*message, 0)
			for i := 0; i < 10; i++ {
				qos := packet.QoS(i%2 + 1)

				topic := fmt.Sprintf("data/%v", i)
				pID := packet.ID(i + 1)
				newPub := packet.NewPublish(pID, tc, topic, qos, 0, 0, nil, nil)
				msg := message{id: messageID(newPub.PacketID), packetID: newPub.PacketID, packet: &newPub}

				s.inflightMessages.PushBack(&msg)
				inflightMsgList = append(inflightMsgList, &msg)
			}

			err := ss.saveSession(s)
			require.Nil(t, err)

			var replies []packet.Packet
			connPkt := &packet.Connect{ClientID: []byte(s.clientID), Version: tc}

			replies, err = h.handlePacket("", connPkt)
			require.Nil(t, err)
			require.Len(t, replies, 11)

			reply := replies[0]
			assert.Equal(t, packet.CONNACK, reply.Type())

			for i := 0; i < 10; i++ {
				reply = replies[i+1]
				require.Equal(t, packet.PUBLISH, reply.Type())

				pubPkt := reply.(*packet.Publish)
				assert.Equal(t, inflightMsgList[i].packet, pubPkt)
				assert.Equal(t, 1, inflightMsgList[i].tries)
				assert.NotZero(t, inflightMsgList[i].lastSent)

				qos := packet.QoS(i%2 + 1)
				assert.Equal(t, qos, pubPkt.QoS)
			}
		})
	}
}

func TestConnectHandlePacketWithInflightMessagesNoPacket(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newDefaultConfig()

			gen := newIDGeneratorMock(10)
			log := newLogger()
			ss := newSessionStore(gen, log)
			h := newConnectHandler(conf, ss, log)

			s := &session{clientID: "client-a"}
			for i := 0; i < 10; i++ {
				msg := message{id: messageID(i + 1), packetID: packet.ID(i + 1)}
				s.inflightMessages.PushBack(&msg)
			}

			err := ss.saveSession(s)
			require.Nil(t, err)

			var replies []packet.Packet
			connPkt := &packet.Connect{ClientID: []byte(s.clientID), Version: tc}

			replies, err = h.handlePacket("", connPkt)
			assert.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			assert.Equal(t, packet.CONNACK, reply.Type())
		})
	}
}

func TestConnectHandlePacketClientIDTooBig(t *testing.T) {
	testCases := []struct {
		version  packet.Version
		id       []byte
		maxIDLen int
		code     packet.ReasonCode
	}{
		{version: packet.MQTT31, id: []byte("012345678901234567890123"), maxIDLen: 65535,
			code: packet.ReasonCodeV3IdentifierRejected},
		{version: packet.MQTT311, id: []byte("0123456789012345678901234567890"), maxIDLen: 30,
			code: packet.ReasonCodeV3IdentifierRejected},
		{version: packet.MQTT50, id: []byte("0123456789012345678901234567890"), maxIDLen: 30,
			code: packet.ReasonCodeV5InvalidClientID},
	}

	for _, tc := range testCases {
		t.Run(tc.version.String(), func(t *testing.T) {
			conf := newDefaultConfig()
			conf.MaxClientIDLen = tc.maxIDLen

			gen := newIDGeneratorMock(10)
			log := newLogger()
			ss := newSessionStore(gen, log)
			h := newConnectHandler(conf, ss, log)

			connPkt := &packet.Connect{ClientID: tc.id, Version: tc.version}
			replies, err := h.handlePacket("", connPkt)
			assert.NotNil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, tc.version, connAckPkt.Version)
			assert.Equal(t, tc.code, connAckPkt.ReasonCode)
		})
	}
}

func TestConnectHandlePacketAllowEmptyClientID(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newDefaultConfig()
			conf.AllowEmptyClientID = true

			gen := newIDGeneratorMock(10)
			log := newLogger()
			ss := newSessionStore(gen, log)
			h := newConnectHandler(conf, ss, log)

			connPkt := &packet.Connect{Version: tc}

			replies, err := h.handlePacket("", connPkt)
			assert.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)

			var s *session
			s, err = ss.readSession(connAckPkt.ClientID)
			require.Nil(t, err)

			assert.True(t, s.connected)
			assert.Greater(t, len(s.clientID), 0)
		})
	}
}

func TestConnectHandlePacketDenyEmptyClientID(t *testing.T) {
	testCases := []struct {
		version packet.Version
		code    packet.ReasonCode
	}{
		{packet.MQTT31, packet.ReasonCodeV3IdentifierRejected},
		{packet.MQTT311, packet.ReasonCodeV3IdentifierRejected},
		{packet.MQTT50, packet.ReasonCodeV5InvalidClientID},
	}

	for _, tc := range testCases {
		t.Run(tc.version.String(), func(t *testing.T) {
			conf := newDefaultConfig()
			conf.AllowEmptyClientID = false

			gen := newIDGeneratorMock(10)
			log := newLogger()
			ss := newSessionStore(gen, log)
			h := newConnectHandler(conf, ss, log)

			connPkt := &packet.Connect{Version: tc.version}
			replies, err := h.handlePacket("", connPkt)
			assert.NotNil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, tc.code, connAckPkt.ReasonCode)
		})
	}
}

func TestConnectHandlePacketV5AssignClientID(t *testing.T) {
	conf := newDefaultConfig()
	conf.AllowEmptyClientID = true

	gen := newIDGeneratorMock(10)
	log := newLogger()
	ss := newSessionStore(gen, log)
	h := newConnectHandler(conf, ss, log)

	connPkt := &packet.Connect{Version: packet.MQTT50}

	replies, err := h.handlePacket("", connPkt)
	assert.Nil(t, err)
	require.Len(t, replies, 1)

	reply := replies[0]
	require.Equal(t, packet.CONNACK, reply.Type())

	connAckPkt := reply.(*packet.ConnAck)
	assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
	assert.NotNil(t, connAckPkt.Properties)
	assert.NotNil(t, connAckPkt.Properties.AssignedClientID)
	assert.Len(t, connAckPkt.Properties.AssignedClientID, 20)

	var s *session
	s, err = ss.readSession(connAckPkt.ClientID)
	require.Nil(t, err)

	assert.True(t, s.connected)
	assignedID := packet.ClientID(connAckPkt.Properties.AssignedClientID)
	assert.Equal(t, assignedID, s.clientID)
}

func TestConnectHandlePacketV5AssignClientIDWithPrefix(t *testing.T) {
	conf := newDefaultConfig()
	conf.AllowEmptyClientID = true
	conf.ClientIDPrefix = []byte("MAXMQ-")

	gen := newIDGeneratorMock(10)
	log := newLogger()
	ss := newSessionStore(gen, log)
	h := newConnectHandler(conf, ss, log)

	connPkt := &packet.Connect{Version: packet.MQTT50}

	replies, err := h.handlePacket("", connPkt)
	assert.Nil(t, err)
	require.Len(t, replies, 1)

	reply := replies[0]
	require.Equal(t, packet.CONNACK, reply.Type())

	connAckPkt := replies[0].(*packet.ConnAck)
	assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
	require.NotNil(t, connAckPkt.Properties)
	require.NotNil(t, connAckPkt.Properties.AssignedClientID)

	assignedID := connAckPkt.Properties.AssignedClientID
	assert.Len(t, connAckPkt.Properties.AssignedClientID, 26)
	assert.Equal(t, conf.ClientIDPrefix, assignedID[:len(conf.ClientIDPrefix)])

	var s *session
	s, err = ss.readSession(connAckPkt.ClientID)
	require.Nil(t, err)

	assert.True(t, s.connected)
	assert.Equal(t, packet.ClientID(assignedID), s.clientID)
}

func TestConnectHandlePacketV5MaxSessionExpiryInterval(t *testing.T) {
	testCases := []struct {
		interval    uint32
		maxInterval uint32
	}{
		{0, 100},
		{100, 100},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.interval), func(t *testing.T) {
			conf := newDefaultConfig()
			conf.MaxSessionExpiryInterval = tc.maxInterval

			gen := newIDGeneratorMock(10)
			log := newLogger()
			ss := newSessionStore(gen, log)
			h := newConnectHandler(conf, ss, log)

			s := &session{clientID: "client-a"}
			props := &packet.Properties{SessionExpiryInterval: &tc.interval}
			connPkt := &packet.Connect{ClientID: []byte(s.clientID), Version: packet.MQTT50, Properties: props}

			replies, err := h.handlePacket("", connPkt)
			assert.Nil(t, err)
			require.Len(t, replies, 1)

			s, err = ss.readSession(s.clientID)
			require.Nil(t, err)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.Nil(t, connAckPkt.Properties)

			assert.True(t, s.connected)
			assert.Equal(t, tc.interval, s.expiryInterval)
		})
	}
}

func TestConnectHandlePacketV5AboveMaxSessionExpInterval(t *testing.T) {
	testCases := []struct {
		interval    uint32
		maxInterval uint32
		resp        uint32
	}{
		{101, 100, 100},
		{2000, 1000, 1000},
		{100000, 80000, 80000},
		{50000000, 32000000, 32000000},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.interval), func(t *testing.T) {
			conf := newDefaultConfig()
			conf.MaxSessionExpiryInterval = tc.maxInterval

			gen := newIDGeneratorMock(10)
			log := newLogger()
			ss := newSessionStore(gen, log)
			h := newConnectHandler(conf, ss, log)

			s := &session{clientID: "client-a"}
			props := &packet.Properties{SessionExpiryInterval: &tc.interval}
			connPkt := &packet.Connect{ClientID: []byte(s.clientID), Version: packet.MQTT50, Properties: props}

			replies, err := h.handlePacket("", connPkt)
			assert.Nil(t, err)
			require.Len(t, replies, 1)

			s, err = ss.readSession(s.clientID)
			require.Nil(t, err)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			require.NotNil(t, connAckPkt.Properties)

			props = connAckPkt.Properties
			require.NotNil(t, props.SessionExpiryInterval)
			assert.Equal(t, tc.resp, *props.SessionExpiryInterval)

			assert.True(t, s.connected)
			assert.Equal(t, tc.resp, s.expiryInterval)
		})
	}
}

func TestConnectHandlePacketV3MaxKeepAliveRejected(t *testing.T) {
	testCases := []struct {
		keepAlive    uint16
		maxKeepAlive uint16
	}{
		{0, 100},
		{101, 100},
		{501, 500},
		{65535, 65534},
	}

	for _, tc := range testCases {
		testName := fmt.Sprintf("%v-%v", tc.keepAlive, tc.maxKeepAlive)
		t.Run(testName, func(t *testing.T) {
			conf := newDefaultConfig()
			conf.MaxKeepAlive = int(tc.maxKeepAlive)

			gen := newIDGeneratorMock(10)
			log := newLogger()
			ss := newSessionStore(gen, log)
			h := newConnectHandler(conf, ss, log)

			connPkt := &packet.Connect{ClientID: []byte("client-a"), Version: packet.MQTT311, KeepAlive: tc.keepAlive}
			replies, err := h.handlePacket("", connPkt)
			assert.NotNil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV3IdentifierRejected, connAckPkt.ReasonCode)
		})
	}
}

func TestConnectHandlePacketV3MaxKeepAliveAccepted(t *testing.T) {
	conf := newDefaultConfig()
	conf.MaxKeepAlive = 100

	gen := newIDGeneratorMock(10)
	log := newLogger()
	ss := newSessionStore(gen, log)
	h := newConnectHandler(conf, ss, log)

	s := &session{clientID: "client-a"}
	connPkt := &packet.Connect{ClientID: []byte(s.clientID), Version: packet.MQTT311, KeepAlive: 100}

	replies, err := h.handlePacket("", connPkt)
	assert.Nil(t, err)

	s, err = ss.readSession(s.clientID)
	require.Nil(t, err)
	require.Len(t, replies, 1)

	reply := replies[0]
	require.Equal(t, packet.CONNACK, reply.Type())

	connAckPkt := reply.(*packet.ConnAck)
	assert.Equal(t, packet.ReasonCodeV3ConnectionAccepted, connAckPkt.ReasonCode)

	assert.True(t, s.connected)
	assert.Equal(t, conf.MaxKeepAlive, s.keepAlive)
}

func TestConnectHandlePacketV5MaxKeepAliveAccepted(t *testing.T) {
	conf := newDefaultConfig()
	conf.MaxKeepAlive = 100

	gen := newIDGeneratorMock(10)
	log := newLogger()
	ss := newSessionStore(gen, log)
	h := newConnectHandler(conf, ss, log)

	s := &session{clientID: "client-a"}
	connPkt := &packet.Connect{ClientID: []byte(s.clientID), Version: packet.MQTT50, KeepAlive: 200}

	replies, err := h.handlePacket("", connPkt)
	assert.Nil(t, err)

	s, err = ss.readSession(s.clientID)
	require.Nil(t, err)
	require.Len(t, replies, 1)

	reply := replies[0]
	require.Equal(t, packet.CONNACK, reply.Type())

	connAckPkt := reply.(*packet.ConnAck)
	assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)

	assert.True(t, s.connected)
	assert.Equal(t, conf.MaxKeepAlive, s.keepAlive)

	require.NotNil(t, connAckPkt.Properties)
	assert.NotNil(t, connAckPkt.Properties.ServerKeepAlive)
	assert.Equal(t, conf.MaxKeepAlive, int(*connAckPkt.Properties.ServerKeepAlive))
}

func TestConnectHandlePacketV5MaxInflightMessages(t *testing.T) {
	testCases := []struct {
		maxInflight uint16
		resp        uint16
	}{
		{0, 0},
		{255, 255},
		{65534, 65534},
		{65535, 0},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.maxInflight), func(t *testing.T) {
			conf := newDefaultConfig()
			conf.MaxInflightMessages = int(tc.maxInflight)

			gen := newIDGeneratorMock(10)
			log := newLogger()
			ss := newSessionStore(gen, log)
			h := newConnectHandler(conf, ss, log)

			s := &session{clientID: "client-a"}
			connPkt := &packet.Connect{ClientID: []byte(s.clientID), Version: packet.MQTT50}

			replies, err := h.handlePacket("", connPkt)
			assert.Nil(t, err)

			s, err = ss.readSession(s.clientID)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.True(t, s.connected)

			if tc.resp > 0 {
				require.NotNil(t, connAckPkt.Properties)
				require.NotNil(t, connAckPkt.Properties.ReceiveMaximum)
				assert.Equal(t, tc.resp, *connAckPkt.Properties.ReceiveMaximum)
			} else {
				assert.Nil(t, connAckPkt.Properties)
			}
		})
	}
}

func TestConnectHandlePacketV5MaxPacketSize(t *testing.T) {
	testCases := []struct {
		maxSize uint32
		resp    uint32
	}{
		{0, 0},
		{255, 255},
		{65535, 65535},
		{16777215, 16777215},
		{268435455, 268435455},
		{268435456, 0},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.maxSize), func(t *testing.T) {
			conf := newDefaultConfig()
			conf.MaxPacketSize = int(tc.maxSize)

			gen := newIDGeneratorMock(10)
			log := newLogger()
			ss := newSessionStore(gen, log)
			h := newConnectHandler(conf, ss, log)

			s := &session{clientID: "client-a"}
			connPkt := &packet.Connect{ClientID: []byte(s.clientID), Version: packet.MQTT50}

			replies, err := h.handlePacket("", connPkt)
			assert.Nil(t, err)

			s, err = ss.readSession(s.clientID)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.True(t, s.connected)

			if tc.resp > 0 {
				require.NotNil(t, connAckPkt.Properties)
				require.NotNil(t, connAckPkt.Properties.MaximumPacketSize)
				assert.Equal(t, tc.resp, *connAckPkt.Properties.MaximumPacketSize)
			} else {
				assert.Nil(t, connAckPkt.Properties)
			}
		})
	}
}

func TestConnectHandlePacketV5MaximumQoS(t *testing.T) {
	testCases := []packet.QoS{
		packet.QoS0,
		packet.QoS1,
		packet.QoS2,
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc), func(t *testing.T) {
			conf := newDefaultConfig()
			conf.MaximumQoS = int(tc)

			gen := newIDGeneratorMock(10)
			log := newLogger()
			ss := newSessionStore(gen, log)
			h := newConnectHandler(conf, ss, log)

			s := &session{clientID: "client-a"}
			connPkt := &packet.Connect{ClientID: []byte(s.clientID), Version: packet.MQTT50}

			replies, err := h.handlePacket("", connPkt)
			assert.Nil(t, err)

			s, err = ss.readSession(s.clientID)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.True(t, s.connected)

			if tc < packet.QoS2 {
				require.NotNil(t, connAckPkt.Properties)
				require.NotNil(t, connAckPkt.Properties.MaximumQoS)
				assert.Equal(t, byte(tc), *connAckPkt.Properties.MaximumQoS)
			} else {
				assert.Nil(t, connAckPkt.Properties)
			}
		})
	}
}

func TestConnectHandlePacketV5TopicAliasMaximum(t *testing.T) {
	testCases := []struct {
		maxAlias uint16
	}{
		{0},
		{255},
		{65535},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.maxAlias), func(t *testing.T) {
			conf := newDefaultConfig()
			conf.MaxTopicAlias = int(tc.maxAlias)

			gen := newIDGeneratorMock(10)
			log := newLogger()
			ss := newSessionStore(gen, log)
			h := newConnectHandler(conf, ss, log)

			s := &session{clientID: "client-a"}
			connPkt := &packet.Connect{ClientID: []byte(s.clientID), Version: packet.MQTT50}

			replies, err := h.handlePacket("", connPkt)
			assert.Nil(t, err)

			s, err = ss.readSession(s.clientID)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.True(t, s.connected)

			if tc.maxAlias > 0 {
				require.NotNil(t, connAckPkt.Properties)
				require.NotNil(t, connAckPkt.Properties.TopicAliasMaximum)
				assert.Equal(t, tc.maxAlias, *connAckPkt.Properties.TopicAliasMaximum)
			} else {
				assert.Nil(t, connAckPkt.Properties)
			}
		})
	}
}

func TestConnectHandlePacketV5RetainAvailable(t *testing.T) {
	testCases := []struct {
		available bool
	}{
		{false},
		{true},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.available), func(t *testing.T) {
			conf := newDefaultConfig()
			conf.RetainAvailable = tc.available

			gen := newIDGeneratorMock(10)
			log := newLogger()
			ss := newSessionStore(gen, log)
			h := newConnectHandler(conf, ss, log)

			s := &session{clientID: "client-a"}
			connPkt := &packet.Connect{ClientID: []byte(s.clientID), Version: packet.MQTT50}

			replies, err := h.handlePacket("", connPkt)
			assert.Nil(t, err)

			s, err = ss.readSession(s.clientID)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.True(t, s.connected)

			if !tc.available {
				require.NotNil(t, connAckPkt.Properties)
				require.NotNil(t, connAckPkt.Properties.RetainAvailable)
				assert.Equal(t, byte(0), *connAckPkt.Properties.RetainAvailable)
			} else {
				assert.Nil(t, connAckPkt.Properties)
			}
		})
	}
}

func TestConnectHandlePacketV5WildcardSubsAvailable(t *testing.T) {
	testCases := []struct{ available bool }{
		{false},
		{true},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.available), func(t *testing.T) {
			conf := newDefaultConfig()
			conf.WildcardSubscriptionAvailable = tc.available

			gen := newIDGeneratorMock(10)
			log := newLogger()
			ss := newSessionStore(gen, log)
			h := newConnectHandler(conf, ss, log)

			s := &session{clientID: "client-a"}
			connPkt := &packet.Connect{ClientID: []byte(s.clientID), Version: packet.MQTT50}

			replies, err := h.handlePacket("", connPkt)
			assert.Nil(t, err)

			s, err = ss.readSession(s.clientID)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.True(t, s.connected)

			if !tc.available {
				require.NotNil(t, connAckPkt.Properties)
				require.NotNil(t, connAckPkt.Properties.WildcardSubscriptionAvailable)
				assert.Equal(t, byte(0), *connAckPkt.Properties.WildcardSubscriptionAvailable)
			} else {
				assert.Nil(t, connAckPkt.Properties)
			}
		})
	}
}

func TestConnectHandlePacketV5SubscriptionIDAvailable(t *testing.T) {
	testCases := []struct{ available bool }{
		{false},
		{true},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.available), func(t *testing.T) {
			conf := newDefaultConfig()
			conf.SubscriptionIDAvailable = tc.available

			gen := newIDGeneratorMock(10)
			log := newLogger()
			ss := newSessionStore(gen, log)
			h := newConnectHandler(conf, ss, log)

			s := &session{clientID: "client-a"}
			connPkt := &packet.Connect{ClientID: []byte(s.clientID), Version: packet.MQTT50}

			replies, err := h.handlePacket("", connPkt)
			assert.Nil(t, err)

			s, err = ss.readSession(s.clientID)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.True(t, s.connected)

			if !tc.available {
				require.NotNil(t, connAckPkt.Properties)
				require.NotNil(t, connAckPkt.Properties.SubscriptionIDAvailable)
				assert.Equal(t, byte(0), *connAckPkt.Properties.SubscriptionIDAvailable)
			} else {
				assert.Nil(t, connAckPkt.Properties)
			}
		})
	}
}

func TestConnectHandlePacketV5SharedSubscriptionAvailable(t *testing.T) {
	testCases := []struct{ available bool }{
		{false},
		{true},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.available), func(t *testing.T) {
			conf := newDefaultConfig()
			conf.SharedSubscriptionAvailable = tc.available

			gen := newIDGeneratorMock(10)
			log := newLogger()
			ss := newSessionStore(gen, log)
			h := newConnectHandler(conf, ss, log)

			s := &session{clientID: "client-a"}
			connPkt := &packet.Connect{ClientID: []byte(s.clientID), Version: packet.MQTT50}

			replies, err := h.handlePacket("", connPkt)
			assert.Nil(t, err)

			s, err = ss.readSession(s.clientID)
			require.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.True(t, s.connected)

			if !tc.available {
				require.NotNil(t, connAckPkt.Properties)
				require.NotNil(t, connAckPkt.Properties.SharedSubscriptionAvailable)
				assert.Equal(t, byte(0), *connAckPkt.Properties.SharedSubscriptionAvailable)
			} else {
				assert.Nil(t, connAckPkt.Properties)
			}
		})
	}
}

func TestConnectHandlePacketV5UserProperty(t *testing.T) {
	conf := newDefaultConfig()
	conf.UserProperties = map[string]string{"k1": "v1"}

	gen := newIDGeneratorMock(10)
	log := newLogger()
	ss := newSessionStore(gen, log)
	h := newConnectHandler(conf, ss, log)

	s := &session{clientID: "client-a"}
	connPkt := &packet.Connect{ClientID: []byte(s.clientID), Version: packet.MQTT50}

	replies, err := h.handlePacket("", connPkt)
	assert.Nil(t, err)

	s, err = ss.readSession(s.clientID)
	require.Nil(t, err)
	require.Len(t, replies, 1)

	reply := replies[0]
	require.Equal(t, packet.CONNACK, reply.Type())

	connAckPkt := reply.(*packet.ConnAck)
	assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)

	assert.True(t, s.connected)
	require.NotNil(t, connAckPkt.Properties)
	require.Len(t, connAckPkt.Properties.UserProperties, 1)
	assert.Equal(t, []byte("k1"), connAckPkt.Properties.UserProperties[0].Key)
	assert.Equal(t, []byte("v1"), connAckPkt.Properties.UserProperties[0].Value)
}
