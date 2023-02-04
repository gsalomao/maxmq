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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type delivererMock struct {
	mock.Mock
}

func (d *delivererMock) deliverPacket(id clientID, pkt *packet.Publish) error {
	args := d.Called(id, pkt)
	return args.Error(0)
}

type pubSubMock struct {
	mock.Mock
}

func (ps *pubSubMock) start() {
	ps.Called()
}

func (ps *pubSubMock) stop() {
	ps.Called()
}

func (ps *pubSubMock) subscribe(s *session, t packet.Topic,
	subsID int) (subscription, error) {

	args := ps.Called(s, t, subsID)
	return args.Get(0).(subscription), args.Error(1)
}

func (ps *pubSubMock) unsubscribe(id clientID, topic string) error {
	args := ps.Called(id, topic)
	return args.Error(0)
}

func (ps *pubSubMock) publish(msg *message) {
	ps.Called(msg)
}

func createSessionManager(conf Configuration) *sessionManager {
	log := newLogger()

	userProps := make([]packet.UserProperty, 0, len(conf.UserProperties))
	for k, v := range conf.UserProperties {
		userProps = append(userProps,
			packet.UserProperty{Key: []byte(k), Value: []byte(v)})
	}

	m := newMetrics(true, &log)
	idGen := &idGeneratorMock{}
	idGen.On("NextID").Return(1).Once()

	ps := &pubSubMock{}
	dl := &delivererMock{}

	sm := newSessionManager(&conf, idGen, m, userProps, &log)
	sm.pubSub = ps
	sm.deliverer = dl

	return sm
}

func TestSessionManagerConnectNewSession(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			connPkt := &packet.Connect{ClientID: []byte(id), Version: tc}

			s, replies, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)
			require.NotNil(t, s)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.False(t, connAckPkt.SessionPresent)

			assert.True(t, s.connected)
			assert.Equal(t, id, s.clientID)
			assert.NotZero(t, s.sessionID)
			assert.Equal(t, int(connPkt.KeepAlive), s.keepAlive)

			s2, ok := sm.sessions[s.clientID]
			require.True(t, ok)
			assert.Equal(t, s, s2)
		})
	}
}

func TestSessionManagerConnectExistingSession(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			s := &session{clientID: id, sessionID: 1, version: tc,
				connectedAt:    time.Now().Add(-1 * time.Minute).Unix(),
				expiryInterval: conf.MaxSessionExpiryInterval,
				subscriptions:  make(map[string]subscription)}

			sub := subscription{id: 1, topicFilter: "test", clientID: id}
			s.subscriptions["test"] = sub
			sm.store.sessions[s.clientID] = s

			connPkt := &packet.Connect{ClientID: []byte(id), Version: tc}

			s, replies, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)
			require.NotNil(t, s)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, tc, connAckPkt.Version)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.True(t, connAckPkt.SessionPresent)

			assert.True(t, s.connected)
			assert.Equal(t, id, s.clientID)
			assert.NotZero(t, s.sessionID)
			assert.NotEmpty(t, s.subscriptions)
		})
	}
}

func TestSessionManagerConnectExistingSessionWithCleanSession(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			s := &session{clientID: id, sessionID: 1, version: tc,
				connectedAt:    time.Now().Add(-1 * time.Minute).Unix(),
				expiryInterval: conf.MaxSessionExpiryInterval,
				subscriptions:  make(map[string]subscription)}

			sub := subscription{id: 1, topicFilter: "test", clientID: id}
			s.subscriptions["test"] = sub
			sm.store.sessions[s.clientID] = s

			ps := sm.pubSub.(*pubSubMock)
			ps.On("unsubscribe", id, sub.topicFilter).Return(nil)

			connPkt := &packet.Connect{ClientID: []byte(id), Version: tc,
				CleanSession: true}

			s, replies, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)
			require.NotNil(t, s)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, tc, connAckPkt.Version)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.False(t, connAckPkt.SessionPresent)

			assert.True(t, s.connected)
			assert.Equal(t, id, s.clientID)
			assert.NotZero(t, s.sessionID)
			assert.Empty(t, s.subscriptions)
			ps.AssertExpectations(t)
		})
	}
}

func TestSessionManagerConnectClientIDTooBig(t *testing.T) {
	testCases := []struct {
		version  packet.MQTTVersion
		id       []byte
		maxIDLen int
		code     packet.ReasonCode
	}{
		{version: packet.MQTT31, id: []byte("012345678901234567890123"),
			maxIDLen: 65535, code: packet.ReasonCodeV3IdentifierRejected,
		},
		{version: packet.MQTT311, id: []byte("0123456789012345678901234567890"),
			maxIDLen: 30, code: packet.ReasonCodeV3IdentifierRejected,
		},
		{version: packet.MQTT50, id: []byte("0123456789012345678901234567890"),
			maxIDLen: 30, code: packet.ReasonCodeV5InvalidClientID,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.version.String(), func(t *testing.T) {
			conf := newConfiguration()
			conf.MaxClientIDLen = tc.maxIDLen
			sm := createSessionManager(conf)

			connPkt := &packet.Connect{ClientID: tc.id, Version: tc.version}

			s, replies, err := sm.handlePacket("", connPkt)
			require.NotNil(t, err)
			assert.Nil(t, s)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, tc.version, connAckPkt.Version)
			assert.Equal(t, tc.code, connAckPkt.ReasonCode)
		})
	}
}

func TestSessionManagerConnectAllowEmptyClientID(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			conf.AllowEmptyClientID = true
			sm := createSessionManager(conf)

			connPkt := &packet.Connect{Version: tc}

			s, replies, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)
			require.NotNil(t, s)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)

			assert.True(t, s.connected)
			assert.Greater(t, len(s.clientID), 0)
		})
	}
}

func TestSessionManagerConnectDenyEmptyClientID(t *testing.T) {
	testCases := []struct {
		version packet.MQTTVersion
		code    packet.ReasonCode
	}{
		{version: packet.MQTT31, code: packet.ReasonCodeV3IdentifierRejected},
		{version: packet.MQTT311, code: packet.ReasonCodeV3IdentifierRejected},
		{version: packet.MQTT50, code: packet.ReasonCodeV5InvalidClientID},
	}

	for _, tc := range testCases {
		t.Run(tc.version.String(), func(t *testing.T) {
			conf := newConfiguration()
			conf.AllowEmptyClientID = false
			sm := createSessionManager(conf)

			connPkt := &packet.Connect{Version: tc.version}

			s, replies, err := sm.handlePacket("", connPkt)
			require.NotNil(t, err)
			assert.Nil(t, s)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, tc.code, connAckPkt.ReasonCode)
		})
	}
}

func TestSessionManagerConnectV5AssignClientID(t *testing.T) {
	conf := newConfiguration()
	conf.AllowEmptyClientID = true
	sm := createSessionManager(conf)

	connPkt := &packet.Connect{Version: packet.MQTT50}

	s, replies, err := sm.handlePacket("", connPkt)
	require.Nil(t, err)
	require.NotNil(t, s)

	require.Len(t, replies, 1)
	reply := replies[0]
	require.Equal(t, packet.CONNACK, reply.Type())

	connAckPkt := reply.(*packet.ConnAck)
	assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
	assert.NotNil(t, connAckPkt.Properties)
	assert.NotNil(t, connAckPkt.Properties.AssignedClientID)
	assert.Equal(t, 20, len(connAckPkt.Properties.AssignedClientID))

	assert.True(t, s.connected)
	assert.NotZero(t, s.sessionID)
	assert.Equal(t, clientID(connAckPkt.Properties.AssignedClientID),
		s.clientID)
}

func TestSessionManagerConnectV5AssignClientIDWithPrefix(t *testing.T) {
	conf := newConfiguration()
	conf.AllowEmptyClientID = true
	conf.ClientIDPrefix = []byte("MAXMQ-")
	sm := createSessionManager(conf)

	connPkt := &packet.Connect{Version: packet.MQTT50}

	s, replies, err := sm.handlePacket("", connPkt)
	require.Nil(t, err)
	require.NotNil(t, s)

	require.Len(t, replies, 1)
	reply := replies[0]
	require.Equal(t, packet.CONNACK, reply.Type())

	connAckPkt := replies[0].(*packet.ConnAck)
	assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
	assert.NotNil(t, connAckPkt.Properties)
	assert.NotNil(t, connAckPkt.Properties.AssignedClientID)
	assert.Equal(t, 26, len(connAckPkt.Properties.AssignedClientID))
	assert.Equal(t, conf.ClientIDPrefix,
		connAckPkt.Properties.AssignedClientID[:len(conf.ClientIDPrefix)])

	assert.True(t, s.connected)
	assert.NotZero(t, s.sessionID)
	assert.Equal(t, clientID(connAckPkt.Properties.AssignedClientID),
		s.clientID)
}

func TestSessionManagerConnectV5MaxSessionExpiryInterval(t *testing.T) {
	testCases := []struct {
		interval    uint32
		maxInterval uint32
		resp        uint32
	}{
		{interval: 0, maxInterval: 100, resp: 0},
		{interval: 100, maxInterval: 100, resp: 0},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.interval), func(t *testing.T) {
			conf := newConfiguration()
			conf.MaxSessionExpiryInterval = tc.maxInterval
			sm := createSessionManager(conf)

			props := &packet.Properties{SessionExpiryInterval: &tc.interval}
			connPkt := &packet.Connect{ClientID: []byte("a"),
				Version: packet.MQTT50, Properties: props}

			s, replies, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)
			require.NotNil(t, s)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.NotNil(t, connAckPkt.Properties)
			assert.True(t, s.connected)

			props = connAckPkt.Properties
			assert.Nil(t, props.SessionExpiryInterval)
		})
	}
}

func TestSessionManagerConnectV5AboveMaxSessionExpiryInterval(t *testing.T) {
	testCases := []struct {
		interval    uint32
		maxInterval uint32
		resp        uint32
	}{
		{interval: 101, maxInterval: 100, resp: 100},
		{interval: 2000, maxInterval: 1000, resp: 1000},
		{interval: 100000, maxInterval: 80000, resp: 80000},
		{interval: 50000000, maxInterval: 32000000, resp: 32000000},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.interval), func(t *testing.T) {
			conf := newConfiguration()
			conf.MaxSessionExpiryInterval = tc.maxInterval
			sm := createSessionManager(conf)

			props := &packet.Properties{SessionExpiryInterval: &tc.interval}
			connPkt := &packet.Connect{ClientID: []byte("a"),
				Version: packet.MQTT50, Properties: props}

			s, replies, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)
			require.NotNil(t, s)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.NotNil(t, connAckPkt.Properties)
			assert.True(t, s.connected)

			props = connAckPkt.Properties
			assert.NotNil(t, props.SessionExpiryInterval)
			assert.Equal(t, tc.resp, *props.SessionExpiryInterval)
			assert.Equal(t, tc.resp, s.expiryInterval)
		})
	}
}

func TestSessionManagerConnectV3MaxKeepAliveRejected(t *testing.T) {
	testCases := []struct {
		keepAlive    uint16
		maxKeepAlive uint16
	}{
		{keepAlive: 0, maxKeepAlive: 100},
		{keepAlive: 101, maxKeepAlive: 100},
		{keepAlive: 501, maxKeepAlive: 500},
		{keepAlive: 65535, maxKeepAlive: 65534},
	}

	for _, tc := range testCases {
		testName := fmt.Sprintf("%v-%v", tc.keepAlive, tc.maxKeepAlive)
		t.Run(testName, func(t *testing.T) {
			conf := newConfiguration()
			conf.MaxKeepAlive = int(tc.maxKeepAlive)
			sm := createSessionManager(conf)

			connPkt := &packet.Connect{ClientID: []byte("a"),
				Version: packet.MQTT311, KeepAlive: tc.keepAlive}

			s, replies, err := sm.handlePacket("", connPkt)
			require.NotNil(t, err)
			assert.Nil(t, s)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV3IdentifierRejected,
				connAckPkt.ReasonCode)
		})
	}
}

func TestSessionManagerConnectV3MaxKeepAliveAccepted(t *testing.T) {
	conf := newConfiguration()
	conf.MaxKeepAlive = 100
	sm := createSessionManager(conf)

	connPkt := &packet.Connect{ClientID: []byte("a"), Version: packet.MQTT311,
		KeepAlive: 100}

	s, replies, err := sm.handlePacket("", connPkt)
	require.Nil(t, err)

	require.Len(t, replies, 1)
	reply := replies[0]
	require.Equal(t, packet.CONNACK, reply.Type())

	connAckPkt := reply.(*packet.ConnAck)
	assert.Equal(t, packet.ReasonCodeV3ConnectionAccepted,
		connAckPkt.ReasonCode)

	require.NotNil(t, s)
	assert.True(t, s.connected)
	assert.Equal(t, conf.MaxKeepAlive, s.keepAlive)
}

func TestSessionManagerConnectV5MaxKeepAliveAccepted(t *testing.T) {
	conf := newConfiguration()
	conf.MaxKeepAlive = 100
	sm := createSessionManager(conf)

	connPkt := &packet.Connect{ClientID: []byte("a"), Version: packet.MQTT50,
		KeepAlive: 200}

	s, replies, err := sm.handlePacket("", connPkt)
	require.Nil(t, err)

	require.Len(t, replies, 1)
	reply := replies[0]
	require.Equal(t, packet.CONNACK, reply.Type())

	connAckPkt := reply.(*packet.ConnAck)
	assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)

	require.NotNil(t, s)
	assert.True(t, s.connected)
	assert.Equal(t, conf.MaxKeepAlive, s.keepAlive)
	assert.NotNil(t, connAckPkt.Properties)
	assert.NotNil(t, connAckPkt.Properties.ServerKeepAlive)
	assert.Equal(t, conf.MaxKeepAlive,
		int(*connAckPkt.Properties.ServerKeepAlive))
}

func TestSessionManagerConnectV5MaxInflightMessages(t *testing.T) {
	testCases := []struct {
		maxInflight uint16
		resp        uint16
	}{
		{maxInflight: 0, resp: 0},
		{maxInflight: 255, resp: 255},
		{maxInflight: 65534, resp: 65534},
		{maxInflight: 65535, resp: 0},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.maxInflight), func(t *testing.T) {
			conf := newConfiguration()
			conf.MaxInflightMessages = int(tc.maxInflight)
			sm := createSessionManager(conf)

			connPkt := &packet.Connect{ClientID: []byte("a"),
				Version: packet.MQTT50}

			s, replies, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)
			require.NotNil(t, s)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			require.NotNil(t, connAckPkt.Properties)
			assert.True(t, s.connected)
			assert.NotZero(t, s.sessionID)

			if tc.resp > 0 {
				assert.NotNil(t, connAckPkt.Properties.ReceiveMaximum)
				assert.Equal(t, tc.resp, *connAckPkt.Properties.ReceiveMaximum)
			} else {
				assert.Nil(t, connAckPkt.Properties.ReceiveMaximum)
			}
		})
	}
}

func TestSessionManagerConnectV5MaxPacketSize(t *testing.T) {
	testCases := []struct {
		maxSize uint32
		resp    uint32
	}{
		{maxSize: 0, resp: 0},
		{maxSize: 255, resp: 255},
		{maxSize: 65535, resp: 65535},
		{maxSize: 16777215, resp: 16777215},
		{maxSize: 268435455, resp: 268435455},
		{maxSize: 268435456, resp: 0},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.maxSize), func(t *testing.T) {
			conf := newConfiguration()
			conf.MaxPacketSize = int(tc.maxSize)
			sm := createSessionManager(conf)

			connPkt := &packet.Connect{ClientID: []byte("a"),
				Version: packet.MQTT50}

			s, replies, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)
			require.NotNil(t, s)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			require.NotNil(t, connAckPkt.Properties)
			assert.True(t, s.connected)

			if tc.resp > 0 {
				assert.NotNil(t, connAckPkt.Properties.MaximumPacketSize)
				assert.Equal(t, tc.resp,
					*connAckPkt.Properties.MaximumPacketSize)
			} else {
				assert.Nil(t, connAckPkt.Properties.MaximumPacketSize)
			}
		})
	}
}

func TestSessionManagerConnectV5MaximumQoS(t *testing.T) {
	testCases := []packet.QoS{
		packet.QoS0,
		packet.QoS1,
		packet.QoS2,
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc), func(t *testing.T) {
			conf := newConfiguration()
			conf.MaximumQoS = int(tc)
			sm := createSessionManager(conf)

			connPkt := &packet.Connect{ClientID: []byte("a"),
				Version: packet.MQTT50}

			s, replies, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)
			require.NotNil(t, s)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			require.NotNil(t, connAckPkt.Properties)
			assert.True(t, s.connected)

			if tc < packet.QoS2 {
				assert.NotNil(t, connAckPkt.Properties.MaximumQoS)
				assert.Equal(t, byte(tc), *connAckPkt.Properties.MaximumQoS)
			} else {
				assert.Nil(t, connAckPkt.Properties.MaximumQoS)
			}
		})
	}
}

func TestSessionManagerConnectV5TopicAliasMaximum(t *testing.T) {
	testCases := []struct{ maxAlias uint16 }{
		{maxAlias: 0},
		{maxAlias: 255},
		{maxAlias: 65535},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.maxAlias), func(t *testing.T) {
			conf := newConfiguration()
			conf.MaxTopicAlias = int(tc.maxAlias)
			sm := createSessionManager(conf)

			connPkt := &packet.Connect{ClientID: []byte("a"),
				Version: packet.MQTT50}

			s, replies, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)
			require.NotNil(t, s)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			require.NotNil(t, connAckPkt.Properties)
			assert.True(t, s.connected)

			if tc.maxAlias > 0 {
				assert.NotNil(t, connAckPkt.Properties.TopicAliasMaximum)
				assert.Equal(t, tc.maxAlias,
					*connAckPkt.Properties.TopicAliasMaximum)
			} else {
				assert.Nil(t, connAckPkt.Properties.TopicAliasMaximum)
			}
		})
	}
}

func TestSessionManagerConnectV5RetainAvailable(t *testing.T) {
	testCases := []struct{ available bool }{
		{available: false},
		{available: true},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.available), func(t *testing.T) {
			conf := newConfiguration()
			conf.RetainAvailable = tc.available
			sm := createSessionManager(conf)

			connPkt := &packet.Connect{ClientID: []byte("a"),
				Version: packet.MQTT50}

			s, replies, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)
			require.NotNil(t, s)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			require.NotNil(t, connAckPkt.Properties)
			assert.True(t, s.connected)

			if !tc.available {
				assert.NotNil(t, connAckPkt.Properties.RetainAvailable)
				assert.Equal(t, byte(0), *connAckPkt.Properties.RetainAvailable)
			} else {
				assert.Nil(t, connAckPkt.Properties.RetainAvailable)
			}
		})
	}
}

func TestSessionManagerConnectV5WildcardSubsAvailable(t *testing.T) {
	testCases := []struct{ available bool }{
		{available: false},
		{available: true},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.available), func(t *testing.T) {
			conf := newConfiguration()
			conf.WildcardSubscriptionAvailable = tc.available
			sm := createSessionManager(conf)

			connPkt := &packet.Connect{ClientID: []byte("a"),
				Version: packet.MQTT50}

			s, replies, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)
			require.NotNil(t, s)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			require.NotNil(t, connAckPkt.Properties)
			assert.True(t, s.connected)

			if !tc.available {
				assert.NotNil(t,
					connAckPkt.Properties.WildcardSubscriptionAvailable)
				assert.Equal(t, byte(0),
					*connAckPkt.Properties.WildcardSubscriptionAvailable)
			} else {
				assert.Nil(t,
					connAckPkt.Properties.WildcardSubscriptionAvailable)
			}
		})
	}
}

func TestSessionManagerConnectV5SubscriptionIDAvailable(t *testing.T) {
	testCases := []struct{ available bool }{
		{available: false},
		{available: true},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.available), func(t *testing.T) {
			conf := newConfiguration()
			conf.SubscriptionIDAvailable = tc.available
			sm := createSessionManager(conf)

			connPkt := &packet.Connect{ClientID: []byte("a"),
				Version: packet.MQTT50}

			s, replies, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)
			require.NotNil(t, s)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			require.NotNil(t, connAckPkt.Properties)
			assert.True(t, s.connected)

			if !tc.available {
				assert.NotNil(t,
					connAckPkt.Properties.SubscriptionIDAvailable)
				assert.Equal(t, byte(0),
					*connAckPkt.Properties.SubscriptionIDAvailable)
			} else {
				assert.Nil(t, connAckPkt.Properties.SubscriptionIDAvailable)
			}
		})
	}
}

func TestSessionManagerConnectV5SharedSubscriptionAvailable(t *testing.T) {
	testCases := []struct{ available bool }{
		{available: false},
		{available: true},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.available), func(t *testing.T) {
			conf := newConfiguration()
			conf.SharedSubscriptionAvailable = tc.available
			sm := createSessionManager(conf)

			connPkt := &packet.Connect{ClientID: []byte("a"),
				Version: packet.MQTT50}

			s, replies, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)
			require.NotNil(t, s)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.NotNil(t, connAckPkt.Properties)
			assert.True(t, s.connected)

			if !tc.available {
				assert.NotNil(t,
					connAckPkt.Properties.SharedSubscriptionAvailable)
				assert.Equal(t, byte(0),
					*connAckPkt.Properties.SharedSubscriptionAvailable)
			} else {
				assert.Nil(t, connAckPkt.Properties.SharedSubscriptionAvailable)
			}
		})
	}
}

func TestSessionManagerConnectV5UserProperty(t *testing.T) {
	conf := newConfiguration()
	conf.UserProperties = map[string]string{"k1": "v1"}
	sm := createSessionManager(conf)

	connPkt := &packet.Connect{ClientID: []byte("a"), Version: packet.MQTT50}

	s, replies, err := sm.handlePacket("", connPkt)
	require.Nil(t, err)
	require.NotNil(t, s)
	assert.True(t, s.connected)

	require.Len(t, replies, 1)
	reply := replies[0]
	require.Equal(t, packet.CONNACK, reply.Type())

	connAckPkt := reply.(*packet.ConnAck)
	assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)

	require.NotNil(t, connAckPkt.Properties)
	assert.Equal(t, 1, len(connAckPkt.Properties.UserProperties))
	assert.Equal(t, []byte("k1"), connAckPkt.Properties.UserProperties[0].Key)
	assert.Equal(t, []byte("v1"), connAckPkt.Properties.UserProperties[0].Value)
}

func TestSessionManagerConnectWithInflightMessages(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)

			s := &session{}
			sm.store.sessions["a"] = s
			inflightMsgList := make([]*message, 0)

			for i := 0; i < 10; i++ {
				qos := packet.QoS(i%2 + 1)

				topic := fmt.Sprintf("data/%v", i)
				newPub := packet.NewPublish(packet.ID(i+1), tc, topic, qos,
					0, 0, nil, nil)

				msg := message{packetID: newPub.PacketID, packet: &newPub,
					id: messageID(newPub.PacketID)}

				s.inflightMessages.PushBack(&msg)
				inflightMsgList = append(inflightMsgList, &msg)
			}

			connPkt := packet.Connect{ClientID: []byte{'a'}, Version: tc}

			s, replies, err := sm.handlePacket("", &connPkt)
			require.Nil(t, err)
			require.NotNil(t, s)

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

func TestSessionManagerConnectWithInflightMessagesWithoutPacket(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)

			s := &session{}
			sm.store.sessions["a"] = s

			for i := 0; i < 10; i++ {
				msg := message{id: messageID(i + 1), packetID: packet.ID(i + 1)}
				s.inflightMessages.PushBack(&msg)
			}

			connPkt := packet.Connect{ClientID: []byte{'a'}, Version: tc}

			s, replies, err := sm.handlePacket("", &connPkt)
			require.Nil(t, err)
			require.NotNil(t, s)

			require.Len(t, replies, 1)
			reply := replies[0]
			assert.Equal(t, packet.CONNACK, reply.Type())
		})
	}
}

func TestSessionManagerPingReq(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			id := clientID('a')
			conf := newConfiguration()
			sm := createSessionManager(conf)

			connPkt := &packet.Connect{ClientID: []byte("a"), Version: tc}

			_, _, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)

			pingReqPkt := &packet.PingReq{}
			_, replies, err := sm.handlePacket(id, pingReqPkt)
			require.Nil(t, err)

			require.Len(t, replies, 1)
			reply := replies[0]
			assert.Equal(t, packet.PINGRESP, reply.Type())
		})
	}
}

func TestSessionManagerPingReqReadSessionError(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			id := clientID('a')
			conf := newConfiguration()
			sm := createSessionManager(conf)

			pingReqPkt := &packet.PingReq{}
			s, replies, err := sm.handlePacket(id, pingReqPkt)
			assert.Nil(t, s)
			assert.Empty(t, replies)
			assert.NotNil(t, err)
		})
	}
}

func TestSessionManagerSubscribe(t *testing.T) {
	testCases := []struct {
		version packet.MQTTVersion
		qos     packet.QoS
	}{
		{version: packet.MQTT31, qos: packet.QoS0},
		{version: packet.MQTT311, qos: packet.QoS0},
		{version: packet.MQTT50, qos: packet.QoS0},
		{version: packet.MQTT31, qos: packet.QoS1},
		{version: packet.MQTT311, qos: packet.QoS1},
		{version: packet.MQTT50, qos: packet.QoS1},
		{version: packet.MQTT31, qos: packet.QoS2},
		{version: packet.MQTT311, qos: packet.QoS2},
		{version: packet.MQTT50, qos: packet.QoS2},
	}

	for _, tc := range testCases {
		name := fmt.Sprintf("%v-%v", tc.version.String(), tc.qos)
		t.Run(name, func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			connPkt := &packet.Connect{ClientID: []byte(id),
				Version: tc.version}

			s, _, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)

			subPkt := packet.Subscribe{PacketID: 1, Version: tc.version,
				Topics: []packet.Topic{{Name: "data", QoS: tc.qos}}}

			sub := subscription{id: 0, clientID: id, topicFilter: "data",
				qos: tc.qos}

			ps := sm.pubSub.(*pubSubMock)
			ps.On("subscribe", s, subPkt.Topics[0], 0).
				Return(sub, nil)

			s, replies, err := sm.handlePacket(id, &subPkt)
			require.Nil(t, err)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.SUBACK, reply.Type())

			subAckPkt := reply.(*packet.SubAck)
			assert.Equal(t, subPkt.PacketID, subAckPkt.PacketID)
			assert.Equal(t, tc.version, subAckPkt.Version)
			assert.Len(t, subAckPkt.ReasonCodes, 1)
			assert.Equal(t, packet.ReasonCode(tc.qos), subAckPkt.ReasonCodes[0])

			require.Len(t, s.subscriptions, 1)
			assert.Equal(t, sub, s.subscriptions["data"])
			ps.AssertExpectations(t)
		})
	}
}

func TestSessionManagerSubscribeError(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			connPkt := &packet.Connect{ClientID: []byte(id), Version: tc}

			_, _, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)

			subPkt := packet.Subscribe{PacketID: 1, Version: tc,
				Topics: []packet.Topic{{Name: "data#", QoS: packet.QoS1}}}

			ps := sm.pubSub.(*pubSubMock)
			ps.On("subscribe", mock.Anything, mock.Anything,
				mock.Anything).
				Return(subscription{}, errors.New("invalid subscription"))

			s, replies, err := sm.handlePacket(id, &subPkt)
			require.Nil(t, err)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.SUBACK, reply.Type())

			subAckPkt := reply.(*packet.SubAck)
			assert.Equal(t, subPkt.PacketID, subAckPkt.PacketID)
			assert.Equal(t, tc, subAckPkt.Version)
			assert.Len(t, subAckPkt.ReasonCodes, 1)
			assert.Equal(t, packet.ReasonCodeV3Failure,
				subAckPkt.ReasonCodes[0])

			require.NotNil(t, s)
			assert.Empty(t, s.subscriptions)
			ps.AssertExpectations(t)
		})
	}
}

func TestSessionManagerSubscribeReadSessionError(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			subPkt := packet.Subscribe{PacketID: 1, Version: tc,
				Topics: []packet.Topic{{Name: "data#", QoS: packet.QoS1}}}

			s, replies, err := sm.handlePacket(id, &subPkt)
			assert.Nil(t, s)
			assert.Empty(t, replies)
			assert.NotNil(t, err)
		})
	}
}

func TestSessionManagerSubscribeMultipleTopics(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			connPkt := &packet.Connect{ClientID: []byte(id), Version: tc}

			s, _, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)

			subPkt := packet.Subscribe{PacketID: 5, Version: tc,
				Topics: []packet.Topic{
					{Name: "data/temp/0", QoS: packet.QoS0},
					{Name: "data/temp/1", QoS: packet.QoS1},
					{Name: "data/temp/2", QoS: packet.QoS2},
					{Name: "data/temp#", QoS: packet.QoS0},
				},
			}

			subs := []subscription{
				{id: 0, clientID: id, topicFilter: subPkt.Topics[0].Name,
					qos: subPkt.Topics[0].QoS},
				{id: 0, clientID: id, topicFilter: subPkt.Topics[1].Name,
					qos: subPkt.Topics[1].QoS},
				{id: 0, clientID: id, topicFilter: subPkt.Topics[2].Name,
					qos: subPkt.Topics[2].QoS},
			}

			ps := sm.pubSub.(*pubSubMock)
			ps.On("subscribe", s, subPkt.Topics[0], 0).
				Return(subs[0], nil).
				On("subscribe", s, subPkt.Topics[1], 0).
				Return(subs[1], nil).
				On("subscribe", s, subPkt.Topics[2], 0).
				Return(subs[2], nil).
				On("subscribe", s, subPkt.Topics[3], 0).
				Return(subscription{}, errors.New("invalid topic"))

			s, replies, err := sm.handlePacket(id, &subPkt)
			require.Nil(t, err)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.SUBACK, reply.Type())

			subAckPkt := reply.(*packet.SubAck)
			assert.Equal(t, subPkt.PacketID, subAckPkt.PacketID)
			assert.Equal(t, subPkt.Version, subAckPkt.Version)
			assert.Len(t, subAckPkt.ReasonCodes, 4)
			assert.Equal(t, packet.ReasonCode(packet.QoS0),
				subAckPkt.ReasonCodes[0])
			assert.Equal(t, packet.ReasonCode(packet.QoS1),
				subAckPkt.ReasonCodes[1])
			assert.Equal(t, packet.ReasonCode(packet.QoS2),
				subAckPkt.ReasonCodes[2])
			assert.Equal(t, packet.ReasonCodeV3Failure,
				subAckPkt.ReasonCodes[3])

			require.Len(t, s.subscriptions, 3)
			assert.Equal(t, subs[0], s.subscriptions[subPkt.Topics[0].Name])
			assert.Equal(t, subs[1], s.subscriptions[subPkt.Topics[1].Name])
			assert.Equal(t, subs[2], s.subscriptions[subPkt.Topics[2].Name])
			ps.AssertExpectations(t)
		})
	}
}

func TestSessionManagerSubscribeV5WithSubID(t *testing.T) {
	conf := newConfiguration()
	conf.SubscriptionIDAvailable = true
	sm := createSessionManager(conf)
	id := clientID("a")

	connPkt := &packet.Connect{ClientID: []byte(id), Version: packet.MQTT50}

	_, _, err := sm.handlePacket("", connPkt)
	require.Nil(t, err)

	props := &packet.Properties{}
	props.SubscriptionIdentifier = new(int)
	*props.SubscriptionIdentifier = 1

	subPkt := packet.Subscribe{PacketID: 2, Version: packet.MQTT50,
		Properties: props, Topics: []packet.Topic{{Name: "topic"}}}

	sub := subscription{id: *props.SubscriptionIdentifier, clientID: id,
		topicFilter: subPkt.Topics[0].Name}

	ps := sm.pubSub.(*pubSubMock)
	ps.On("subscribe", mock.Anything, mock.Anything,
		*props.SubscriptionIdentifier).Return(sub, nil)

	_, replies, err := sm.handlePacket(id, &subPkt)
	assert.Nil(t, err)
	require.NotNil(t, replies)
	ps.AssertExpectations(t)

	require.Len(t, replies, 1)
	reply := replies[0]
	require.Equal(t, packet.SUBACK, reply.Type())
}

func TestSessionManagerSubscribeV5WithSubIDError(t *testing.T) {
	conf := newConfiguration()
	conf.SubscriptionIDAvailable = false
	sm := createSessionManager(conf)
	id := clientID("a")

	connPkt := &packet.Connect{ClientID: []byte(id), Version: packet.MQTT50}

	_, _, err := sm.handlePacket("", connPkt)
	require.Nil(t, err)

	props := &packet.Properties{}
	props.SubscriptionIdentifier = new(int)
	*props.SubscriptionIdentifier = 1

	subPkt := packet.Subscribe{PacketID: 2, Version: packet.MQTT50,
		Properties: props, Topics: []packet.Topic{{Name: "topic"}}}

	_, replies, err := sm.handlePacket(id, &subPkt)
	require.NotNil(t, err)

	require.Len(t, replies, 1)
	reply := replies[0]
	require.Equal(t, packet.DISCONNECT, reply.Type())

	discPkt := reply.(*packet.Disconnect)
	assert.Equal(t, packet.MQTT50, discPkt.Version)
	assert.Equal(t, packet.ReasonCodeV5SubscriptionIDNotSupported,
		discPkt.ReasonCode)
}

func TestSessionManagerUnsubscribeV3(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			connPkt := &packet.Connect{ClientID: []byte(id), Version: tc}

			s, _, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)

			sub := subscription{id: 1, clientID: id, topicFilter: "data"}
			s.subscriptions["data"] = sub

			ps := sm.pubSub.(*pubSubMock)
			ps.On("unsubscribe", id, "data").Return(nil)

			unsubPkt := packet.Unsubscribe{PacketID: 1, Version: tc,
				Topics: []string{"data"}}

			s, replies, err := sm.handlePacket(id, &unsubPkt)
			require.Nil(t, err)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.UNSUBACK, reply.Type())

			unsubAckPkt := reply.(*packet.UnsubAck)
			assert.Equal(t, unsubPkt.PacketID, unsubAckPkt.PacketID)
			assert.Equal(t, unsubPkt.Version, unsubAckPkt.Version)
			assert.Empty(t, unsubAckPkt.ReasonCodes)

			require.NotNil(t, s)
			assert.Empty(t, s.subscriptions)
			ps.AssertExpectations(t)
		})
	}
}

func TestSessionManagerUnsubscribeV5(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)
	id := clientID("a")

	connPkt := &packet.Connect{ClientID: []byte(id), Version: packet.MQTT50}

	s, _, err := sm.handlePacket("", connPkt)
	require.Nil(t, err)

	sub := subscription{id: 1, clientID: id, topicFilter: "data"}
	s.subscriptions["data"] = sub

	ps := sm.pubSub.(*pubSubMock)
	ps.On("unsubscribe", id, "data").Return(nil)

	unsubPkt := packet.Unsubscribe{PacketID: 1, Version: packet.MQTT50,
		Topics: []string{"data"}}

	s, replies, err := sm.handlePacket(id, &unsubPkt)
	require.Nil(t, err)

	require.Len(t, replies, 1)
	reply := replies[0]
	require.Equal(t, packet.UNSUBACK, reply.Type())

	unsubAckPkt := reply.(*packet.UnsubAck)
	assert.Equal(t, unsubPkt.PacketID, unsubAckPkt.PacketID)
	assert.Equal(t, unsubPkt.Version, unsubAckPkt.Version)

	codes := []packet.ReasonCode{packet.ReasonCodeV5Success}
	assert.Equal(t, codes, unsubAckPkt.ReasonCodes)

	require.NotNil(t, s)
	assert.Empty(t, s.subscriptions)
	ps.AssertExpectations(t)
}

func TestSessionManagerUnsubscribeReadSessionError(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			unsubPkt := packet.Unsubscribe{PacketID: 1, Version: tc,
				Topics: []string{"data"}}

			s, replies, err := sm.handlePacket(id, &unsubPkt)
			assert.Nil(t, s)
			assert.Empty(t, replies)
			assert.NotNil(t, err)
		})
	}
}

func TestSessionManagerUnsubscribeV3MissingSubscription(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			connPkt := &packet.Connect{ClientID: []byte(id), Version: tc}

			_, _, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)

			ps := sm.pubSub.(*pubSubMock)
			ps.On("unsubscribe", id, "data").Return(errSubscriptionNotFound)

			unsubPkt := packet.Unsubscribe{PacketID: 1, Version: tc,
				Topics: []string{"data"}}

			_, replies, err := sm.handlePacket(id, &unsubPkt)
			require.Nil(t, err)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.UNSUBACK, reply.Type())

			unsubAckPkt := reply.(*packet.UnsubAck)
			assert.Equal(t, unsubPkt.PacketID, unsubAckPkt.PacketID)
			assert.Equal(t, unsubPkt.Version, unsubAckPkt.Version)
			assert.Empty(t, unsubAckPkt.ReasonCodes)
			ps.AssertExpectations(t)
		})
	}
}

func TestSessionManagerUnsubscribeV5MissingSubscription(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)
	id := clientID("a")

	connPkt := &packet.Connect{ClientID: []byte(id), Version: packet.MQTT50}

	_, _, err := sm.handlePacket("", connPkt)
	require.Nil(t, err)

	ps := sm.pubSub.(*pubSubMock)
	ps.On("unsubscribe", id, "data").Return(errSubscriptionNotFound)

	unsubPkt := packet.Unsubscribe{PacketID: 1, Version: packet.MQTT50,
		Topics: []string{"data"}}

	_, replies, err := sm.handlePacket(id, &unsubPkt)
	require.Nil(t, err)

	require.Len(t, replies, 1)
	reply := replies[0]
	require.Equal(t, packet.UNSUBACK, reply.Type())

	unsubAckPkt := reply.(*packet.UnsubAck)
	assert.Equal(t, unsubPkt.PacketID, unsubAckPkt.PacketID)
	assert.Equal(t, unsubPkt.Version, unsubAckPkt.Version)

	codes := []packet.ReasonCode{packet.ReasonCodeV5NoSubscriptionExisted}
	assert.Equal(t, codes, unsubAckPkt.ReasonCodes)
	ps.AssertExpectations(t)
}

func TestSessionManagerUnsubscribeV3MultipleTopics(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			connPkt := &packet.Connect{ClientID: []byte(id), Version: tc}

			s, _, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)

			s.subscriptions["data/0"] = subscription{id: 1, clientID: id,
				topicFilter: "data/0", qos: packet.QoS0}
			s.subscriptions["data/1"] = subscription{id: 3, clientID: id,
				topicFilter: "data/1", qos: packet.QoS1}
			s.subscriptions["data/#"] = subscription{id: 3, clientID: id,
				topicFilter: "data/#", qos: packet.QoS2}

			ps := sm.pubSub.(*pubSubMock)
			ps.On("unsubscribe", id, "data/0").Return(nil).
				On("unsubscribe", id, "data/#").Return(nil)

			topicNames := []string{"data/0", "data/#"}
			unsubPkt := packet.Unsubscribe{PacketID: 1, Version: tc,
				Topics: topicNames}

			s, replies, err := sm.handlePacket(id, &unsubPkt)
			require.Nil(t, err)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.UNSUBACK, reply.Type())

			unsubAckPkt := reply.(*packet.UnsubAck)
			assert.Equal(t, unsubPkt.PacketID, unsubAckPkt.PacketID)
			assert.Equal(t, unsubPkt.Version, unsubAckPkt.Version)
			assert.Empty(t, unsubAckPkt.ReasonCodes)

			require.NotNil(t, s)
			assert.Len(t, s.subscriptions, 1)
			ps.AssertExpectations(t)
		})
	}
}

func TestSessionManagerUnsubscribeV5MultipleTopics(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)
	id := clientID("a")

	connPkt := &packet.Connect{ClientID: []byte(id), Version: packet.MQTT50}

	s, _, err := sm.handlePacket("", connPkt)
	require.Nil(t, err)

	s.subscriptions["data/0"] = subscription{id: 1, clientID: id,
		topicFilter: "data/0", qos: packet.QoS0}
	s.subscriptions["data/1"] = subscription{id: 3, clientID: id,
		topicFilter: "data/1", qos: packet.QoS1}
	s.subscriptions["data/#"] = subscription{id: 3, clientID: id,
		topicFilter: "data/#", qos: packet.QoS2}

	ps := sm.pubSub.(*pubSubMock)
	ps.On("unsubscribe", id, "data/0").Return(nil).
		On("unsubscribe", id, "data/#").Return(nil)

	topicNames := []string{"data/0", "data/#"}
	unsubPkt := packet.Unsubscribe{PacketID: 1, Version: packet.MQTT50,
		Topics: topicNames}

	s, replies, err := sm.handlePacket(id, &unsubPkt)
	require.Nil(t, err)

	require.Len(t, replies, 1)
	reply := replies[0]
	require.Equal(t, packet.UNSUBACK, reply.Type())

	unsubAckPkt := reply.(*packet.UnsubAck)
	assert.Equal(t, unsubPkt.PacketID, unsubAckPkt.PacketID)
	assert.Equal(t, unsubPkt.Version, unsubAckPkt.Version)

	codes := []packet.ReasonCode{packet.ReasonCodeV5Success,
		packet.ReasonCodeV5Success}
	assert.Equal(t, codes, unsubAckPkt.ReasonCodes)

	require.NotNil(t, s)
	assert.Len(t, s.subscriptions, 1)
	ps.AssertExpectations(t)
}

func TestSessionManagerPublishQoS0(t *testing.T) {
	testCases := []struct {
		topic   string
		version packet.MQTTVersion
		payload string
	}{
		{topic: "data/temp/1", version: packet.MQTT31, payload: "1"},
		{topic: "data/temp/2", version: packet.MQTT311, payload: "2"},
		{topic: "data/temp/3", version: packet.MQTT50, payload: "3"},
	}

	for _, tc := range testCases {
		t.Run(tc.topic, func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			connPkt := &packet.Connect{ClientID: []byte(id),
				Version: tc.version}

			_, _, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)

			pubPkt := packet.NewPublish(1, tc.version, tc.topic, packet.QoS0,
				0, 0, []byte(tc.payload), nil)

			msg := &message{id: 10, packetID: pubPkt.PacketID,
				packet: &pubPkt}

			idGen := sm.idGen.(*idGeneratorMock)
			idGen.On("NextID").Return(int(msg.id))

			ps := sm.pubSub.(*pubSubMock)
			ps.On("publish", msg)

			_, replies, err := sm.handlePacket(id, &pubPkt)
			require.Nil(t, err)
			require.Empty(t, replies)
			ps.AssertExpectations(t)
		})
	}
}

func TestSessionManagerPublishQoS1(t *testing.T) {
	testCases := []struct {
		topic   string
		version packet.MQTTVersion
		payload string
	}{
		{topic: "data/temp/1", version: packet.MQTT31, payload: "1"},
		{topic: "data/temp/2", version: packet.MQTT311, payload: "2"},
		{topic: "data/temp/3", version: packet.MQTT50, payload: "3"},
	}

	for _, tc := range testCases {
		t.Run(tc.topic, func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			connPkt := &packet.Connect{ClientID: []byte(id),
				Version: tc.version}

			_, _, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)

			pubPkt := packet.NewPublish(1, tc.version, tc.topic, packet.QoS1,
				0, 0, []byte(tc.payload), nil)

			msg := &message{id: 10, packetID: pubPkt.PacketID,
				packet: &pubPkt}

			idGen := sm.idGen.(*idGeneratorMock)
			idGen.On("NextID").Return(int(msg.id))

			ps := sm.pubSub.(*pubSubMock)
			ps.On("publish", msg)

			_, replies, err := sm.handlePacket(id, &pubPkt)
			require.Nil(t, err)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.PUBACK, reply.Type())

			pubAckPkt := reply.(*packet.PubAck)
			assert.Equal(t, packet.ID(1), pubAckPkt.PacketID)
			assert.Equal(t, tc.version, pubAckPkt.Version)
			assert.Equal(t, packet.ReasonCodeV5Success, pubAckPkt.ReasonCode)
			ps.AssertExpectations(t)
		})
	}
}

func TestSessionManagerPublishQoS2(t *testing.T) {
	testCases := []struct {
		topic   string
		version packet.MQTTVersion
		payload string
	}{
		{topic: "data/temp/1", version: packet.MQTT31, payload: "1"},
		{topic: "data/temp/2", version: packet.MQTT311, payload: "2"},
		{topic: "data/temp/3", version: packet.MQTT50, payload: "3"},
	}

	for _, tc := range testCases {
		t.Run(tc.topic, func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			connPkt := &packet.Connect{ClientID: []byte(id),
				Version: tc.version}

			_, _, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)

			pubPkt := packet.NewPublish(1, tc.version, tc.topic, packet.QoS2,
				0, 0, []byte(tc.payload), nil)

			msg := &message{id: 10, packetID: pubPkt.PacketID, packet: &pubPkt}

			idGen := sm.idGen.(*idGeneratorMock)
			idGen.On("NextID").Return(int(msg.id))

			s, replies, err := sm.handlePacket(id, &pubPkt)
			require.Nil(t, err)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.PUBREC, reply.Type())

			pubRecPkt := reply.(*packet.PubRec)
			assert.Equal(t, packet.ID(1), pubRecPkt.PacketID)
			assert.Equal(t, tc.version, pubRecPkt.Version)
			assert.Equal(t, packet.ReasonCodeV5Success, pubRecPkt.ReasonCode)

			require.NotNil(t, s)
			require.Len(t, s.unAckPubMessages, 1)

			unAckMsg, ok := s.unAckPubMessages[msg.packetID]
			require.True(t, ok)
			assert.Equal(t, msg, unAckMsg)
		})
	}
}

func TestSessionManagerPublishQoS2NoDuplication(t *testing.T) {
	testCases := []struct {
		topic   string
		version packet.MQTTVersion
		payload string
	}{
		{topic: "data/temp/1", version: packet.MQTT31, payload: "1"},
		{topic: "data/temp/2", version: packet.MQTT311, payload: "2"},
		{topic: "data/temp/3", version: packet.MQTT50, payload: "3"},
	}

	for _, tc := range testCases {
		t.Run(tc.topic, func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			connPkt := &packet.Connect{ClientID: []byte(id),
				Version: tc.version}

			_, _, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)

			pubPkt1 := packet.NewPublish(1, tc.version, tc.topic,
				packet.QoS2, 0, 0, []byte(tc.payload), nil)

			msg1 := &message{id: 10, packetID: pubPkt1.PacketID,
				packet: &pubPkt1}

			pubPkt2 := packet.NewPublish(2, tc.version, tc.topic,
				packet.QoS2, 0, 0, []byte(tc.payload), nil)

			msg2 := &message{id: 11, packetID: pubPkt2.PacketID,
				packet: &pubPkt2}

			idGen := sm.idGen.(*idGeneratorMock)
			idGen.On("NextID").Return(int(msg1.id)).Once()
			idGen.On("NextID").Return(int(msg2.id)).Once()
			idGen.On("NextID").Return(int(msg2.id + 1)).Once()

			_, replies, err := sm.handlePacket(id, &pubPkt1)
			require.Nil(t, err)
			pubRecPkt := replies[0].(*packet.PubRec)
			assert.Equal(t, msg1.packetID, pubRecPkt.PacketID)
			assert.Equal(t, packet.ReasonCodeV5Success, pubRecPkt.ReasonCode)

			_, replies, err = sm.handlePacket(id, &pubPkt2)
			require.Nil(t, err)
			pubRecPkt = replies[0].(*packet.PubRec)
			assert.Equal(t, msg2.packetID, pubRecPkt.PacketID)
			assert.Equal(t, packet.ReasonCodeV5Success, pubRecPkt.ReasonCode)

			s, replies, err := sm.handlePacket(id, &pubPkt1)
			require.Nil(t, err)
			pubRecPkt = replies[0].(*packet.PubRec)
			assert.Equal(t, msg1.packetID, pubRecPkt.PacketID)
			assert.Equal(t, packet.ReasonCodeV5Success, pubRecPkt.ReasonCode)

			require.NotNil(t, s)
			require.Len(t, s.unAckPubMessages, 2)

			unAckMsg1, ok := s.unAckPubMessages[msg1.packetID]
			require.True(t, ok)
			assert.Equal(t, msg1, unAckMsg1)

			unAckMsg2, ok := s.unAckPubMessages[msg2.packetID]
			require.True(t, ok)
			assert.Equal(t, msg2, unAckMsg2)
		})
	}
}

func TestSessionManagerPublishQoS2SamePacketIDNewMessage(t *testing.T) {
	testCases := []struct {
		topic   string
		version packet.MQTTVersion
		payload string
	}{
		{topic: "data/temp/1", version: packet.MQTT31, payload: "1"},
		{topic: "data/temp/2", version: packet.MQTT311, payload: "2"},
		{topic: "data/temp/3", version: packet.MQTT50, payload: "3"},
	}

	for _, tc := range testCases {
		t.Run(tc.topic, func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			connPkt := &packet.Connect{ClientID: []byte(id),
				Version: tc.version}

			s, _, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)

			pubPkt := packet.NewPublish(10, tc.version, tc.topic,
				packet.QoS2, 0, 0, []byte(tc.payload), nil)

			msg1 := &message{id: 10, packetID: pubPkt.PacketID, tries: 1,
				lastSent: time.Now().UnixMicro()}
			s.unAckPubMessages[msg1.packetID] = msg1

			msg2 := &message{id: 11, packetID: pubPkt.PacketID, packet: &pubPkt}

			idGen := sm.idGen.(*idGeneratorMock)
			idGen.On("NextID").Return(int(msg2.id))

			_, replies, err := sm.handlePacket(id, &pubPkt)
			require.Nil(t, err)
			pubRecPkt := replies[0].(*packet.PubRec)
			assert.Equal(t, msg2.packetID, pubRecPkt.PacketID)
			assert.Equal(t, packet.ReasonCodeV5Success, pubRecPkt.ReasonCode)

			require.NotNil(t, s)
			require.Len(t, s.unAckPubMessages, 1)

			unAckMsg, ok := s.unAckPubMessages[msg2.packetID]
			require.True(t, ok)
			assert.Equal(t, msg2, unAckMsg)
		})
	}
}

func TestSessionManagerPublishReadSessionError(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			pubPkt := packet.NewPublish(1, tc, "data", packet.QoS0,
				0, 0, []byte("data"), nil)

			s, replies, err := sm.handlePacket(id, &pubPkt)
			assert.Nil(t, s)
			assert.Empty(t, replies)
			assert.NotNil(t, err)
		})
	}
}

func TestSessionManagerPubAck(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			connPkt := packet.Connect{ClientID: []byte(id), Version: tc}

			s, _, err := sm.handlePacket("", &connPkt)
			require.Nil(t, err)

			pubPkt := packet.NewPublish(1, tc, "data", packet.QoS1,
				0, 0, nil, nil)

			msg := &message{id: 1, packetID: 1, packet: &pubPkt, tries: 1,
				lastSent: time.Now().UnixMicro()}
			s.inflightMessages.PushBack(msg)

			pubAckPkt := packet.NewPubAck(pubPkt.PacketID, pubPkt.Version,
				packet.ReasonCodeV5Success, nil)

			s, replies, err := sm.handlePacket(id, &pubAckPkt)
			require.Nil(t, err)
			assert.Empty(t, replies)

			require.NotNil(t, s)
			assert.Zero(t, s.inflightMessages.Len())

			s2 := sm.store.sessions[s.clientID]
			assert.Zero(t, s2.inflightMessages.Len())
		})
	}
}

func TestSessionManagerPubAckUnknownMessage(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)
	id := clientID("a")

	connPkt := &packet.Connect{ClientID: []byte(id), Version: packet.MQTT311}

	_, _, err := sm.handlePacket("", connPkt)
	require.Nil(t, err)

	pubAckPkt := packet.NewPubAck(10, packet.MQTT311,
		packet.ReasonCodeV5Success, nil)

	_, replies, err := sm.handlePacket(id, &pubAckPkt)
	require.NotNil(t, err)
	require.Empty(t, replies)
}

func TestSessionManagerPubAckReadSessionError(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			pubAckPkt := packet.NewPubAck(1, tc, packet.ReasonCodeV5Success,
				nil)

			s, replies, err := sm.handlePacket(id, &pubAckPkt)
			assert.Nil(t, s)
			assert.Empty(t, replies)
			assert.NotNil(t, err)
		})
	}
}

func TestSessionManagerPubRec(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			connPkt := packet.Connect{ClientID: []byte(id), Version: tc}

			s, _, err := sm.handlePacket("", &connPkt)
			require.Nil(t, err)

			pubPkt := packet.NewPublish(1, tc, "topic",
				packet.QoS2, 0, 0, []byte("data"), nil)

			msg := &message{packetID: pubPkt.PacketID, packet: &pubPkt,
				tries: 1, lastSent: time.Now().UnixMicro()}
			s.inflightMessages.PushBack(msg)

			pubRecPkt := packet.NewPubRec(pubPkt.PacketID, pubPkt.Version,
				packet.ReasonCodeV5Success, nil)

			s, replies, err := sm.handlePacket(id, &pubRecPkt)
			require.Nil(t, err)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.PUBREL, reply.Type())

			pubRelPkt := reply.(*packet.PubRel)
			assert.Equal(t, pubPkt.PacketID, pubRelPkt.PacketID)
			assert.Equal(t, tc, pubRelPkt.Version)
			assert.Equal(t, packet.ReasonCodeV5Success, pubRelPkt.ReasonCode)

			require.NotNil(t, s)
			require.Equal(t, 1, s.inflightMessages.Len())

			inflightMsg := s.inflightMessages.Front().Value.(*message)
			assert.Equal(t, msg.id, inflightMsg.id)
			assert.Equal(t, msg.packetID, inflightMsg.packetID)
			assert.Nil(t, inflightMsg.packet)
			assert.Equal(t, msg.tries, inflightMsg.tries)
			assert.Equal(t, msg.lastSent, inflightMsg.lastSent)
		})
	}
}

func TestSessionManagerPubRecDuplicatePacketNoError(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			connPkt := packet.Connect{ClientID: []byte(id), Version: tc}

			s, _, err := sm.handlePacket("", &connPkt)
			require.Nil(t, err)

			pubPkt := packet.NewPublish(1, tc, "topic",
				packet.QoS2, 0, 0, []byte("data"), nil)

			msg := &message{packetID: pubPkt.PacketID, packet: &pubPkt,
				tries: 1, lastSent: time.Now().UnixMicro()}
			s.inflightMessages.PushBack(msg)

			pubRecPkt := packet.NewPubRec(pubPkt.PacketID, pubPkt.Version,
				packet.ReasonCodeV5Success, nil)

			_, _, err = sm.handlePacket(id, &pubRecPkt)
			require.Nil(t, err)

			s, replies, err := sm.handlePacket(id, &pubRecPkt)
			require.Nil(t, err)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.PUBREL, reply.Type())

			pubRelPkt := reply.(*packet.PubRel)
			assert.Equal(t, pubPkt.PacketID, pubRelPkt.PacketID)
			assert.Equal(t, tc, pubRelPkt.Version)
			assert.Equal(t, packet.ReasonCodeV5Success, pubRelPkt.ReasonCode)

			require.NotNil(t, s)
			require.Equal(t, 1, s.inflightMessages.Len())

			inflightMsg := s.inflightMessages.Front().Value.(*message)
			assert.Equal(t, msg.id, inflightMsg.id)
			assert.Equal(t, msg.packetID, inflightMsg.packetID)
			assert.Nil(t, inflightMsg.packet)
			assert.Equal(t, msg.tries, inflightMsg.tries)
			assert.Equal(t, msg.lastSent, inflightMsg.lastSent)
		})
	}
}

func TestSessionManagerPubRecV3PacketIDNotFound(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			connPkt := packet.Connect{ClientID: []byte(id), Version: tc}

			s, _, err := sm.handlePacket("", &connPkt)
			require.Nil(t, err)

			pubPkt := packet.NewPublish(1, tc, "topic",
				packet.QoS2, 0, 0, []byte("data"), nil)

			msg := &message{packetID: pubPkt.PacketID, packet: &pubPkt,
				tries: 1, lastSent: time.Now().UnixMicro()}
			s.inflightMessages.PushBack(msg)

			pubRecPkt := packet.NewPubRec(2, tc, packet.ReasonCodeV5Success,
				nil)

			s, replies, err := sm.handlePacket(id, &pubRecPkt)
			assert.NotNil(t, err)
			assert.Len(t, replies, 0)

			require.NotNil(t, s)
			require.Equal(t, 1, s.inflightMessages.Len())

			inflightMsg := s.inflightMessages.Front().Value.(*message)
			assert.Equal(t, msg, inflightMsg)
		})
	}
}

func TestSessionManagerPubRecV5PacketIDNotFound(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)
	id := clientID("a")

	connPkt := packet.Connect{ClientID: []byte(id), Version: packet.MQTT50}

	s, _, err := sm.handlePacket("", &connPkt)
	require.Nil(t, err)

	pubPkt := packet.NewPublish(1, packet.MQTT50, "topic",
		packet.QoS2, 0, 0, []byte("data"), nil)

	msg := &message{packetID: pubPkt.PacketID, packet: &pubPkt, tries: 1,
		lastSent: time.Now().UnixMicro()}
	s.inflightMessages.PushBack(msg)

	pubRecPkt := packet.NewPubRec(2, packet.MQTT50, packet.ReasonCodeV5Success,
		nil)

	s, replies, err := sm.handlePacket(id, &pubRecPkt)
	require.Nil(t, err)

	require.Len(t, replies, 1)
	reply := replies[0]
	require.Equal(t, packet.PUBREL, reply.Type())

	pubRelPkt := reply.(*packet.PubRel)
	assert.Equal(t, pubRecPkt.PacketID, pubRelPkt.PacketID)
	assert.Equal(t, packet.MQTT50, pubRelPkt.Version)
	assert.Equal(t, packet.ReasonCodeV5PacketIDNotFound, pubRelPkt.ReasonCode)

	require.NotNil(t, s)
	require.Equal(t, 1, s.inflightMessages.Len())

	inflightMsg := s.inflightMessages.Front().Value.(*message)
	assert.Equal(t, msg, inflightMsg)
}

func TestSessionManagerPubRecReadSessionError(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			pubRecPkt := packet.NewPubRec(1, tc, packet.ReasonCodeV5Success,
				nil)

			s, replies, err := sm.handlePacket(id, &pubRecPkt)
			assert.Nil(t, s)
			assert.Empty(t, replies)
			assert.NotNil(t, err)
		})
	}
}

func TestSessionManagerPubRel(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			connPkt := packet.Connect{ClientID: []byte(id), Version: tc}

			s, _, err := sm.handlePacket("", &connPkt)
			require.Nil(t, err)

			pubPkt := packet.NewPublish(1, tc, "topic",
				packet.QoS2, 0, 0, []byte("data"), nil)

			msg := &message{packetID: pubPkt.PacketID, packet: &pubPkt}
			s.unAckPubMessages[msg.packetID] = msg.clone()

			ps := sm.pubSub.(*pubSubMock)
			ps.On("publish", msg)

			pubRelPkt := packet.NewPubRel(pubPkt.PacketID, tc,
				packet.ReasonCodeV5Success, nil)

			s, replies, err := sm.handlePacket(id, &pubRelPkt)
			require.Nil(t, err)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.PUBCOMP, reply.Type())

			pubCompPkt := reply.(*packet.PubComp)
			assert.Equal(t, pubPkt.PacketID, pubCompPkt.PacketID)
			assert.Equal(t, tc, pubCompPkt.Version)
			assert.Equal(t, packet.ReasonCodeV5Success, pubCompPkt.ReasonCode)

			require.NotNil(t, s)
			require.Len(t, s.unAckPubMessages, 1)

			unAckMsg, ok := s.unAckPubMessages[msg.packetID]
			require.True(t, ok)
			assert.Equal(t, msg.packetID, unAckMsg.packetID)
			assert.Nil(t, unAckMsg.packet)
			assert.NotZero(t, unAckMsg.lastSent)
			assert.Equal(t, 1, unAckMsg.tries)
			ps.AssertExpectations(t)
		})
	}
}

func TestSessionManagerPubRelDuplicatePacket(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			connPkt := packet.Connect{ClientID: []byte(id), Version: tc}

			s, _, err := sm.handlePacket("", &connPkt)
			require.Nil(t, err)

			pubPkt := packet.NewPublish(1, tc, "topic",
				packet.QoS2, 0, 0, []byte("data"), nil)

			msg := &message{packetID: pubPkt.PacketID, packet: &pubPkt}
			s.unAckPubMessages[msg.packetID] = msg.clone()

			ps := sm.pubSub.(*pubSubMock)
			ps.On("publish", msg).Once()

			pubRelPkt := packet.NewPubRel(pubPkt.PacketID, tc,
				packet.ReasonCodeV5Success, nil)

			_, _, err = sm.handlePacket(id, &pubRelPkt)
			require.Nil(t, err)

			s, replies, err := sm.handlePacket(id, &pubRelPkt)
			require.Nil(t, err)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.PUBCOMP, reply.Type())

			pubCompPkt := reply.(*packet.PubComp)
			assert.Equal(t, pubPkt.PacketID, pubCompPkt.PacketID)
			assert.Equal(t, tc, pubCompPkt.Version)
			assert.Equal(t, packet.ReasonCodeV5Success, pubCompPkt.ReasonCode)

			require.NotNil(t, s)
			require.Len(t, s.unAckPubMessages, 1)

			unAckMsg, ok := s.unAckPubMessages[msg.packetID]
			require.True(t, ok)
			assert.Equal(t, msg.packetID, unAckMsg.packetID)
			assert.Nil(t, unAckMsg.packet)
			assert.NotZero(t, unAckMsg.lastSent)
			assert.Equal(t, 1, unAckMsg.tries)
			ps.AssertExpectations(t)
		})
	}
}

func TestSessionManagerPubRelV3PacketNotFound(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			connPkt := packet.Connect{ClientID: []byte(id), Version: tc}

			s, _, err := sm.handlePacket("", &connPkt)
			require.Nil(t, err)

			pubPkt := packet.NewPublish(1, tc, "topic",
				packet.QoS2, 0, 0, []byte("data"), nil)

			msg := &message{packetID: pubPkt.PacketID, packet: &pubPkt}
			s.unAckPubMessages[msg.packetID] = msg

			pubRelPkt := packet.NewPubRel(10, tc, packet.ReasonCodeV5Success,
				nil)

			s, replies, err := sm.handlePacket(id, &pubRelPkt)
			assert.NotNil(t, err)
			assert.Len(t, replies, 0)

			require.NotNil(t, s)
			require.Len(t, s.unAckPubMessages, 1)

			unAckMsg, ok := s.unAckPubMessages[msg.packetID]
			require.True(t, ok)
			assert.Equal(t, msg.packetID, unAckMsg.packetID)
			assert.Same(t, &pubPkt, unAckMsg.packet)
			assert.Zero(t, unAckMsg.lastSent)
			assert.Zero(t, unAckMsg.tries)
		})
	}
}

func TestSessionManagerPubRelV5PacketNotFound(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)
	id := clientID("a")

	connPkt := packet.Connect{ClientID: []byte(id), Version: packet.MQTT50}

	s, _, err := sm.handlePacket("", &connPkt)
	require.Nil(t, err)

	pubPkt := packet.NewPublish(1, packet.MQTT50, "topic",
		packet.QoS2, 0, 0, []byte("data"), nil)

	msg := &message{packetID: pubPkt.PacketID, packet: &pubPkt}
	s.unAckPubMessages[msg.packetID] = msg

	pubRelPkt := packet.NewPubRel(10, packet.MQTT50, packet.ReasonCodeV5Success,
		nil)

	s, replies, err := sm.handlePacket(id, &pubRelPkt)
	require.Nil(t, err)

	require.Len(t, replies, 1)
	reply := replies[0]
	require.Equal(t, packet.PUBCOMP, reply.Type())

	pubCompPkt := reply.(*packet.PubComp)
	assert.Equal(t, pubRelPkt.PacketID, pubCompPkt.PacketID)
	assert.Equal(t, packet.MQTT50, pubCompPkt.Version)
	assert.Equal(t, packet.ReasonCodeV5PacketIDNotFound, pubCompPkt.ReasonCode)

	require.NotNil(t, s)
	require.Len(t, s.unAckPubMessages, 1)

	unAckMsg, ok := s.unAckPubMessages[msg.packetID]
	require.True(t, ok)
	assert.Equal(t, msg.packetID, unAckMsg.packetID)
	assert.Same(t, &pubPkt, unAckMsg.packet)
	assert.Zero(t, unAckMsg.lastSent)
	assert.Zero(t, unAckMsg.tries)
}

func TestSessionManagerPubRelReadSessionError(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			pubRelPkt := packet.NewPubRel(1, tc, packet.ReasonCodeV5Success,
				nil)

			s, replies, err := sm.handlePacket(id, &pubRelPkt)
			assert.Nil(t, s)
			assert.Empty(t, replies)
			assert.NotNil(t, err)
		})
	}
}

func TestSessionManagerPubComp(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			connPkt := &packet.Connect{ClientID: []byte(id), Version: tc}

			s, _, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)

			msg := &message{packetID: 1, tries: 1,
				lastSent: time.Now().UnixMicro()}
			s.inflightMessages.PushBack(msg)

			pubCompPkt := packet.NewPubComp(msg.packetID, tc,
				packet.ReasonCodeV5Success, nil)

			s, replies, err := sm.handlePacket(id, &pubCompPkt)
			require.Nil(t, err)
			require.Empty(t, replies)

			require.NotNil(t, s)
			require.Equal(t, 0, s.inflightMessages.Len())
		})
	}
}

func TestSessionManagerPubCompPacketNotFound(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			connPkt := &packet.Connect{ClientID: []byte(id), Version: tc}

			s, _, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)

			msg := &message{packetID: 1, tries: 1,
				lastSent: time.Now().UnixMicro()}
			s.inflightMessages.PushBack(msg)

			pubCompPkt := packet.NewPubComp(2, tc, packet.ReasonCodeV5Success,
				nil)

			s, replies, err := sm.handlePacket(id, &pubCompPkt)
			require.NotNil(t, err)
			require.Empty(t, replies)

			require.NotNil(t, s)
			require.Equal(t, 1, s.inflightMessages.Len())
			assert.Equal(t, msg, s.inflightMessages.Front().Value)
		})
	}
}

func TestSessionManagerPubCompReadSessionError(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			pubCompPkt := packet.NewPubComp(1, tc, packet.ReasonCodeV5Success,
				nil)

			s, replies, err := sm.handlePacket(id, &pubCompPkt)
			assert.Nil(t, s)
			assert.Empty(t, replies)
			assert.NotNil(t, err)
		})
	}
}

func TestSessionManagerDisconnect(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			connPkt := &packet.Connect{ClientID: []byte(id), Version: tc}

			s, _, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)

			sub := subscription{id: 1, topicFilter: "test", clientID: id}
			s.subscriptions["test"] = sub

			discPkt := packet.NewDisconnect(tc, packet.ReasonCodeV5Success,
				nil)

			_, replies, err := sm.handlePacket(id, &discPkt)
			assert.Nil(t, err)
			require.Empty(t, replies)

			require.NotNil(t, s)
			assert.False(t, s.connected)
			assert.NotEmpty(t, s.subscriptions)

			_, ok := sm.sessions[id]
			assert.False(t, ok)
		})
	}
}

func TestSessionManagerDisconnectCleanSession(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			connPkt := &packet.Connect{ClientID: []byte(id), Version: tc,
				CleanSession: true}

			s, _, err := sm.handlePacket("", connPkt)
			require.Nil(t, err)

			sub := subscription{id: 1, topicFilter: "test", clientID: id}
			s.subscriptions["test"] = sub

			ps := sm.pubSub.(*pubSubMock)
			ps.On("unsubscribe", id, "test").Return(nil)

			discPkt := packet.NewDisconnect(tc, packet.ReasonCodeV5Success,
				nil)

			s, replies, err := sm.handlePacket(id, &discPkt)
			assert.Nil(t, err)
			require.Empty(t, replies)

			require.NotNil(t, s)
			assert.False(t, s.connected)
			assert.Empty(t, s.subscriptions)

			_, ok := sm.sessions[id]
			assert.False(t, ok)
			ps.AssertExpectations(t)
		})
	}
}

func TestSessionManagerDisconnectExpiryInterval(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)
	id := clientID("a")

	props := &packet.Properties{}
	props.SessionExpiryInterval = new(uint32)
	*props.SessionExpiryInterval = 600

	connPkt := &packet.Connect{ClientID: []byte(id), Version: packet.MQTT50,
		CleanSession: true, Properties: props}

	s, _, err := sm.handlePacket("", connPkt)
	require.Nil(t, err)

	props.SessionExpiryInterval = new(uint32)
	*props.SessionExpiryInterval = 300
	require.Equal(t, uint32(600), s.expiryInterval)

	discPkt := packet.NewDisconnect(packet.MQTT50, packet.ReasonCodeV5Success,
		props)

	_, replies, err := sm.handlePacket(id, &discPkt)
	assert.Nil(t, err)
	assert.Empty(t, replies)
	assert.Equal(t, uint32(300), s.expiryInterval)
}

func TestSessionManagerDisconnectInvalidExpiryInterval(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)
	id := clientID("a")

	connPkt := &packet.Connect{ClientID: []byte(id), Version: packet.MQTT50}

	_, _, err := sm.handlePacket("", connPkt)
	require.Nil(t, err)

	props := &packet.Properties{}
	props.SessionExpiryInterval = new(uint32)
	*props.SessionExpiryInterval = 300

	discPkt := packet.NewDisconnect(packet.MQTT50, packet.ReasonCodeV5Success,
		props)

	_, replies, err := sm.handlePacket(id, &discPkt)
	require.NotNil(t, err)

	require.Len(t, replies, 1)
	reply := replies[0]
	assert.Equal(t, packet.DISCONNECT, reply.Type())

	discReplyPkt := reply.(*packet.Disconnect)
	assert.Equal(t, packet.MQTT50, discReplyPkt.Version)
	assert.Equal(t, packet.ReasonCodeV5ProtocolError, discReplyPkt.ReasonCode)
}

func TestSessionManagerDisconnectReadSessionError(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := clientID("a")

			discPkt := packet.NewDisconnect(tc, packet.ReasonCodeV5Success,
				nil)

			s, replies, err := sm.handlePacket(id, &discPkt)
			assert.Nil(t, s)
			assert.Empty(t, replies)
			assert.NotNil(t, err)
		})
	}
}

func TestSessionManagerDisconnectErrorOnSaveSessionIsOkay(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)
	id := clientID("a")

	props := &packet.Properties{}
	props.SessionExpiryInterval = new(uint32)
	*props.SessionExpiryInterval = 600

	connPkt := &packet.Connect{ClientID: []byte(id), Version: packet.MQTT50,
		CleanSession: true, Properties: props}

	s, _, err := sm.handlePacket("", connPkt)
	require.Nil(t, err)

	props.SessionExpiryInterval = new(uint32)
	*props.SessionExpiryInterval = 300
	require.Equal(t, uint32(600), s.expiryInterval)

	discPkt := packet.NewDisconnect(packet.MQTT50, packet.ReasonCodeV5Success,
		props)

	_, replies, err := sm.handlePacket(id, &discPkt)
	assert.Nil(t, err)
	assert.Empty(t, replies)
	assert.Equal(t, uint32(300), s.expiryInterval)
}

func TestSessionManagerDisconnectFailedToDeleteSession(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)
	id := clientID("a")

	connPkt := &packet.Connect{ClientID: []byte(id), Version: packet.MQTT311,
		CleanSession: true}

	_, _, err := sm.handlePacket("", connPkt)
	require.Nil(t, err)

	discPkt := packet.NewDisconnect(packet.MQTT311, packet.ReasonCodeV5Success,
		nil)

	_, replies, err := sm.handlePacket(id, &discPkt)
	assert.Nil(t, err)
	assert.Empty(t, replies)
}

func TestSessionManagerInvalidPacket(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)
	id := clientID("a")

	connPkt := &packet.Connect{ClientID: []byte(id), Version: packet.MQTT311}

	_, _, err := sm.handlePacket("", connPkt)
	require.Nil(t, err)

	pingReqPkt := packet.NewPingResp()

	_, replies, err := sm.handlePacket(id, &pingReqPkt)
	assert.NotNil(t, err)
	assert.Nil(t, replies)
}

func TestSessionManagerPublishMessageQoS0(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)

			s := &session{}
			s.clientID = "client-1"
			s.version = tc
			s.connected = true
			sm.store.sessions[s.clientID] = s

			pubPkt := packet.NewPublish(1, tc, "data", packet.QoS0,
				0, 0, nil, nil)
			msg := &message{id: 1, packet: &pubPkt}

			pktDeliverer := sm.deliverer.(*delivererMock)
			pktDeliverer.On("deliverPacket", s.clientID, mock.Anything).
				Return(nil)

			err := sm.publishMessage(s.clientID, msg)
			assert.Nil(t, err)
			pub := pktDeliverer.Calls[0].Arguments.Get(1).(*packet.Publish)

			assert.Zero(t, s.inflightMessages.Len())
			assert.Equal(t, &pubPkt, pub)
			pktDeliverer.AssertExpectations(t)
		})
	}
}

func TestSessionManagerPublishMessageQoS1(t *testing.T) {
	testCases := []packet.MQTTVersion{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)

			s := &session{}
			s.clientID = "client-1"
			s.version = tc
			s.connected = true
			sm.store.sessions[s.clientID] = s

			pubPkt := packet.NewPublish(1, tc, "data", packet.QoS1,
				0, 0, nil, nil)
			msg := &message{id: 1, packet: &pubPkt}

			pktDeliverer := sm.deliverer.(*delivererMock)
			pktDeliverer.On("deliverPacket", s.clientID, mock.Anything).
				Return(nil)

			err := sm.publishMessage(s.clientID, msg)
			assert.Nil(t, err)
			pub := pktDeliverer.Calls[0].Arguments.Get(1).(*packet.Publish)

			assert.Equal(t, 1, s.inflightMessages.Len())
			firstMsg := s.inflightMessages.Front().Value.(*message)
			require.NotNil(t, firstMsg)
			assert.Equal(t, messageID(1), firstMsg.id)
			assert.Equal(t, packet.ID(s.lastPacketID), firstMsg.packetID)
			assert.Equal(t, msg.packet.QoS, firstMsg.packet.QoS)
			assert.Equal(t, msg.packet.Version, firstMsg.packet.Version)
			assert.Equal(t, msg.packet.TopicName, firstMsg.packet.TopicName)
			assert.Equal(t, msg.packet.Payload, firstMsg.packet.Payload)
			assert.NotZero(t, firstMsg.lastSent)
			assert.Equal(t, 1, firstMsg.tries)
			assert.Equal(t, firstMsg.packet, pub)

			s2 := sm.store.sessions[s.clientID]
			assert.Equal(t, 1, s2.inflightMessages.Len())
			m := s2.inflightMessages.Front().Value.(*message)
			assert.Equal(t, firstMsg, m)
			pktDeliverer.AssertExpectations(t)
		})
	}
}

func TestSessionManagerPublishMessageDifferentVersion(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)

	s := &session{}
	s.clientID = "client-1"
	s.version = packet.MQTT50
	s.connected = true
	sm.store.sessions[s.clientID] = s

	pubPkt := packet.NewPublish(10, packet.MQTT311, "data",
		packet.QoS0, 0, 0, nil, nil)
	msg := message{id: 50, packet: &pubPkt}

	pktDeliverer := sm.deliverer.(*delivererMock)
	pktDeliverer.On("deliverPacket", s.clientID, &pubPkt).
		Return(nil)

	err := sm.publishMessage(s.clientID, &msg)
	assert.Nil(t, err)

	pubPktDelivered := pktDeliverer.Calls[0].Arguments.Get(1).(*packet.Publish)
	assert.Equal(t, pubPkt.PacketID, pubPktDelivered.PacketID)
	assert.Equal(t, pubPkt.QoS, pubPktDelivered.QoS)
	assert.Equal(t, pubPkt.TopicName, pubPktDelivered.TopicName)
	assert.Equal(t, s.version, pubPktDelivered.Version)
	pktDeliverer.AssertExpectations(t)
}

func TestSessionManagerPublishMessageDisconnectedSession(t *testing.T) {
	testCases := []packet.QoS{
		packet.QoS0,
		packet.QoS1,
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)

			s := &session{}
			s.clientID = "client-1"
			s.connected = false
			sm.store.sessions[s.clientID] = s

			pubPkt := packet.NewPublish(1, packet.MQTT311, "data",
				tc, 0, 0, nil, nil)
			msg := message{id: 1, packet: &pubPkt}

			err := sm.publishMessage(s.clientID, &msg)
			assert.Nil(t, err)

			pktDeliverer := sm.deliverer.(*delivererMock)
			assert.Len(t, pktDeliverer.Calls, 0)
			pktDeliverer.AssertExpectations(t)
		})
	}
}

func TestSessionManagerPublishMessageError(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)

	s := &session{}
	s.clientID = "client-1"
	s.version = packet.MQTT311
	s.connected = true
	sm.store.sessions[s.clientID] = s

	pubPkt := packet.NewPublish(10, packet.MQTT311, "data",
		packet.QoS0, 0, 0, nil, nil)
	msg := message{id: 50, packet: &pubPkt}

	pktDeliverer := sm.deliverer.(*delivererMock)
	pktDeliverer.On("deliverPacket", s.clientID, &pubPkt).
		Return(errors.New("failed to deliver message"))

	err := sm.publishMessage(s.clientID, &msg)
	assert.NotNil(t, err)
	pktDeliverer.AssertExpectations(t)
}

func TestSessionManagerPublishMessageReadSessionError(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)

	pubPkt := packet.NewPublish(10, packet.MQTT311, "data",
		packet.QoS0, 0, 0, nil, nil)
	msg := message{id: 50, packet: &pubPkt}

	err := sm.publishMessage("id-0", &msg)
	assert.NotNil(t, err)
}
