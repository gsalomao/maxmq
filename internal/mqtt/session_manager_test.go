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

func (d *delivererMock) deliverPacket(id ClientID, pkt *packet.Publish) error {
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

func (ps *pubSubMock) subscribe(s *Session, t packet.Topic,
	subsID int,
) (Subscription, error) {
	args := ps.Called(s, t, subsID)
	return args.Get(0).(Subscription), args.Error(1)
}

func (ps *pubSubMock) unsubscribe(id ClientID, topic string) error {
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
			id := ClientID("a")

			pkt := &packet.Connect{ClientID: []byte(id), Version: tc}

			session, replies, err := sm.handleConnect(pkt)
			require.Nil(t, err)
			require.NotNil(t, session)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAck := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
			assert.False(t, connAck.SessionPresent)

			assert.True(t, session.connected)
			assert.Equal(t, id, session.ClientID)
			assert.NotZero(t, session.SessionID)
			assert.Equal(t, int(pkt.KeepAlive), session.KeepAlive)

			s, ok := sm.sessions[session.ClientID]
			require.True(t, ok)
			assert.Equal(t, session, s)
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
			id := ClientID("a")

			s := &Session{ClientID: id, SessionID: 1, Version: tc,
				ConnectedAt:    time.Now().Add(-1 * time.Minute).Unix(),
				ExpiryInterval: conf.MaxSessionExpiryInterval,
				Subscriptions:  make(map[string]Subscription)}

			sub := Subscription{ID: 1, TopicFilter: "test", ClientID: id}
			s.Subscriptions["test"] = sub
			sm.store.sessions[s.ClientID] = s

			pkt := &packet.Connect{ClientID: []byte(id), Version: tc}

			session, replies, err := sm.handlePacket("", pkt)
			require.Nil(t, err)
			require.NotNil(t, session)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAck := reply.(*packet.ConnAck)
			assert.Equal(t, tc, connAck.Version)
			assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
			assert.True(t, connAck.SessionPresent)

			assert.True(t, session.connected)
			assert.Equal(t, id, session.ClientID)
			assert.NotZero(t, session.SessionID)
			assert.NotEmpty(t, session.Subscriptions)
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
			id := ClientID("a")

			s := &Session{ClientID: id, SessionID: 1, Version: tc,
				ConnectedAt:    time.Now().Add(-1 * time.Minute).Unix(),
				ExpiryInterval: conf.MaxSessionExpiryInterval,
				Subscriptions:  make(map[string]Subscription)}

			sub := Subscription{ID: 1, TopicFilter: "test", ClientID: id}
			s.Subscriptions["test"] = sub
			sm.store.sessions[s.ClientID] = s

			ps := sm.pubSub.(*pubSubMock)
			ps.On("unsubscribe", id, sub.TopicFilter).Return(nil)

			pkt := &packet.Connect{ClientID: []byte(id), Version: tc,
				CleanSession: true}

			session, replies, err := sm.handlePacket("", pkt)
			require.Nil(t, err)
			require.NotNil(t, session)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAck := reply.(*packet.ConnAck)
			assert.Equal(t, tc, connAck.Version)
			assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
			assert.False(t, connAck.SessionPresent)

			assert.True(t, session.connected)
			assert.Equal(t, id, session.ClientID)
			assert.NotZero(t, session.SessionID)
			assert.Empty(t, session.Subscriptions)
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

			pkt := &packet.Connect{ClientID: tc.id, Version: tc.version}

			session, replies, err := sm.handlePacket("", pkt)
			require.NotNil(t, err)
			assert.Nil(t, session)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAck := reply.(*packet.ConnAck)
			assert.Equal(t, tc.version, connAck.Version)
			assert.Equal(t, tc.code, connAck.ReasonCode)
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

			pkt := &packet.Connect{Version: tc}

			session, replies, err := sm.handlePacket("", pkt)
			require.Nil(t, err)
			require.NotNil(t, session)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAck := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)

			assert.True(t, session.connected)
			assert.Greater(t, len(session.ClientID), 0)
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

			pkt := &packet.Connect{Version: tc.version}

			session, replies, err := sm.handlePacket("", pkt)
			require.NotNil(t, err)
			assert.Nil(t, session)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAck := reply.(*packet.ConnAck)
			assert.Equal(t, tc.code, connAck.ReasonCode)
		})
	}
}

func TestSessionManagerConnectV5AssignClientID(t *testing.T) {
	conf := newConfiguration()
	conf.AllowEmptyClientID = true
	sm := createSessionManager(conf)

	pkt := &packet.Connect{Version: packet.MQTT50}

	session, replies, err := sm.handlePacket("", pkt)
	require.Nil(t, err)
	require.NotNil(t, session)

	require.Len(t, replies, 1)
	reply := replies[0]
	require.Equal(t, packet.CONNACK, reply.Type())

	connAck := reply.(*packet.ConnAck)
	assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
	assert.NotNil(t, connAck.Properties)
	assert.NotNil(t, connAck.Properties.AssignedClientID)
	assert.Equal(t, 20, len(connAck.Properties.AssignedClientID))

	assert.True(t, session.connected)
	assert.NotZero(t, session.SessionID)
	assert.Equal(t, ClientID(connAck.Properties.AssignedClientID),
		session.ClientID)
}

func TestSessionManagerConnectV5AssignClientIDWithPrefix(t *testing.T) {
	conf := newConfiguration()
	conf.AllowEmptyClientID = true
	conf.ClientIDPrefix = []byte("MAXMQ-")
	sm := createSessionManager(conf)

	pkt := &packet.Connect{Version: packet.MQTT50}

	session, replies, err := sm.handlePacket("", pkt)
	require.Nil(t, err)
	require.NotNil(t, session)

	require.Len(t, replies, 1)
	reply := replies[0]
	require.Equal(t, packet.CONNACK, reply.Type())

	connAck := replies[0].(*packet.ConnAck)
	assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
	assert.NotNil(t, connAck.Properties)
	assert.NotNil(t, connAck.Properties.AssignedClientID)
	assert.Equal(t, 26, len(connAck.Properties.AssignedClientID))
	assert.Equal(t, conf.ClientIDPrefix,
		connAck.Properties.AssignedClientID[:len(conf.ClientIDPrefix)])

	assert.True(t, session.connected)
	assert.NotZero(t, session.SessionID)
	assert.Equal(t, ClientID(connAck.Properties.AssignedClientID),
		session.ClientID)
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
			pkt := &packet.Connect{ClientID: []byte("a"),
				Version: packet.MQTT50, Properties: props}

			session, replies, err := sm.handlePacket("", pkt)
			require.Nil(t, err)
			require.NotNil(t, session)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAck := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
			assert.NotNil(t, connAck.Properties)
			assert.True(t, session.connected)

			props = connAck.Properties
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
			pkt := &packet.Connect{ClientID: []byte("a"),
				Version: packet.MQTT50, Properties: props}

			session, replies, err := sm.handlePacket("", pkt)
			require.Nil(t, err)
			require.NotNil(t, session)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAck := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
			assert.NotNil(t, connAck.Properties)
			assert.True(t, session.connected)

			props = connAck.Properties
			assert.NotNil(t, props.SessionExpiryInterval)
			assert.Equal(t, tc.resp, *props.SessionExpiryInterval)
			assert.Equal(t, tc.resp, session.ExpiryInterval)
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

			pkt := &packet.Connect{ClientID: []byte("a"),
				Version: packet.MQTT311, KeepAlive: tc.keepAlive}

			session, replies, err := sm.handlePacket("", pkt)
			require.NotNil(t, err)
			assert.Nil(t, session)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAck := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV3IdentifierRejected,
				connAck.ReasonCode)
		})
	}
}

func TestSessionManagerConnectV3MaxKeepAliveAccepted(t *testing.T) {
	conf := newConfiguration()
	conf.MaxKeepAlive = 100
	sm := createSessionManager(conf)

	pkt := &packet.Connect{ClientID: []byte("a"), Version: packet.MQTT311,
		KeepAlive: 100}

	session, replies, err := sm.handlePacket("", pkt)
	require.Nil(t, err)

	require.Len(t, replies, 1)
	reply := replies[0]
	require.Equal(t, packet.CONNACK, reply.Type())

	connAck := reply.(*packet.ConnAck)
	assert.Equal(t, packet.ReasonCodeV3ConnectionAccepted, connAck.ReasonCode)

	require.NotNil(t, session)
	assert.True(t, session.connected)
	assert.Equal(t, conf.MaxKeepAlive, session.KeepAlive)
}

func TestSessionManagerConnectV5MaxKeepAliveAccepted(t *testing.T) {
	conf := newConfiguration()
	conf.MaxKeepAlive = 100
	sm := createSessionManager(conf)

	pkt := &packet.Connect{ClientID: []byte("a"), Version: packet.MQTT50,
		KeepAlive: 200}

	session, replies, err := sm.handlePacket("", pkt)
	require.Nil(t, err)

	require.Len(t, replies, 1)
	reply := replies[0]
	require.Equal(t, packet.CONNACK, reply.Type())

	connAck := reply.(*packet.ConnAck)
	assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)

	require.NotNil(t, session)
	assert.True(t, session.connected)
	assert.Equal(t, conf.MaxKeepAlive, session.KeepAlive)
	assert.NotNil(t, connAck.Properties)
	assert.NotNil(t, connAck.Properties.ServerKeepAlive)
	assert.Equal(t, conf.MaxKeepAlive, int(*connAck.Properties.ServerKeepAlive))
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

			pkt := &packet.Connect{ClientID: []byte("a"),
				Version: packet.MQTT50}

			session, replies, err := sm.handlePacket("", pkt)
			require.Nil(t, err)
			require.NotNil(t, session)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAck := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
			require.NotNil(t, connAck.Properties)
			assert.True(t, session.connected)
			assert.NotZero(t, session.SessionID)

			if tc.resp > 0 {
				assert.NotNil(t, connAck.Properties.ReceiveMaximum)
				assert.Equal(t, tc.resp, *connAck.Properties.ReceiveMaximum)
			} else {
				assert.Nil(t, connAck.Properties.ReceiveMaximum)
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

			pkt := &packet.Connect{ClientID: []byte("a"),
				Version: packet.MQTT50}

			session, replies, err := sm.handlePacket("", pkt)
			require.Nil(t, err)
			require.NotNil(t, session)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAck := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
			require.NotNil(t, connAck.Properties)
			assert.True(t, session.connected)

			if tc.resp > 0 {
				assert.NotNil(t, connAck.Properties.MaximumPacketSize)
				assert.Equal(t, tc.resp,
					*connAck.Properties.MaximumPacketSize)
			} else {
				assert.Nil(t, connAck.Properties.MaximumPacketSize)
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

			pkt := &packet.Connect{ClientID: []byte("a"),
				Version: packet.MQTT50}

			session, replies, err := sm.handlePacket("", pkt)
			require.Nil(t, err)
			require.NotNil(t, session)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAck := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
			require.NotNil(t, connAck.Properties)
			assert.True(t, session.connected)

			if tc < packet.QoS2 {
				assert.NotNil(t, connAck.Properties.MaximumQoS)
				assert.Equal(t, byte(tc), *connAck.Properties.MaximumQoS)
			} else {
				assert.Nil(t, connAck.Properties.MaximumQoS)
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

			pkt := &packet.Connect{ClientID: []byte("a"),
				Version: packet.MQTT50}

			session, replies, err := sm.handlePacket("", pkt)
			require.Nil(t, err)
			require.NotNil(t, session)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAck := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
			require.NotNil(t, connAck.Properties)
			assert.True(t, session.connected)

			if tc.maxAlias > 0 {
				assert.NotNil(t, connAck.Properties.TopicAliasMaximum)
				assert.Equal(t, tc.maxAlias,
					*connAck.Properties.TopicAliasMaximum)
			} else {
				assert.Nil(t, connAck.Properties.TopicAliasMaximum)
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

			pkt := &packet.Connect{ClientID: []byte("a"),
				Version: packet.MQTT50}

			session, replies, err := sm.handlePacket("", pkt)
			require.Nil(t, err)
			require.NotNil(t, session)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAck := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
			require.NotNil(t, connAck.Properties)
			assert.True(t, session.connected)

			if !tc.available {
				assert.NotNil(t, connAck.Properties.RetainAvailable)
				assert.Equal(t, byte(0), *connAck.Properties.RetainAvailable)
			} else {
				assert.Nil(t, connAck.Properties.RetainAvailable)
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

			pkt := &packet.Connect{ClientID: []byte("a"),
				Version: packet.MQTT50}

			session, replies, err := sm.handlePacket("", pkt)
			require.Nil(t, err)
			require.NotNil(t, session)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAck := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
			require.NotNil(t, connAck.Properties)
			assert.True(t, session.connected)

			if !tc.available {
				assert.NotNil(t,
					connAck.Properties.WildcardSubscriptionAvailable)
				assert.Equal(t, byte(0),
					*connAck.Properties.WildcardSubscriptionAvailable)
			} else {
				assert.Nil(t, connAck.Properties.WildcardSubscriptionAvailable)
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

			pkt := &packet.Connect{ClientID: []byte("a"),
				Version: packet.MQTT50}

			session, replies, err := sm.handlePacket("", pkt)
			require.Nil(t, err)
			require.NotNil(t, session)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAck := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
			require.NotNil(t, connAck.Properties)
			assert.True(t, session.connected)

			if !tc.available {
				assert.NotNil(t,
					connAck.Properties.SubscriptionIDAvailable)
				assert.Equal(t, byte(0),
					*connAck.Properties.SubscriptionIDAvailable)
			} else {
				assert.Nil(t, connAck.Properties.SubscriptionIDAvailable)
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

			pkt := &packet.Connect{ClientID: []byte("a"),
				Version: packet.MQTT50}

			session, replies, err := sm.handlePacket("", pkt)
			require.Nil(t, err)
			require.NotNil(t, session)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAck := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
			assert.NotNil(t, connAck.Properties)
			assert.True(t, session.connected)

			if !tc.available {
				assert.NotNil(t,
					connAck.Properties.SharedSubscriptionAvailable)
				assert.Equal(t, byte(0),
					*connAck.Properties.SharedSubscriptionAvailable)
			} else {
				assert.Nil(t, connAck.Properties.SharedSubscriptionAvailable)
			}
		})
	}
}

func TestSessionManagerConnectV5UserProperty(t *testing.T) {
	conf := newConfiguration()
	conf.UserProperties = map[string]string{"k1": "v1"}
	sm := createSessionManager(conf)

	pkt := &packet.Connect{ClientID: []byte("a"), Version: packet.MQTT50}

	session, replies, err := sm.handlePacket("", pkt)
	require.Nil(t, err)
	require.NotNil(t, session)
	assert.True(t, session.connected)

	require.Len(t, replies, 1)
	reply := replies[0]
	require.Equal(t, packet.CONNACK, reply.Type())

	connAck := reply.(*packet.ConnAck)
	assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)

	require.NotNil(t, connAck.Properties)
	assert.Equal(t, 1, len(connAck.Properties.UserProperties))
	assert.Equal(t, []byte("k1"), connAck.Properties.UserProperties[0].Key)
	assert.Equal(t, []byte("v1"), connAck.Properties.UserProperties[0].Value)
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

			session := &Session{}
			sm.store.sessions["a"] = session
			inflightMsgList := make([]*message, 0)

			for i := 0; i < 10; i++ {
				qos := packet.QoS(i%2 + 1)

				topic := fmt.Sprintf("data/%v", i)
				newPub := packet.NewPublish(packet.ID(i+1), tc, topic, qos,
					0, 0, nil, nil)

				msg := message{packetID: newPub.PacketID, packet: &newPub,
					id: messageID(newPub.PacketID)}

				session.inflightMessages.PushBack(&msg)
				inflightMsgList = append(inflightMsgList, &msg)
			}

			connect := packet.Connect{ClientID: []byte{'a'}, Version: tc}

			session, replies, err := sm.handlePacket("", &connect)
			require.Nil(t, err)
			require.NotNil(t, session)

			require.Len(t, replies, 11)
			reply := replies[0]
			assert.Equal(t, packet.CONNACK, reply.Type())

			for i := 0; i < 10; i++ {
				reply = replies[i+1]
				require.Equal(t, packet.PUBLISH, reply.Type())

				pub := reply.(*packet.Publish)
				assert.Equal(t, inflightMsgList[i].packet, pub)
				assert.Equal(t, 1, inflightMsgList[i].tries)
				assert.NotZero(t, inflightMsgList[i].lastSent)

				qos := packet.QoS(i%2 + 1)
				assert.Equal(t, qos, pub.QoS)
			}
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
			id := ClientID('a')
			conf := newConfiguration()
			sm := createSessionManager(conf)

			pkt := &packet.Connect{ClientID: []byte("a"), Version: tc}

			_, _, err := sm.handlePacket("", pkt)
			require.Nil(t, err)

			pingReq := &packet.PingReq{}
			_, replies, err := sm.handlePacket(id, pingReq)
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
			id := ClientID('a')
			conf := newConfiguration()
			sm := createSessionManager(conf)

			pingReq := &packet.PingReq{}
			session, replies, err := sm.handlePacket(id, pingReq)
			assert.Nil(t, session)
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
			id := ClientID("a")

			pkt := &packet.Connect{ClientID: []byte(id), Version: tc.version}

			session, _, err := sm.handlePacket("", pkt)
			require.Nil(t, err)

			subPkt := packet.Subscribe{PacketID: 1, Version: tc.version,
				Topics: []packet.Topic{{Name: "data", QoS: tc.qos}}}

			subs := Subscription{ID: 0, ClientID: id, TopicFilter: "data",
				QoS: tc.qos}

			ps := sm.pubSub.(*pubSubMock)
			ps.On("subscribe", session, subPkt.Topics[0], 0).
				Return(subs, nil)

			session, replies, err := sm.handlePacket(id, &subPkt)
			require.Nil(t, err)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.SUBACK, reply.Type())

			subAck := reply.(*packet.SubAck)
			assert.Equal(t, subPkt.PacketID, subAck.PacketID)
			assert.Equal(t, tc.version, subAck.Version)
			assert.Len(t, subAck.ReasonCodes, 1)
			assert.Equal(t, packet.ReasonCode(tc.qos), subAck.ReasonCodes[0])

			require.Len(t, session.Subscriptions, 1)
			assert.Equal(t, subs, session.Subscriptions["data"])
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
			id := ClientID("a")

			pkt := &packet.Connect{ClientID: []byte(id), Version: tc}

			_, _, err := sm.handlePacket("", pkt)
			require.Nil(t, err)

			subPkt := packet.Subscribe{PacketID: 1, Version: tc,
				Topics: []packet.Topic{{Name: "data#", QoS: packet.QoS1}}}

			ps := sm.pubSub.(*pubSubMock)
			ps.On("subscribe", mock.Anything, mock.Anything,
				mock.Anything).
				Return(Subscription{}, errors.New("invalid subscription"))

			session, replies, err := sm.handlePacket(id, &subPkt)
			require.Nil(t, err)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.SUBACK, reply.Type())

			subAck := reply.(*packet.SubAck)
			assert.Equal(t, subPkt.PacketID, subAck.PacketID)
			assert.Equal(t, tc, subAck.Version)
			assert.Len(t, subAck.ReasonCodes, 1)
			assert.Equal(t, packet.ReasonCodeV3Failure, subAck.ReasonCodes[0])

			require.NotNil(t, session)
			assert.Empty(t, session.Subscriptions)
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
			id := ClientID("a")

			subPkt := packet.Subscribe{PacketID: 1, Version: tc,
				Topics: []packet.Topic{{Name: "data#", QoS: packet.QoS1}}}

			session, replies, err := sm.handlePacket(id, &subPkt)
			assert.Nil(t, session)
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
			id := ClientID("a")

			pkt := &packet.Connect{ClientID: []byte(id), Version: tc}

			session, _, err := sm.handlePacket("", pkt)
			require.Nil(t, err)

			subPkt := packet.Subscribe{PacketID: 5, Version: tc,
				Topics: []packet.Topic{
					{Name: "data/temp/0", QoS: packet.QoS0},
					{Name: "data/temp/1", QoS: packet.QoS1},
					{Name: "data/temp/2", QoS: packet.QoS2},
					{Name: "data/temp#", QoS: packet.QoS0},
				},
			}

			subs := []Subscription{
				{ID: 0, ClientID: id, TopicFilter: subPkt.Topics[0].Name,
					QoS: subPkt.Topics[0].QoS},
				{ID: 0, ClientID: id, TopicFilter: subPkt.Topics[1].Name,
					QoS: subPkt.Topics[1].QoS},
				{ID: 0, ClientID: id, TopicFilter: subPkt.Topics[2].Name,
					QoS: subPkt.Topics[2].QoS},
			}

			ps := sm.pubSub.(*pubSubMock)
			ps.On("subscribe", session, subPkt.Topics[0], 0).
				Return(subs[0], nil).
				On("subscribe", session, subPkt.Topics[1], 0).
				Return(subs[1], nil).
				On("subscribe", session, subPkt.Topics[2], 0).
				Return(subs[2], nil).
				On("subscribe", session, subPkt.Topics[3], 0).
				Return(Subscription{}, errors.New("invalid topic"))

			session, replies, err := sm.handlePacket(id, &subPkt)
			require.Nil(t, err)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.SUBACK, reply.Type())

			subAck := reply.(*packet.SubAck)
			assert.Equal(t, subPkt.PacketID, subAck.PacketID)
			assert.Equal(t, subPkt.Version, subAck.Version)
			assert.Len(t, subAck.ReasonCodes, 4)
			assert.Equal(t, packet.ReasonCode(packet.QoS0),
				subAck.ReasonCodes[0])
			assert.Equal(t, packet.ReasonCode(packet.QoS1),
				subAck.ReasonCodes[1])
			assert.Equal(t, packet.ReasonCode(packet.QoS2),
				subAck.ReasonCodes[2])
			assert.Equal(t, packet.ReasonCodeV3Failure, subAck.ReasonCodes[3])

			require.Len(t, session.Subscriptions, 3)
			assert.Equal(t, subs[0],
				session.Subscriptions[subPkt.Topics[0].Name])
			assert.Equal(t, subs[1],
				session.Subscriptions[subPkt.Topics[1].Name])
			assert.Equal(t, subs[2],
				session.Subscriptions[subPkt.Topics[2].Name])
			ps.AssertExpectations(t)
		})
	}
}

func TestSessionManagerSubscribeV5WithSubID(t *testing.T) {
	conf := newConfiguration()
	conf.SubscriptionIDAvailable = true
	sm := createSessionManager(conf)
	id := ClientID("a")

	pkt := &packet.Connect{ClientID: []byte(id), Version: packet.MQTT50}

	_, _, err := sm.handlePacket("", pkt)
	require.Nil(t, err)

	props := &packet.Properties{}
	props.SubscriptionIdentifier = new(int)
	*props.SubscriptionIdentifier = 1

	subPkt := packet.Subscribe{PacketID: 2, Version: packet.MQTT50,
		Properties: props, Topics: []packet.Topic{{Name: "topic"}}}

	subs := Subscription{ID: *props.SubscriptionIdentifier, ClientID: id,
		TopicFilter: subPkt.Topics[0].Name}

	ps := sm.pubSub.(*pubSubMock)
	ps.On("subscribe", mock.Anything, mock.Anything,
		*props.SubscriptionIdentifier).Return(subs, nil)

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
	id := ClientID("a")

	pkt := &packet.Connect{ClientID: []byte(id), Version: packet.MQTT50}

	_, _, err := sm.handlePacket("", pkt)
	require.Nil(t, err)

	props := &packet.Properties{}
	props.SubscriptionIdentifier = new(int)
	*props.SubscriptionIdentifier = 1

	sub := packet.Subscribe{PacketID: 2, Version: packet.MQTT50,
		Properties: props, Topics: []packet.Topic{{Name: "topic"}}}

	_, replies, err := sm.handlePacket(id, &sub)
	require.NotNil(t, err)

	require.Len(t, replies, 1)
	reply := replies[0]
	require.Equal(t, packet.DISCONNECT, reply.Type())

	disconnect := reply.(*packet.Disconnect)
	assert.Equal(t, packet.MQTT50, disconnect.Version)
	assert.Equal(t, packet.ReasonCodeV5SubscriptionIDNotSupported,
		disconnect.ReasonCode)
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
			id := ClientID("a")

			pkt := &packet.Connect{ClientID: []byte(id), Version: tc}

			session, _, err := sm.handlePacket("", pkt)
			require.Nil(t, err)

			subs := Subscription{ID: 1, ClientID: id, TopicFilter: "data"}
			session.Subscriptions["data"] = subs

			ps := sm.pubSub.(*pubSubMock)
			ps.On("unsubscribe", id, "data").Return(nil)

			unsub := packet.Unsubscribe{PacketID: 1, Version: tc,
				Topics: []string{"data"}}

			session, replies, err := sm.handlePacket(id, &unsub)
			require.Nil(t, err)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.UNSUBACK, reply.Type())

			unsubAck := reply.(*packet.UnsubAck)
			assert.Equal(t, unsub.PacketID, unsubAck.PacketID)
			assert.Equal(t, unsub.Version, unsubAck.Version)
			assert.Empty(t, unsubAck.ReasonCodes)

			require.NotNil(t, session)
			assert.Empty(t, session.Subscriptions)
			ps.AssertExpectations(t)
		})
	}
}

func TestSessionManagerUnsubscribeV5(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)
	id := ClientID("a")

	pkt := &packet.Connect{ClientID: []byte(id), Version: packet.MQTT50}

	session, _, err := sm.handlePacket("", pkt)
	require.Nil(t, err)

	subs := Subscription{ID: 1, ClientID: id, TopicFilter: "data"}
	session.Subscriptions["data"] = subs

	ps := sm.pubSub.(*pubSubMock)
	ps.On("unsubscribe", id, "data").Return(nil)

	unsub := packet.Unsubscribe{PacketID: 1, Version: packet.MQTT50,
		Topics: []string{"data"}}

	session, replies, err := sm.handlePacket(id, &unsub)
	require.Nil(t, err)

	require.Len(t, replies, 1)
	reply := replies[0]
	require.Equal(t, packet.UNSUBACK, reply.Type())

	unsubAck := reply.(*packet.UnsubAck)
	assert.Equal(t, unsub.PacketID, unsubAck.PacketID)
	assert.Equal(t, unsub.Version, unsubAck.Version)

	codes := []packet.ReasonCode{packet.ReasonCodeV5Success}
	assert.Equal(t, codes, unsubAck.ReasonCodes)

	require.NotNil(t, session)
	assert.Empty(t, session.Subscriptions)
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
			id := ClientID("a")

			unsub := packet.Unsubscribe{PacketID: 1, Version: tc,
				Topics: []string{"data"}}

			session, replies, err := sm.handlePacket(id, &unsub)
			assert.Nil(t, session)
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
			id := ClientID("a")

			pkt := &packet.Connect{ClientID: []byte(id), Version: tc}

			_, _, err := sm.handlePacket("", pkt)
			require.Nil(t, err)

			ps := sm.pubSub.(*pubSubMock)
			ps.On("unsubscribe", id, "data").Return(ErrSubscriptionNotFound)

			unsub := packet.Unsubscribe{PacketID: 1, Version: tc,
				Topics: []string{"data"}}

			_, replies, err := sm.handlePacket(id, &unsub)
			require.Nil(t, err)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.UNSUBACK, reply.Type())

			unsubAck := reply.(*packet.UnsubAck)
			assert.Equal(t, unsub.PacketID, unsubAck.PacketID)
			assert.Equal(t, unsub.Version, unsubAck.Version)
			assert.Empty(t, unsubAck.ReasonCodes)
			ps.AssertExpectations(t)
		})
	}
}

func TestSessionManagerUnsubscribeV5MissingSubscription(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)
	id := ClientID("a")

	pkt := &packet.Connect{ClientID: []byte(id), Version: packet.MQTT50}

	_, _, err := sm.handlePacket("", pkt)
	require.Nil(t, err)

	ps := sm.pubSub.(*pubSubMock)
	ps.On("unsubscribe", id, "data").Return(ErrSubscriptionNotFound)

	unsub := packet.Unsubscribe{PacketID: 1, Version: packet.MQTT50,
		Topics: []string{"data"}}

	_, replies, err := sm.handlePacket(id, &unsub)
	require.Nil(t, err)

	require.Len(t, replies, 1)
	reply := replies[0]
	require.Equal(t, packet.UNSUBACK, reply.Type())

	unsubAck := reply.(*packet.UnsubAck)
	assert.Equal(t, unsub.PacketID, unsubAck.PacketID)
	assert.Equal(t, unsub.Version, unsubAck.Version)

	codes := []packet.ReasonCode{packet.ReasonCodeV5NoSubscriptionExisted}
	assert.Equal(t, codes, unsubAck.ReasonCodes)
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
			id := ClientID("a")

			pkt := &packet.Connect{ClientID: []byte(id), Version: tc}

			session, _, err := sm.handlePacket("", pkt)
			require.Nil(t, err)

			session.Subscriptions["data/0"] = Subscription{ID: 1, ClientID: id,
				TopicFilter: "data/0", QoS: packet.QoS0}
			session.Subscriptions["data/1"] = Subscription{ID: 3, ClientID: id,
				TopicFilter: "data/1", QoS: packet.QoS1}
			session.Subscriptions["data/#"] = Subscription{ID: 3, ClientID: id,
				TopicFilter: "data/#", QoS: packet.QoS2}

			ps := sm.pubSub.(*pubSubMock)
			ps.On("unsubscribe", id, "data/0").Return(nil).
				On("unsubscribe", id, "data/#").Return(nil)

			topicNames := []string{"data/0", "data/#"}
			unsub := packet.Unsubscribe{PacketID: 1, Version: tc,
				Topics: topicNames}

			session, replies, err := sm.handlePacket(id, &unsub)
			require.Nil(t, err)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.UNSUBACK, reply.Type())

			unsubAck := reply.(*packet.UnsubAck)
			assert.Equal(t, unsub.PacketID, unsubAck.PacketID)
			assert.Equal(t, unsub.Version, unsubAck.Version)
			assert.Empty(t, unsubAck.ReasonCodes)

			require.NotNil(t, session)
			assert.Len(t, session.Subscriptions, 1)
			ps.AssertExpectations(t)
		})
	}
}

func TestSessionManagerUnsubscribeV5MultipleTopics(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)
	id := ClientID("a")

	pkt := &packet.Connect{ClientID: []byte(id), Version: packet.MQTT50}

	session, _, err := sm.handlePacket("", pkt)
	require.Nil(t, err)

	session.Subscriptions["data/0"] = Subscription{ID: 1, ClientID: id,
		TopicFilter: "data/0", QoS: packet.QoS0}
	session.Subscriptions["data/1"] = Subscription{ID: 3, ClientID: id,
		TopicFilter: "data/1", QoS: packet.QoS1}
	session.Subscriptions["data/#"] = Subscription{ID: 3, ClientID: id,
		TopicFilter: "data/#", QoS: packet.QoS2}

	ps := sm.pubSub.(*pubSubMock)
	ps.On("unsubscribe", id, "data/0").Return(nil).
		On("unsubscribe", id, "data/#").Return(nil)

	topicNames := []string{"data/0", "data/#"}
	unsub := packet.Unsubscribe{PacketID: 1, Version: packet.MQTT50,
		Topics: topicNames}

	session, replies, err := sm.handlePacket(id, &unsub)
	require.Nil(t, err)

	require.Len(t, replies, 1)
	reply := replies[0]
	require.Equal(t, packet.UNSUBACK, reply.Type())

	unsubAck := reply.(*packet.UnsubAck)
	assert.Equal(t, unsub.PacketID, unsubAck.PacketID)
	assert.Equal(t, unsub.Version, unsubAck.Version)

	codes := []packet.ReasonCode{packet.ReasonCodeV5Success,
		packet.ReasonCodeV5Success}
	assert.Equal(t, codes, unsubAck.ReasonCodes)

	require.NotNil(t, session)
	assert.Len(t, session.Subscriptions, 1)
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
			id := ClientID("a")

			pkt := &packet.Connect{ClientID: []byte(id), Version: tc.version}

			_, _, err := sm.handlePacket("", pkt)
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
			id := ClientID("a")

			pkt := &packet.Connect{ClientID: []byte(id), Version: tc.version}

			_, _, err := sm.handlePacket("", pkt)
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

			pubAck := reply.(*packet.PubAck)
			assert.Equal(t, packet.ID(1), pubAck.PacketID)
			assert.Equal(t, tc.version, pubAck.Version)
			assert.Equal(t, packet.ReasonCodeV5Success, pubAck.ReasonCode)
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
			id := ClientID("a")

			connect := &packet.Connect{ClientID: []byte(id),
				Version: tc.version}

			_, _, err := sm.handlePacket("", connect)
			require.Nil(t, err)

			pubPkt := packet.NewPublish(1, tc.version, tc.topic, packet.QoS2,
				0, 0, []byte(tc.payload), nil)

			idGen := sm.idGen.(*idGeneratorMock)
			idGen.On("NextID").Return(10)

			session, replies, err := sm.handlePacket(id, &pubPkt)
			require.Nil(t, err)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.PUBREC, reply.Type())

			pubRec := reply.(*packet.PubRec)
			assert.Equal(t, packet.ID(1), pubRec.PacketID)
			assert.Equal(t, tc.version, pubRec.Version)
			assert.Equal(t, packet.ReasonCodeV5Success, pubRec.ReasonCode)

			require.NotNil(t, session)
			require.Equal(t, 1, session.unAckPubPackets.Len())

			pkt := session.unAckPubPackets.Front().Value.(*packet.Publish)
			assert.Same(t, &pubPkt, pkt)
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
			id := ClientID("a")

			connect := &packet.Connect{ClientID: []byte(id),
				Version: tc.version}

			_, _, err := sm.handlePacket("", connect)
			require.Nil(t, err)

			pubPkt1 := packet.NewPublish(1, tc.version, tc.topic,
				packet.QoS2, 0, 0, []byte(tc.payload), nil)

			pubPkt2 := packet.NewPublish(2, tc.version, tc.topic,
				packet.QoS2, 0, 0, []byte(tc.payload), nil)

			idGen := sm.idGen.(*idGeneratorMock)
			idGen.On("NextID").Return(10)

			_, replies, err := sm.handlePacket(id, &pubPkt1)
			require.Nil(t, err)
			pubRec := replies[0].(*packet.PubRec)
			assert.Equal(t, packet.ID(1), pubRec.PacketID)
			assert.Equal(t, packet.ReasonCodeV5Success, pubRec.ReasonCode)

			_, replies, err = sm.handlePacket(id, &pubPkt2)
			require.Nil(t, err)
			pubRec = replies[0].(*packet.PubRec)
			assert.Equal(t, packet.ID(2), pubRec.PacketID)
			assert.Equal(t, packet.ReasonCodeV5Success, pubRec.ReasonCode)

			session, replies, err := sm.handlePacket(id, &pubPkt1)
			require.Nil(t, err)
			pubRec = replies[0].(*packet.PubRec)
			assert.Equal(t, packet.ID(1), pubRec.PacketID)
			assert.Equal(t, packet.ReasonCodeV5Success, pubRec.ReasonCode)

			require.NotNil(t, session)
			require.Equal(t, 2, session.unAckPubPackets.Len())

			pkt := session.unAckPubPackets.Front()
			assert.Same(t, &pubPkt1, pkt.Value)

			pkt = pkt.Next()
			assert.Same(t, &pubPkt2, pkt.Value)
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
			id := ClientID("a")

			pubPkt := packet.NewPublish(1, tc, "data", packet.QoS0,
				0, 0, []byte("data"), nil)

			session, replies, err := sm.handlePacket(id, &pubPkt)
			assert.Nil(t, session)
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
			id := ClientID("a")

			connect := packet.Connect{ClientID: []byte(id), Version: tc}

			session, _, err := sm.handlePacket("", &connect)
			require.Nil(t, err)

			publish := packet.NewPublish(1, tc, "data", packet.QoS1,
				0, 0, nil, nil)

			msg := &message{id: 1, packetID: 1, packet: &publish, tries: 1,
				lastSent: time.Now().UnixMicro()}
			session.inflightMessages.PushBack(msg)

			pubAck := packet.NewPubAck(publish.PacketID, publish.Version,
				packet.ReasonCodeV5Success, nil)

			session, replies, err := sm.handlePacket(id, &pubAck)
			require.Nil(t, err)
			assert.Empty(t, replies)

			require.NotNil(t, session)
			assert.Zero(t, session.inflightMessages.Len())

			s := sm.store.sessions[session.ClientID]
			assert.Zero(t, s.inflightMessages.Len())
		})
	}
}

func TestSessionManagerPubAckUnknownMessage(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)
	id := ClientID("a")

	pkt := &packet.Connect{ClientID: []byte(id), Version: packet.MQTT311}

	_, _, err := sm.handlePacket("", pkt)
	require.Nil(t, err)

	pubAck := packet.NewPubAck(10, packet.MQTT311,
		packet.ReasonCodeV5Success, nil)

	_, replies, err := sm.handlePacket(id, &pubAck)
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
			id := ClientID("a")

			pubAck := packet.NewPubAck(1, tc, packet.ReasonCodeV5Success, nil)

			session, replies, err := sm.handlePacket(id, &pubAck)
			assert.Nil(t, session)
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
			id := ClientID("a")

			pkt := &packet.Connect{ClientID: []byte(id), Version: tc}

			session, _, err := sm.handlePacket("", pkt)
			require.Nil(t, err)

			sub := Subscription{ID: 1, TopicFilter: "test", ClientID: id}
			session.Subscriptions["test"] = sub

			disconnect := packet.NewDisconnect(tc, packet.ReasonCodeV5Success,
				nil)

			_, replies, err := sm.handlePacket(id, &disconnect)
			assert.Nil(t, err)
			require.Empty(t, replies)

			require.NotNil(t, session)
			assert.False(t, session.connected)
			assert.NotEmpty(t, session.Subscriptions)

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
			id := ClientID("a")

			pkt := &packet.Connect{ClientID: []byte(id), Version: tc,
				CleanSession: true}

			session, _, err := sm.handlePacket("", pkt)
			require.Nil(t, err)

			sub := Subscription{ID: 1, TopicFilter: "test", ClientID: id}
			session.Subscriptions["test"] = sub

			ps := sm.pubSub.(*pubSubMock)
			ps.On("unsubscribe", id, "test").Return(nil)

			disconnect := packet.NewDisconnect(tc, packet.ReasonCodeV5Success,
				nil)

			session, replies, err := sm.handlePacket(id, &disconnect)
			assert.Nil(t, err)
			require.Empty(t, replies)

			require.NotNil(t, session)
			assert.False(t, session.connected)
			assert.Empty(t, session.Subscriptions)

			_, ok := sm.sessions[id]
			assert.False(t, ok)
			ps.AssertExpectations(t)
		})
	}
}

func TestSessionManagerDisconnectExpiryInterval(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)
	id := ClientID("a")

	props := &packet.Properties{}
	props.SessionExpiryInterval = new(uint32)
	*props.SessionExpiryInterval = 600

	pkt := &packet.Connect{ClientID: []byte(id), Version: packet.MQTT50,
		CleanSession: true, Properties: props}

	session, _, err := sm.handlePacket("", pkt)
	require.Nil(t, err)

	props.SessionExpiryInterval = new(uint32)
	*props.SessionExpiryInterval = 300
	require.Equal(t, uint32(600), session.ExpiryInterval)

	disconnect := packet.NewDisconnect(packet.MQTT50,
		packet.ReasonCodeV5Success, props)

	_, replies, err := sm.handlePacket(id, &disconnect)
	assert.Nil(t, err)
	assert.Empty(t, replies)
	assert.Equal(t, uint32(300), session.ExpiryInterval)
}

func TestSessionManagerDisconnectInvalidExpiryInterval(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)
	id := ClientID("a")

	pkt := &packet.Connect{ClientID: []byte(id), Version: packet.MQTT50}

	_, _, err := sm.handlePacket("", pkt)
	require.Nil(t, err)

	props := &packet.Properties{}
	props.SessionExpiryInterval = new(uint32)
	*props.SessionExpiryInterval = 300

	disconnect := packet.NewDisconnect(packet.MQTT50,
		packet.ReasonCodeV5Success, props)

	_, replies, err := sm.handlePacket(id, &disconnect)
	require.NotNil(t, err)

	require.Len(t, replies, 1)
	reply := replies[0]
	assert.Equal(t, packet.DISCONNECT, reply.Type())

	discReply := reply.(*packet.Disconnect)
	assert.Equal(t, packet.MQTT50, discReply.Version)
	assert.Equal(t, packet.ReasonCodeV5ProtocolError, discReply.ReasonCode)
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
			id := ClientID("a")

			disconnect := packet.NewDisconnect(tc, packet.ReasonCodeV5Success,
				nil)

			session, replies, err := sm.handlePacket(id, &disconnect)
			assert.Nil(t, session)
			assert.Empty(t, replies)
			assert.NotNil(t, err)
		})
	}
}

func TestSessionManagerDisconnectErrorOnSaveSessionIsOkay(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)
	id := ClientID("a")

	props := &packet.Properties{}
	props.SessionExpiryInterval = new(uint32)
	*props.SessionExpiryInterval = 600

	pkt := &packet.Connect{ClientID: []byte(id), Version: packet.MQTT50,
		CleanSession: true, Properties: props}

	session, _, err := sm.handlePacket("", pkt)
	require.Nil(t, err)

	props.SessionExpiryInterval = new(uint32)
	*props.SessionExpiryInterval = 300
	require.Equal(t, uint32(600), session.ExpiryInterval)

	disconnect := packet.NewDisconnect(packet.MQTT50,
		packet.ReasonCodeV5Success, props)

	_, replies, err := sm.handlePacket(id, &disconnect)
	assert.Nil(t, err)
	assert.Empty(t, replies)
	assert.Equal(t, uint32(300), session.ExpiryInterval)
}

func TestSessionManagerDisconnectFailedToDeleteSession(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)
	id := ClientID("a")

	pkt := &packet.Connect{ClientID: []byte(id), Version: packet.MQTT311,
		CleanSession: true}

	_, _, err := sm.handlePacket("", pkt)
	require.Nil(t, err)

	disconnect := packet.NewDisconnect(packet.MQTT311,
		packet.ReasonCodeV5Success, nil)

	_, replies, err := sm.handlePacket(id, &disconnect)
	assert.Nil(t, err)
	assert.Empty(t, replies)
}

func TestSessionManagerInvalidPacket(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)
	id := ClientID("a")

	pkt := &packet.Connect{ClientID: []byte(id), Version: packet.MQTT311}

	_, _, err := sm.handlePacket("", pkt)
	require.Nil(t, err)

	pingReq := packet.NewPingResp()

	_, reply, err := sm.handlePacket(id, &pingReq)
	assert.NotNil(t, err)
	assert.Nil(t, reply)
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

			session := &Session{}
			session.ClientID = "client-1"
			session.Version = tc
			session.connected = true
			sm.store.sessions[session.ClientID] = session

			pkt := packet.NewPublish(1, tc, "data", packet.QoS0,
				0, 0, nil, nil)
			msg := &message{id: 1, packet: &pkt}

			pktDeliverer := sm.deliverer.(*delivererMock)
			pktDeliverer.On("deliverPacket", session.ClientID, mock.Anything).
				Return(nil)

			err := sm.publishMessage(session.ClientID, msg)
			assert.Nil(t, err)
			pub := pktDeliverer.Calls[0].Arguments.Get(1).(*packet.Publish)

			assert.Zero(t, session.inflightMessages.Len())
			assert.Equal(t, &pkt, pub)
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

			session := &Session{}
			session.ClientID = "client-1"
			session.Version = tc
			session.connected = true
			sm.store.sessions[session.ClientID] = session

			pkt := packet.NewPublish(1, tc, "data", packet.QoS1,
				0, 0, nil, nil)
			msg := &message{id: 1, packet: &pkt}

			pktDeliverer := sm.deliverer.(*delivererMock)
			pktDeliverer.On("deliverPacket", session.ClientID, mock.Anything).
				Return(nil)

			err := sm.publishMessage(session.ClientID, msg)
			assert.Nil(t, err)
			pub := pktDeliverer.Calls[0].Arguments.Get(1).(*packet.Publish)

			assert.Equal(t, 1, session.inflightMessages.Len())
			firstMsg := session.inflightMessages.Front().Value.(*message)
			require.NotNil(t, firstMsg)
			assert.Equal(t, messageID(1), firstMsg.id)
			assert.Equal(t, packet.ID(session.lastPacketID), firstMsg.packetID)
			assert.Equal(t, msg.packet.QoS, firstMsg.packet.QoS)
			assert.Equal(t, msg.packet.Version, firstMsg.packet.Version)
			assert.Equal(t, msg.packet.TopicName, firstMsg.packet.TopicName)
			assert.Equal(t, msg.packet.Payload, firstMsg.packet.Payload)
			assert.NotZero(t, firstMsg.lastSent)
			assert.Equal(t, 1, firstMsg.tries)
			assert.Equal(t, firstMsg.packet, pub)

			s := sm.store.sessions[session.ClientID]
			assert.Equal(t, 1, s.inflightMessages.Len())
			m := s.inflightMessages.Front().Value.(*message)
			assert.Equal(t, firstMsg, m)
			pktDeliverer.AssertExpectations(t)
		})
	}
}

func TestSessionManagerPublishMessageDifferentVersion(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)

	session := &Session{}
	session.ClientID = "client-1"
	session.Version = packet.MQTT50
	session.connected = true
	sm.store.sessions[session.ClientID] = session

	pkt := packet.NewPublish(10, packet.MQTT311, "data",
		packet.QoS0, 0, 0, nil, nil)
	msg := message{id: 50, packet: &pkt}

	pktDeliverer := sm.deliverer.(*delivererMock)
	pktDeliverer.On("deliverPacket", session.ClientID, &pkt).
		Return(nil)

	err := sm.publishMessage(session.ClientID, &msg)
	assert.Nil(t, err)

	pub := pktDeliverer.Calls[0].Arguments.Get(1).(*packet.Publish)
	assert.Equal(t, pkt.PacketID, pub.PacketID)
	assert.Equal(t, pkt.QoS, pub.QoS)
	assert.Equal(t, pkt.TopicName, pub.TopicName)
	assert.Equal(t, session.Version, pub.Version)
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

			session := &Session{}
			session.ClientID = "client-1"
			session.connected = false
			sm.store.sessions[session.ClientID] = session

			pkt := packet.NewPublish(1, packet.MQTT311, "data",
				tc, 0, 0, nil, nil)
			msg := message{id: 1, packet: &pkt}

			err := sm.publishMessage(session.ClientID, &msg)
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

	session := &Session{}
	session.ClientID = "client-1"
	session.Version = packet.MQTT311
	session.connected = true
	sm.store.sessions[session.ClientID] = session

	pkt := packet.NewPublish(10, packet.MQTT311, "data",
		packet.QoS0, 0, 0, nil, nil)
	msg := message{id: 50, packet: &pkt}

	pktDeliverer := sm.deliverer.(*delivererMock)
	pktDeliverer.On("deliverPacket", session.ClientID, &pkt).
		Return(errors.New("failed to deliver message"))

	err := sm.publishMessage(session.ClientID, &msg)
	assert.NotNil(t, err)
	pktDeliverer.AssertExpectations(t)
}

func TestSessionManagerPublishMessageReadSessionError(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)

	pkt := packet.NewPublish(10, packet.MQTT311, "data",
		packet.QoS0, 0, 0, nil, nil)
	msg := message{id: 50, packet: &pkt}

	err := sm.publishMessage("id-0", &msg)
	assert.NotNil(t, err)
}
