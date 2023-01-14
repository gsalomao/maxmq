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

func createSessionManager(conf Configuration) *sessionManager {
	log := newLogger()
	deliverer := delivererMock{}

	userProps := make([]packet.UserProperty, 0, len(conf.UserProperties))
	for k, v := range conf.UserProperties {
		userProps = append(userProps,
			packet.UserProperty{Key: []byte(k), Value: []byte(v)})
	}

	m := newMetrics(true, &log)
	idGen := &idGeneratorMock{}
	idGen.On("NextID").Return(1)

	return newSessionManager(&deliverer, idGen, &conf, m, userProps, &log)
}

func subscribe(sm *sessionManager, id ClientID, topics []packet.Topic,
	version packet.MQTTVersion) error {

	sub := packet.Subscribe{PacketID: 5, Version: version, Topics: topics}
	_, _, err := sm.handlePacket(id, &sub)
	if err != nil {
		return err
	}

	return nil
}

func TestSessionManagerConnectNewSession(t *testing.T) {
	testCases := []struct {
		name         string
		version      packet.MQTTVersion
		cleanSession bool
		code         packet.ReasonCode
	}{
		{name: "3.1", version: packet.MQTT31,
			cleanSession: false, code: packet.ReasonCodeV3ConnectionAccepted},
		{name: "3.1.1", version: packet.MQTT311,
			cleanSession: false, code: packet.ReasonCodeV3ConnectionAccepted},
		{name: "5.0", version: packet.MQTT50,
			cleanSession: false, code: packet.ReasonCodeV5Success},
		{name: "3.1-CleanSession", version: packet.MQTT31,
			cleanSession: true, code: packet.ReasonCodeV3ConnectionAccepted},
		{name: "3.1.1-CleanSession", version: packet.MQTT311,
			cleanSession: true, code: packet.ReasonCodeV3ConnectionAccepted},
		{name: "5.0-CleanSession", version: packet.MQTT50,
			cleanSession: true, code: packet.ReasonCodeV5Success},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)
			id := ClientID("a")

			pkt := &packet.Connect{ClientID: []byte(id), Version: tc.version,
				CleanSession: tc.cleanSession}
			session, replies, err := sm.handleConnect(pkt)
			require.Nil(t, err)
			require.NotNil(t, session)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAck := reply.(*packet.ConnAck)
			assert.Equal(t, tc.code, connAck.ReasonCode)
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
	testCases := []struct {
		name         string
		version      packet.MQTTVersion
		cleanSession bool
		code         packet.ReasonCode
	}{
		{name: "3.1", version: packet.MQTT31,
			cleanSession: false, code: packet.ReasonCodeV3ConnectionAccepted},
		{name: "3.1.1", version: packet.MQTT311,
			cleanSession: false, code: packet.ReasonCodeV3ConnectionAccepted},
		{name: "5.0", version: packet.MQTT50,
			cleanSession: false, code: packet.ReasonCodeV5Success},
		{name: "3.1-CleanSession", version: packet.MQTT31,
			cleanSession: true, code: packet.ReasonCodeV3ConnectionAccepted},
		{name: "3.1.1-CleanSession", version: packet.MQTT311,
			cleanSession: true, code: packet.ReasonCodeV3ConnectionAccepted},
		{name: "5.0-CleanSession", version: packet.MQTT50,
			cleanSession: true, code: packet.ReasonCodeV5Success},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			conf := newConfiguration()
			id := ClientID("a")

			sm := createSessionManager(conf)
			s := &Session{
				ClientID:       id,
				SessionID:      1,
				ConnectedAt:    time.Now().Add(-1 * time.Minute).Unix(),
				ExpiryInterval: conf.MaxSessionExpiryInterval,
				Version:        tc.version,
				Subscriptions:  make(map[string]Subscription),
			}

			topic := packet.Topic{Name: "test"}
			sub, err := sm.pubSub.subscribe(s, topic, 1)
			require.Nil(t, err)
			s.Subscriptions["test"] = sub
			sm.store.sessions[s.ClientID] = s

			pkt := &packet.Connect{ClientID: []byte(id), Version: tc.version,
				CleanSession: tc.cleanSession}
			session, replies, err := sm.handlePacket("", pkt)
			require.Nil(t, err)
			require.NotNil(t, session)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAck := reply.(*packet.ConnAck)
			assert.Equal(t, tc.version, connAck.Version)
			assert.Equal(t, tc.code, connAck.ReasonCode)
			assert.True(t, session.connected)
			assert.Equal(t, id, session.ClientID)
			assert.NotZero(t, session.SessionID)

			if tc.cleanSession {
				assert.False(t, connAck.SessionPresent)
				assert.Empty(t, session.Subscriptions)
				assert.Empty(t, sm.pubSub.tree.root.children)
			} else {
				assert.True(t, connAck.SessionPresent)
				assert.NotEmpty(t, session.Subscriptions)
				assert.NotEmpty(t, sm.pubSub.tree.root.children)
			}
		})
	}
}

func TestSessionManagerConnectClientIDTooBig(t *testing.T) {
	testCases := []struct {
		name     string
		id       []byte
		maxIDLen int
		version  packet.MQTTVersion
		code     packet.ReasonCode
	}{
		{name: "3.1", id: []byte("012345678901234567890123"),
			version: packet.MQTT31, maxIDLen: 65535,
			code: packet.ReasonCodeV3IdentifierRejected,
		},
		{name: "3.1.1", id: []byte("0123456789012345678901234567890"),
			version: packet.MQTT311, maxIDLen: 30,
			code: packet.ReasonCodeV3IdentifierRejected,
		},
		{name: "5.0", id: []byte("0123456789012345678901234567890"),
			version: packet.MQTT50, maxIDLen: 30,
			code: packet.ReasonCodeV5InvalidClientID,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
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
	testCases := []struct {
		name    string
		version packet.MQTTVersion
		code    packet.ReasonCode
	}{
		{name: "3.1", version: packet.MQTT31,
			code: packet.ReasonCodeV3ConnectionAccepted},
		{name: "3.1.1", version: packet.MQTT311,
			code: packet.ReasonCodeV3ConnectionAccepted},
		{name: "5.0", version: packet.MQTT50, code: packet.ReasonCodeV5Success},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			conf := newConfiguration()
			conf.AllowEmptyClientID = true
			sm := createSessionManager(conf)

			pkt := &packet.Connect{Version: tc.version}
			session, replies, err := sm.handlePacket("", pkt)
			require.Nil(t, err)
			require.NotNil(t, session)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAck := reply.(*packet.ConnAck)
			assert.Equal(t, tc.code, connAck.ReasonCode)
			assert.True(t, session.connected)
			assert.Greater(t, len(session.ClientID), 0)
		})
	}
}

func TestSessionManagerConnectDenyEmptyClientID(t *testing.T) {
	testCases := []struct {
		name    string
		version packet.MQTTVersion
		code    packet.ReasonCode
	}{
		{name: "3.1", version: packet.MQTT31,
			code: packet.ReasonCodeV3IdentifierRejected},
		{name: "3.1.1", version: packet.MQTT311,
			code: packet.ReasonCodeV3IdentifierRejected},
		{name: "5.0", version: packet.MQTT50,
			code: packet.ReasonCodeV5InvalidClientID},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
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

func TestSessionManagerConnectAssignClientID(t *testing.T) {
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

func TestSessionManagerConnectAssignClientIDWithPrefix(t *testing.T) {
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

func TestSessionManagerConnectMaxSessionExpiryInterval(t *testing.T) {
	testCases := []struct {
		interval    uint32
		maxInterval uint32
		resp        uint32
	}{
		{interval: 0, maxInterval: 100, resp: 0},
		{interval: 100, maxInterval: 100, resp: 0},
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

			if tc.resp > 0 {
				assert.NotNil(t, props.SessionExpiryInterval)
				assert.Equal(t, tc.resp, *props.SessionExpiryInterval)
				assert.Equal(t, tc.resp, session.ExpiryInterval)
			} else {
				assert.Nil(t, props.SessionExpiryInterval)
			}
		})
	}
}

func TestSessionManagerConnectMaxKeepAlive(t *testing.T) {
	testCases := []struct {
		version      packet.MQTTVersion
		keepAlive    uint16
		maxKeepAlive uint16
		code         packet.ReasonCode
	}{
		{version: packet.MQTT31, keepAlive: 0, maxKeepAlive: 100,
			code: packet.ReasonCodeV3IdentifierRejected,
		},
		{version: packet.MQTT311, keepAlive: 100, maxKeepAlive: 100,
			code: packet.ReasonCodeV3ConnectionAccepted,
		},
		{version: packet.MQTT31, keepAlive: 101, maxKeepAlive: 100,
			code: packet.ReasonCodeV3IdentifierRejected,
		},
		{version: packet.MQTT311, keepAlive: 501, maxKeepAlive: 500,
			code: packet.ReasonCodeV3IdentifierRejected,
		},
		{version: packet.MQTT311, keepAlive: 65535, maxKeepAlive: 65534,
			code: packet.ReasonCodeV3IdentifierRejected,
		},
		{version: packet.MQTT50, keepAlive: 200, maxKeepAlive: 100,
			code: packet.ReasonCodeV5Success,
		},
	}

	for _, tc := range testCases {
		testName := fmt.Sprintf("v%v-%v/%v", tc.version.String(),
			tc.keepAlive, tc.maxKeepAlive)

		t.Run(testName, func(t *testing.T) {
			conf := newConfiguration()
			conf.MaxKeepAlive = int(tc.maxKeepAlive)
			sm := createSessionManager(conf)

			pkt := &packet.Connect{ClientID: []byte("a"),
				Version: tc.version, KeepAlive: tc.keepAlive}
			session, replies, err := sm.handlePacket("", pkt)
			if tc.code == packet.ReasonCodeV5Success {
				require.Nil(t, err)
				require.NotNil(t, session)
				assert.True(t, session.connected)
			} else {
				require.NotNil(t, err)
				assert.Nil(t, session)
			}

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAck := reply.(*packet.ConnAck)
			assert.Equal(t, tc.code, connAck.ReasonCode)

			if tc.version == packet.MQTT50 {
				require.NotNil(t, session)
				assert.NotZero(t, session.KeepAlive)
				assert.NotNil(t, connAck.Properties)
				assert.NotNil(t, connAck.Properties.ServerKeepAlive)
				assert.Equal(t, tc.maxKeepAlive,
					*connAck.Properties.ServerKeepAlive)
			} else {
				assert.Nil(t, connAck.Properties)
			}
		})
	}
}

func TestSessionManagerConnectMaxInflightMessages(t *testing.T) {
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
			assert.NotNil(t, connAck.Properties)
			assert.True(t, session.connected)
			assert.NotZero(t, session.SessionID)

			if tc.resp > 0 {
				assert.NotNil(t, connAck.Properties)
				assert.NotNil(t, connAck.Properties.ReceiveMaximum)
				assert.Equal(t, tc.resp, *connAck.Properties.ReceiveMaximum)
			} else {
				assert.Nil(t, connAck.Properties.ReceiveMaximum)
			}
		})
	}
}

func TestSessionManagerConnectMaxPacketSize(t *testing.T) {
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
			assert.NotNil(t, connAck.Properties)
			assert.True(t, session.connected)

			if tc.resp > 0 {
				assert.NotNil(t, connAck.Properties)
				assert.NotNil(t, connAck.Properties.MaximumPacketSize)
				assert.Equal(t, tc.resp,
					*connAck.Properties.MaximumPacketSize)
			} else {
				assert.Nil(t, connAck.Properties.MaximumPacketSize)
			}
		})
	}
}

func TestSessionManagerConnectMaximumQoS(t *testing.T) {
	testCases := []struct {
		name   string
		maxQoS packet.QoS
	}{
		{name: "QoS0", maxQoS: packet.QoS0},
		{name: "QoS1", maxQoS: packet.QoS1},
		{name: "QoS2", maxQoS: packet.QoS2},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			conf := newConfiguration()
			conf.MaximumQoS = int(tc.maxQoS)
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

			if tc.maxQoS < packet.QoS2 {
				assert.NotNil(t, connAck.Properties)
				assert.NotNil(t, connAck.Properties.MaximumQoS)
				assert.Equal(t, byte(tc.maxQoS),
					*connAck.Properties.MaximumQoS)
			} else {
				assert.Nil(t, connAck.Properties.MaximumQoS)
			}
		})
	}
}

func TestSessionManagerConnectTopicAliasMaximum(t *testing.T) {
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
			assert.NotNil(t, connAck.Properties)
			assert.True(t, session.connected)

			if tc.maxAlias > 0 {
				assert.NotNil(t, connAck.Properties)
				assert.NotNil(t, connAck.Properties.TopicAliasMaximum)
				assert.Equal(t, tc.maxAlias,
					*connAck.Properties.TopicAliasMaximum)
			} else {
				assert.Nil(t, connAck.Properties.TopicAliasMaximum)
			}
		})
	}
}

func TestSessionManagerConnectRetainAvailable(t *testing.T) {
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
			assert.NotNil(t, connAck.Properties)
			assert.True(t, session.connected)

			if !tc.available {
				assert.NotNil(t, connAck.Properties)
				assert.NotNil(t, connAck.Properties.RetainAvailable)
				assert.Equal(t, byte(0), *connAck.Properties.RetainAvailable)
			} else {
				assert.Nil(t, connAck.Properties.RetainAvailable)
			}
		})
	}
}

func TestSessionManagerConnectWildcardSubsAvailable(t *testing.T) {
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
			assert.NotNil(t, connAck.Properties)
			assert.True(t, session.connected)

			if !tc.available {
				assert.NotNil(t, connAck.Properties)
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

func TestSessionManagerConnectSubscriptionIDAvailable(t *testing.T) {
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
			assert.NotNil(t, connAck.Properties)
			assert.True(t, session.connected)

			if !tc.available {
				assert.NotNil(t, connAck.Properties)
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

func TestSessionManagerConnectSharedSubscriptionAvailable(t *testing.T) {
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
				assert.NotNil(t, connAck.Properties)
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

func TestSessionManagerConnectUserProperty(t *testing.T) {
	conf := newConfiguration()
	conf.UserProperties = map[string]string{"k1": "v1"}
	sm := createSessionManager(conf)

	pkt := &packet.Connect{ClientID: []byte("a"), Version: packet.MQTT50}
	session, replies, err := sm.handlePacket("", pkt)
	require.Nil(t, err)
	require.NotNil(t, session)

	require.Len(t, replies, 1)
	reply := replies[0]
	require.Equal(t, packet.CONNACK, reply.Type())

	connAck := reply.(*packet.ConnAck)
	assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
	assert.NotNil(t, connAck.Properties)
	assert.Equal(t, 1, len(connAck.Properties.UserProperties))
	assert.Equal(t, []byte("k1"), connAck.Properties.UserProperties[0].Key)
	assert.Equal(t, []byte("v1"), connAck.Properties.UserProperties[0].Value)
	assert.True(t, session.connected)
}

func TestSessionManagerConnectWithInflightMessages(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)

	session := &Session{}
	sm.store.sessions["a"] = session
	inflightMsgList := make([]*message, 0)

	for i := 0; i < 10; i++ {
		topic := fmt.Sprintf("data/%v", i)
		newPub := packet.NewPublish(packet.ID(i+1), packet.MQTT311, topic,
			packet.QoS1, 0, 0, nil, nil)
		msg := message{packetID: newPub.PacketID,
			id: messageID(newPub.PacketID), packet: &newPub}
		session.inflightMessages.PushBack(&msg)
		inflightMsgList = append(inflightMsgList, &msg)
	}

	connect := packet.Connect{ClientID: []byte{'a'}, Version: packet.MQTT311}
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
	}
}

func TestSessionManagerPingReq(t *testing.T) {
	id := ClientID('a')
	conf := newConfiguration()
	sm := createSessionManager(conf)

	pkt := &packet.Connect{ClientID: []byte("a"), Version: packet.MQTT311}
	_, _, err := sm.handlePacket("", pkt)
	require.Nil(t, err)

	pingReq := &packet.PingReq{}
	_, replies, err := sm.handlePacket(id, pingReq)
	require.Nil(t, err)

	require.Len(t, replies, 1)
	reply := replies[0]
	assert.Equal(t, packet.PINGRESP, reply.Type())
}

func TestSessionManagerPingReqWithoutConnect(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)

	pkt := &packet.PingReq{}
	_, replies, err := sm.handlePacket("", pkt)
	assert.NotNil(t, err)
	assert.Nil(t, replies)
}

func TestSessionManagerSubscribe(t *testing.T) {
	testCases := []struct {
		id           packet.ID
		version      packet.MQTTVersion
		cleanSession bool
		topic        string
		qos          packet.QoS
	}{
		{id: 1, version: packet.MQTT31, topic: "data", qos: packet.QoS0},
		{id: 2, version: packet.MQTT311, topic: "data/temp",
			qos: packet.QoS1},
		{id: 3, version: packet.MQTT50, topic: "data/temp/#",
			qos: packet.QoS2},
		{id: 4, version: packet.MQTT31, cleanSession: true, topic: "data",
			qos: packet.QoS0},
		{id: 5, version: packet.MQTT311, cleanSession: true, topic: "data/temp",
			qos: packet.QoS1},
		{id: 6, version: packet.MQTT50, cleanSession: true,
			topic: "data/temp/#", qos: packet.QoS2},
	}

	conf := newConfiguration()

	for _, tc := range testCases {
		testName := fmt.Sprintf("%v-%v-%s-%v", tc.id, tc.version.String(),
			tc.topic, tc.qos)

		t.Run(testName, func(t *testing.T) {
			sm := createSessionManager(conf)
			id := ClientID("a")

			pkt := &packet.Connect{ClientID: []byte(id), Version: tc.version}
			_, _, err := sm.handlePacket("", pkt)
			require.Nil(t, err)

			sub := packet.Subscribe{PacketID: tc.id, Version: tc.version,
				Topics: []packet.Topic{{Name: tc.topic, QoS: tc.qos}}}
			_, replies, err := sm.handlePacket(id, &sub)
			require.Nil(t, err)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.SUBACK, reply.Type())

			subAck := reply.(*packet.SubAck)
			assert.Equal(t, tc.id, subAck.PacketID)
			assert.Equal(t, tc.version, subAck.Version)
			assert.Len(t, subAck.ReasonCodes, 1)
			assert.Equal(t, packet.ReasonCode(tc.qos), subAck.ReasonCodes[0])
		})
	}
}

func TestSessionManagerSubscribeError(t *testing.T) {
	testCases := []struct {
		id      packet.ID
		version packet.MQTTVersion
		topic   string
		qos     packet.QoS
	}{
		{id: 1, version: packet.MQTT31, topic: "data#", qos: packet.QoS0},
		{id: 2, version: packet.MQTT311, topic: "data+", qos: packet.QoS1},
		{id: 3, version: packet.MQTT50, topic: "data/#/temp",
			qos: packet.QoS2},
	}

	conf := newConfiguration()

	for _, tc := range testCases {
		testName := fmt.Sprintf("%v-%v-%s-%v", tc.id, tc.version.String(),
			tc.topic, tc.qos)

		t.Run(testName, func(t *testing.T) {
			sm := createSessionManager(conf)
			id := ClientID("a")

			pkt := &packet.Connect{ClientID: []byte(id), Version: tc.version}
			_, _, err := sm.handlePacket("", pkt)
			require.Nil(t, err)

			sub := packet.Subscribe{PacketID: tc.id, Version: tc.version,
				Topics: []packet.Topic{{Name: tc.topic, QoS: tc.qos}}}
			_, replies, err := sm.handlePacket(id, &sub)
			require.Nil(t, err)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.SUBACK, reply.Type())

			subAck := reply.(*packet.SubAck)
			assert.Equal(t, tc.id, subAck.PacketID)
			assert.Equal(t, tc.version, subAck.Version)
			assert.Len(t, subAck.ReasonCodes, 1)
			assert.Equal(t, packet.ReasonCodeV3Failure, subAck.ReasonCodes[0])
		})
	}
}

func TestSessionManagerSubscribeMultipleTopics(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)
	id := ClientID("a")

	pkt := &packet.Connect{ClientID: []byte(id), Version: packet.MQTT311}
	_, _, err := sm.handlePacket("", pkt)
	require.Nil(t, err)

	sub := packet.Subscribe{PacketID: 5, Version: packet.MQTT311,
		Topics: []packet.Topic{
			{Name: "data/temp/0", QoS: packet.QoS0},
			{Name: "data/temp#", QoS: packet.QoS0},
			{Name: "data/temp/1", QoS: packet.QoS1},
			{Name: "data/temp/2", QoS: packet.QoS2},
		},
	}
	_, replies, err := sm.handlePacket(id, &sub)
	require.Nil(t, err)

	require.Len(t, replies, 1)
	reply := replies[0]
	require.Equal(t, packet.SUBACK, reply.Type())

	subAck := reply.(*packet.SubAck)
	assert.Equal(t, sub.PacketID, subAck.PacketID)
	assert.Equal(t, sub.Version, subAck.Version)
	assert.Len(t, subAck.ReasonCodes, 4)
	assert.Equal(t, packet.ReasonCode(packet.QoS0), subAck.ReasonCodes[0])
	assert.Equal(t, packet.ReasonCodeV3Failure, subAck.ReasonCodes[1])
	assert.Equal(t, packet.ReasonCode(packet.QoS1), subAck.ReasonCodes[2])
	assert.Equal(t, packet.ReasonCode(packet.QoS2), subAck.ReasonCodes[3])
}

func TestSessionManagerSubscribeWithSubID(t *testing.T) {
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

	sub := packet.Subscribe{PacketID: 2, Version: packet.MQTT50,
		Properties: props, Topics: []packet.Topic{{Name: "topic"}}}
	_, replies, err := sm.handlePacket(id, &sub)
	assert.Nil(t, err)
	require.NotNil(t, replies)

	require.Len(t, replies, 1)
	reply := replies[0]
	require.Equal(t, packet.SUBACK, reply.Type())
}

func TestSessionManagerSubscribeWithSubIDError(t *testing.T) {
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

func TestSessionManagerUnsubscribe(t *testing.T) {
	testCases := []struct {
		name         string
		id           packet.ID
		version      packet.MQTTVersion
		cleanSession bool
		topic        string
		codes        []packet.ReasonCode
	}{
		{name: "3.1", id: 1, version: packet.MQTT31, topic: "data/0"},
		{name: "3.1.1", id: 2, version: packet.MQTT311, topic: "data/1"},
		{name: "5.0", id: 3, version: packet.MQTT50, topic: "data/2",
			codes: []packet.ReasonCode{packet.ReasonCodeV5Success}},
		{name: "3.1-CleanSession", id: 1, version: packet.MQTT31,
			cleanSession: true, topic: "data/0"},
		{name: "3.1.1-CleanSession", id: 2, version: packet.MQTT311,
			cleanSession: true, topic: "data/1"},
		{name: "5.0-CleanSession", id: 3, version: packet.MQTT50,
			cleanSession: true, topic: "data/2",
			codes: []packet.ReasonCode{packet.ReasonCodeV5Success}},
	}

	conf := newConfiguration()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sm := createSessionManager(conf)
			id := ClientID("a")

			pkt := &packet.Connect{ClientID: []byte(id), Version: tc.version}
			_, _, err := sm.handlePacket("", pkt)
			require.Nil(t, err)

			topics := []packet.Topic{{Name: tc.topic}}
			err = subscribe(sm, id, topics, tc.version)
			require.Nil(t, err)

			unsub := packet.Unsubscribe{PacketID: tc.id,
				Version: tc.version, Topics: []string{tc.topic}}
			_, replies, err := sm.handlePacket(id, &unsub)
			require.Nil(t, err)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.UNSUBACK, reply.Type())

			unsubAck := reply.(*packet.UnsubAck)
			assert.Equal(t, unsub.PacketID, unsubAck.PacketID)
			assert.Equal(t, unsub.Version, unsubAck.Version)

			if tc.version == packet.MQTT50 {
				assert.Equal(t, tc.codes, unsubAck.ReasonCodes)
			} else {
				assert.Empty(t, unsubAck.ReasonCodes)
			}
		})
	}
}

func TestSessionManagerUnsubscribeMissingSubscription(t *testing.T) {
	testCases := []struct {
		name    string
		id      packet.ID
		version packet.MQTTVersion
		topic   string
		codes   []packet.ReasonCode
	}{
		{name: "3.1", id: 1, version: packet.MQTT31, topic: "data/0"},
		{name: "3.1.1", id: 2, version: packet.MQTT311, topic: "data/1"},
		{name: "5.0", id: 3, version: packet.MQTT50, topic: "data/2",
			codes: []packet.ReasonCode{
				packet.ReasonCodeV5NoSubscriptionExisted}},
	}

	conf := newConfiguration()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sm := createSessionManager(conf)
			id := ClientID("a")

			pkt := &packet.Connect{ClientID: []byte(id), Version: tc.version}
			_, _, err := sm.handlePacket("", pkt)
			require.Nil(t, err)

			unsub := packet.Unsubscribe{PacketID: tc.id, Version: tc.version,
				Topics: []string{tc.topic}}
			_, replies, err := sm.handlePacket(id, &unsub)
			require.Nil(t, err)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.UNSUBACK, reply.Type())

			unsubAck := reply.(*packet.UnsubAck)
			assert.Equal(t, unsub.PacketID, unsubAck.PacketID)
			assert.Equal(t, unsub.Version, unsubAck.Version)

			if tc.version == packet.MQTT50 {
				assert.Equal(t, tc.codes, unsubAck.ReasonCodes)
			} else {
				assert.Empty(t, unsubAck.ReasonCodes)
			}
		})
	}
}

func TestSessionManagerUnsubscribeMultipleTopics(t *testing.T) {
	testCases := []struct {
		name    string
		id      packet.ID
		version packet.MQTTVersion
		codes   []packet.ReasonCode
	}{
		{name: "3.1", id: 1, version: packet.MQTT31},
		{name: "3.1.1", id: 2, version: packet.MQTT311},
		{name: "5.0", id: 3, version: packet.MQTT50, codes: []packet.ReasonCode{
			packet.ReasonCodeV5Success,
			packet.ReasonCodeV5Success,
			packet.ReasonCodeV5NoSubscriptionExisted}},
	}

	conf := newConfiguration()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sm := createSessionManager(conf)
			id := ClientID("a")

			pkt := &packet.Connect{ClientID: []byte(id), Version: tc.version}
			_, _, err := sm.handlePacket("", pkt)
			require.Nil(t, err)

			topics := []packet.Topic{
				{Name: "data/temp/0", QoS: packet.QoS0},
				{Name: "data/temp/1", QoS: packet.QoS1},
				{Name: "data/temp/#", QoS: packet.QoS2},
			}

			err = subscribe(sm, id, topics, tc.version)
			require.Nil(t, err)

			topicNames := []string{"data/temp/0", "data/temp/#", "data/temp/2"}
			unsub := packet.Unsubscribe{PacketID: tc.id,
				Version: tc.version, Topics: topicNames}
			_, replies, err := sm.handlePacket(id, &unsub)
			require.Nil(t, err)

			require.Len(t, replies, 1)
			reply := replies[0]
			require.Equal(t, packet.UNSUBACK, reply.Type())

			unsubAck := reply.(*packet.UnsubAck)
			assert.Equal(t, unsub.PacketID, unsubAck.PacketID)
			assert.Equal(t, unsub.Version, unsubAck.Version)

			if tc.version == packet.MQTT50 {
				assert.Equal(t, tc.codes, unsubAck.ReasonCodes)
			} else {
				assert.Empty(t, unsubAck.ReasonCodes)
			}
		})
	}
}

func TestSessionManagerPublish(t *testing.T) {
	testCases := []struct {
		id      packet.ID
		qos     packet.QoS
		version packet.MQTTVersion
		topic   string
		payload string
	}{
		{id: 1, qos: packet.QoS0, version: packet.MQTT31, topic: "data/temp/0",
			payload: "0"},
		{id: 2, qos: packet.QoS0, version: packet.MQTT311, topic: "data/temp/1",
			payload: "1"},
		{id: 3, qos: packet.QoS0, version: packet.MQTT50, topic: "data/temp/2",
			payload: "2"},
		{id: 4, qos: packet.QoS1, version: packet.MQTT31, topic: "data/temp/3",
			payload: "3"},
		{id: 5, qos: packet.QoS1, version: packet.MQTT311, topic: "data/temp/4",
			payload: "4"},
		{id: 6, qos: packet.QoS1, version: packet.MQTT50, topic: "data/temp/5",
			payload: "5"},
	}

	conf := newConfiguration()

	for _, tc := range testCases {
		name := fmt.Sprintf("%v-%v-%v", tc.qos, tc.version.String(),
			tc.id)
		t.Run(name, func(t *testing.T) {
			sm := createSessionManager(conf)
			id := ClientID("a")

			pkt := &packet.Connect{ClientID: []byte(id), Version: tc.version}
			_, _, err := sm.handlePacket("", pkt)
			require.Nil(t, err)

			idGen := sm.pubSub.idGen.(*idGeneratorMock)
			idGen.On("NextID").Return(3)

			pubPkt := packet.NewPublish(tc.id, tc.version, tc.topic,
				tc.qos, 0, 0, []byte(tc.payload), nil)
			_, replies, err := sm.handlePacket(id, &pubPkt)
			require.Nil(t, err)

			if tc.qos > packet.QoS0 {
				require.Len(t, replies, 1)
				reply := replies[0]
				require.Equal(t, packet.PUBACK, reply.Type())

				pubAck := reply.(*packet.PubAck)
				assert.Equal(t, tc.id, pubAck.PacketID)
				assert.Equal(t, tc.version, pubAck.Version)
				assert.Equal(t, packet.ReasonCodeV5Success, pubAck.ReasonCode)
			} else {
				require.Empty(t, replies)
			}
		})
	}
}

func TestSessionManagerPubAck(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)
	id := ClientID("a")

	connect := packet.Connect{ClientID: []byte(id), Version: packet.MQTT311}
	session, _, err := sm.handlePacket("", &connect)
	require.Nil(t, err)

	publish := packet.NewPublish(1, packet.MQTT311, "data",
		packet.QoS1, 0, 0, nil, nil)

	msg := &message{id: 1, packetID: 1, packet: &publish, tries: 1,
		lastSent: time.Now().UnixMicro()}
	session.inflightMessages.PushBack(msg)

	pubAck := packet.NewPubAck(publish.PacketID, publish.Version,
		packet.ReasonCodeV5Success, nil)
	_, replies, err := sm.handlePacket(id, &pubAck)
	require.Nil(t, err)
	assert.Empty(t, replies)
	assert.Zero(t, session.inflightMessages.Len())

	s := sm.store.sessions[session.ClientID]
	assert.Zero(t, s.inflightMessages.Len())
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

func TestSessionManagerDisconnect(t *testing.T) {
	testCases := []struct {
		version      packet.MQTTVersion
		cleanSession bool
	}{
		{version: packet.MQTT31, cleanSession: false},
		{version: packet.MQTT31, cleanSession: true},
		{version: packet.MQTT311, cleanSession: false},
		{version: packet.MQTT311, cleanSession: true},
		{version: packet.MQTT50, cleanSession: false},
		{version: packet.MQTT50, cleanSession: true},
	}

	conf := newConfiguration()

	for _, tc := range testCases {
		name := fmt.Sprintf("%v-%v", tc.version.String(), tc.cleanSession)
		t.Run(name, func(t *testing.T) {
			sm := createSessionManager(conf)
			id := ClientID("a")

			pkt := &packet.Connect{ClientID: []byte(id), Version: tc.version,
				CleanSession: tc.cleanSession}
			session, _, err := sm.handlePacket("", pkt)
			require.Nil(t, err)

			sub, err := sm.pubSub.subscribe(session,
				packet.Topic{Name: "test"}, 1)
			require.Nil(t, err)
			session.Subscriptions["test"] = sub

			disconnect := packet.Disconnect{Properties: &packet.Properties{}}
			_, replies, err := sm.handlePacket(id, &disconnect)
			assert.Nil(t, err)
			require.Empty(t, replies)
			require.NotNil(t, session)
			assert.False(t, session.connected)

			_, ok := sm.sessions[id]
			assert.False(t, ok)

			if tc.cleanSession {
				assert.Empty(t, sm.pubSub.tree.root.children)
			} else {
				assert.NotEmpty(t, sm.pubSub.tree.root.children)
			}
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

	disconnect := packet.Disconnect{Properties: props}
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

	disconnect := packet.Disconnect{Properties: props}
	_, replies, err := sm.handlePacket(id, &disconnect)
	require.NotNil(t, err)

	require.Len(t, replies, 1)
	reply := replies[0]
	assert.Equal(t, packet.DISCONNECT, reply.Type())

	discReply := reply.(*packet.Disconnect)
	assert.Equal(t, packet.MQTT50, discReply.Version)
	assert.Equal(t, packet.ReasonCodeV5ProtocolError, discReply.ReasonCode)
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

	disconnect := packet.Disconnect{Properties: props}
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

	disconnect := packet.Disconnect{}
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

	pingReq := &packet.PingResp{}
	_, reply, err := sm.handlePacket(id, pingReq)
	assert.NotNil(t, err)
	assert.Nil(t, reply)
}

func TestSessionManagerPublishMessage(t *testing.T) {
	testCases := []struct {
		name string
		id   packet.ID
		qos  packet.QoS
	}{
		{name: "QoS0", id: 10, qos: packet.QoS0},
		{name: "QoS1", id: 20, qos: packet.QoS1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)

			session := &Session{}
			session.ClientID = "client-1"
			session.Version = packet.MQTT311
			session.connected = true
			sm.store.sessions[session.ClientID] = session

			pkt := packet.NewPublish(tc.id, packet.MQTT311, "data",
				tc.qos, 0, 0, nil, nil)
			msg := &message{id: messageID(tc.id), packet: &pkt}

			pktDeliverer := sm.deliverer.(*delivererMock)
			pktDeliverer.On("deliverPacket", session.ClientID, mock.Anything).
				Return(nil)

			err := sm.publishMessage(session.ClientID, msg)
			assert.Nil(t, err)
			pub := pktDeliverer.Calls[0].Arguments.Get(1).(*packet.Publish)

			if tc.qos == 0 {
				assert.Zero(t, session.inflightMessages.Len())
				assert.Equal(t, &pkt, pub)
			} else {
				assert.Equal(t, 1, session.inflightMessages.Len())

				firstMsg := session.inflightMessages.Front().Value.(*message)
				require.NotNil(t, firstMsg)
				assert.Equal(t, messageID(tc.id), firstMsg.id)
				assert.Equal(t, packet.ID(session.lastPacketID),
					firstMsg.packetID)
				assert.Equal(t, msg.packet.QoS, firstMsg.packet.QoS)
				assert.Equal(t, msg.packet.Version, firstMsg.packet.Version)
				assert.Equal(t, msg.packet.TopicName,
					firstMsg.packet.TopicName)
				assert.Equal(t, msg.packet.Payload, firstMsg.packet.Payload)
				assert.NotZero(t, firstMsg.lastSent)
				assert.Equal(t, 1, firstMsg.tries)
				assert.Equal(t, firstMsg.packet, pub)

				s := sm.store.sessions[session.ClientID]
				assert.Equal(t, 1, s.inflightMessages.Len())
				m := s.inflightMessages.Front().Value.(*message)
				assert.Equal(t, firstMsg, m)
			}
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
	testCases := []struct {
		name string
		id   packet.ID
		qos  packet.QoS
	}{
		{name: "QoS0", id: 10, qos: packet.QoS0},
		{name: "QoS1", id: 20, qos: packet.QoS1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)

			session := &Session{}
			session.ClientID = "client-1"
			session.connected = false
			sm.store.sessions[session.ClientID] = session

			pkt := packet.NewPublish(tc.id, packet.MQTT311, "data",
				tc.qos, 0, 0, nil, nil)
			msg := message{id: messageID(tc.id), packet: &pkt}

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
