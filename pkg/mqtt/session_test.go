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

	"github.com/gsalomao/maxmq/mocks"
	packet "github.com/gsalomao/maxmq/pkg/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type delivererMock struct {
	mock.Mock
}

func (d *delivererMock) deliverPacket(id SessionID, pkt *packet.Publish) error {
	args := d.Called(id, pkt)
	return args.Error(0)
}

func createSessionManager(conf Configuration) *sessionManager {
	logger := mocks.NewLoggerStub()
	packetDeliverer := delivererMock{}

	userProps := make([]packet.UserProperty, 0, len(conf.UserProperties))
	for k, v := range conf.UserProperties {
		userProps = append(userProps,
			packet.UserProperty{Key: []byte(k), Value: []byte(v)})
	}

	m := newMetrics(true, logger.Logger())
	idGen := &idGeneratorMock{}
	idGen.On("NextID").Return(1)

	return newSessionManager(&packetDeliverer, idGen, &conf, m,
		userProps, logger.Logger())
}

func checkConnect(t *testing.T, conf Configuration, pkt *packet.Connect,
	success bool) (*Session, *packet.ConnAck) {

	sm := createSessionManager(conf)

	session, replies, err := sm.handlePacket(nil, pkt)
	if success {
		assert.Nil(t, err)
		require.NotNil(t, session)
		assert.True(t, session.connected)
		assert.NotZero(t, session.SessionID)
	} else {
		assert.NotNil(t, err)
		assert.Nil(t, session)
	}

	require.Len(t, replies, 1)
	reply := replies[0]
	assert.Equal(t, packet.CONNACK, reply.Type())

	connAck := reply.(*packet.ConnAck)
	assert.Equal(t, pkt.Version, connAck.Version)
	return session, connAck
}

func connectClient(sm *sessionManager, version packet.MQTTVersion,
	cleanSession bool, props *packet.Properties) (*Session, error) {

	pkt := packet.Connect{ClientID: ClientID{'a'}, Version: version,
		CleanSession: cleanSession, Properties: props}
	session, _, err := sm.handlePacket(nil, &pkt)
	return session, err
}

func subscribe(sm *sessionManager, session *Session, topics []packet.Topic,
	version packet.MQTTVersion) error {

	sub := packet.Subscribe{PacketID: 5, Version: version, Topics: topics}
	_, _, err := sm.handlePacket(session, &sub)
	if err != nil {
		return err
	}

	return nil
}

func TestSessionManager_ConnectNewSession(t *testing.T) {
	testCases := []struct {
		version packet.MQTTVersion
		code    packet.ReasonCode
	}{
		{version: packet.MQTT31, code: packet.ReasonCodeV3ConnectionAccepted},
		{version: packet.MQTT311,
			code: packet.ReasonCodeV3ConnectionAccepted},
		{version: packet.MQTT50, code: packet.ReasonCodeV5Success},
	}

	for _, test := range testCases {
		t.Run(test.version.String(), func(t *testing.T) {
			conf := newConfiguration()
			pkt := packet.Connect{ClientID: ClientID{'a'}, KeepAlive: 600,
				Version: test.version, Properties: &packet.Properties{}}

			session, connAck := checkConnect(t, conf, &pkt, true)
			assert.Equal(t, test.code, connAck.ReasonCode)
			assert.False(t, connAck.SessionPresent)
			assert.Equal(t, ClientID(pkt.ClientID), session.ClientID)
			assert.Equal(t, int(pkt.KeepAlive), session.KeepAlive)
		})
	}
}

func TestSessionManager_ConnectExistingSession(t *testing.T) {
	testCases := []struct {
		version packet.MQTTVersion
		code    packet.ReasonCode
	}{
		{version: packet.MQTT31, code: packet.ReasonCodeV3ConnectionAccepted},
		{version: packet.MQTT311,
			code: packet.ReasonCodeV3ConnectionAccepted},
		{version: packet.MQTT50, code: packet.ReasonCodeV5Success},
	}

	for _, test := range testCases {
		t.Run(test.version.String(), func(t *testing.T) {
			conf := newConfiguration()
			clientID := ClientID{'a'}
			pkt := packet.Connect{ClientID: clientID, Version: test.version}

			sm := createSessionManager(conf)
			s := &Session{
				ClientID:       clientID,
				ConnectedAt:    time.Now().Add(-1 * time.Minute).Unix(),
				ExpiryInterval: conf.MaxSessionExpiryInterval,
				Version:        test.version,
				Subscriptions:  make(map[string]Subscription),
			}

			sm.store.sessions[string(s.ClientID)] = s

			session, replies, err := sm.handlePacket(nil, &pkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)
			require.NotNil(t, session)
			assert.Equal(t, ClientID(pkt.ClientID), session.ClientID)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAck := reply.(*packet.ConnAck)
			assert.Equal(t, test.version, connAck.Version)
			assert.Equal(t, test.code, connAck.ReasonCode)
			assert.True(t, connAck.SessionPresent)
		})
	}
}

func TestSessionManager_ConnectCleanSessionNoExisting(t *testing.T) {
	testCases := []struct {
		version packet.MQTTVersion
		code    packet.ReasonCode
	}{
		{version: packet.MQTT31, code: packet.ReasonCodeV3ConnectionAccepted},
		{version: packet.MQTT311,
			code: packet.ReasonCodeV3ConnectionAccepted},
		{version: packet.MQTT50, code: packet.ReasonCodeV5Success},
	}

	for _, test := range testCases {
		t.Run(test.version.String(), func(t *testing.T) {
			conf := newConfiguration()
			clientID := ClientID{'a'}
			pkt := packet.Connect{ClientID: clientID, Version: test.version,
				CleanSession: true}

			sm := createSessionManager(conf)

			session, replies, err := sm.handlePacket(nil, &pkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)
			require.NotNil(t, session)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAck := reply.(*packet.ConnAck)
			assert.Equal(t, test.version, connAck.Version)
			assert.Equal(t, test.code, connAck.ReasonCode)
			assert.False(t, connAck.SessionPresent)
		})
	}
}

func TestSessionManager_ConnectCleanSessionExisting(t *testing.T) {
	testCases := []struct {
		version packet.MQTTVersion
		code    packet.ReasonCode
	}{
		{version: packet.MQTT31, code: packet.ReasonCodeV3ConnectionAccepted},
		{version: packet.MQTT311,
			code: packet.ReasonCodeV3ConnectionAccepted},
		{version: packet.MQTT50, code: packet.ReasonCodeV5Success},
	}

	for _, test := range testCases {
		t.Run(test.version.String(), func(t *testing.T) {
			conf := newConfiguration()
			clientID := ClientID{'a'}
			pkt := packet.Connect{ClientID: clientID, Version: test.version,
				CleanSession: true}

			sm := createSessionManager(conf)
			s := &Session{
				ClientID:       clientID,
				ConnectedAt:    time.Now().Add(-1 * time.Minute).Unix(),
				ExpiryInterval: conf.MaxSessionExpiryInterval,
				Version:        test.version,
				Subscriptions:  make(map[string]Subscription),
			}

			sub, err := sm.pubSub.subscribe(s, packet.Topic{Name: "test"}, 1)
			require.Nil(t, err)
			s.Subscriptions["test"] = sub

			sm.store.sessions[string(s.ClientID)] = s

			session, replies, err := sm.handlePacket(nil, &pkt)
			require.Nil(t, err)
			require.Len(t, replies, 1)
			require.NotNil(t, session)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAck := reply.(*packet.ConnAck)
			assert.Equal(t, test.version, connAck.Version)
			assert.Equal(t, test.code, connAck.ReasonCode)
			assert.False(t, connAck.SessionPresent)
			assert.Equal(t, clientID, session.ClientID)
			assert.Empty(t, session.Subscriptions)
			assert.Empty(t, sm.pubSub.tree.root.children)
		})
	}
}

func TestSessionManager_ConnectClientIDTooBig(t *testing.T) {
	testCases := []struct {
		id       []byte
		maxIDLen int
		version  packet.MQTTVersion
		code     packet.ReasonCode
	}{
		{id: []byte("012345678901234567890123"), version: packet.MQTT31,
			maxIDLen: 65535, code: packet.ReasonCodeV3IdentifierRejected,
		},
		{id: []byte("0123456789012345678901234567890"),
			version:  packet.MQTT311,
			maxIDLen: 30, code: packet.ReasonCodeV3IdentifierRejected,
		},
		{id: []byte("0123456789012345678901234567890"), version: packet.MQTT50,
			maxIDLen: 30, code: packet.ReasonCodeV5InvalidClientID,
		},
	}

	for _, test := range testCases {
		t.Run(test.version.String(), func(t *testing.T) {
			conf := newConfiguration()
			conf.MaxClientIDLen = test.maxIDLen

			pkt := packet.Connect{ClientID: test.id, Version: test.version}
			_, connAck := checkConnect(t, conf, &pkt, false)
			assert.Equal(t, test.code, connAck.ReasonCode)
		})
	}
}

func TestSessionManager_ConnectAllowEmptyClientID(t *testing.T) {
	testCases := []struct {
		version packet.MQTTVersion
		code    packet.ReasonCode
	}{
		{version: packet.MQTT31, code: packet.ReasonCodeV3ConnectionAccepted},
		{version: packet.MQTT311,
			code: packet.ReasonCodeV3ConnectionAccepted},
		{version: packet.MQTT50, code: packet.ReasonCodeV5Success},
	}

	for _, test := range testCases {
		t.Run(test.version.String(), func(t *testing.T) {
			conf := newConfiguration()
			conf.AllowEmptyClientID = true

			pkt := packet.Connect{Version: test.version}
			session, connAck := checkConnect(t, conf, &pkt, true)
			assert.Equal(t, test.code, connAck.ReasonCode)
			assert.Greater(t, len(session.ClientID), 0)
		})
	}
}

func TestSessionManager_ConnectDenyEmptyClientID(t *testing.T) {
	testCases := []struct {
		version packet.MQTTVersion
		code    packet.ReasonCode
	}{
		{version: packet.MQTT31, code: packet.ReasonCodeV3IdentifierRejected},
		{version: packet.MQTT311,
			code: packet.ReasonCodeV3IdentifierRejected},
		{version: packet.MQTT50, code: packet.ReasonCodeV5InvalidClientID},
	}

	for _, test := range testCases {
		t.Run(test.version.String(), func(t *testing.T) {
			conf := newConfiguration()
			conf.AllowEmptyClientID = false

			pkt := packet.Connect{Version: test.version}
			_, connAck := checkConnect(t, conf, &pkt, false)
			assert.Equal(t, test.code, connAck.ReasonCode)
		})
	}
}

func TestSessionManager_ConnectAssignClientID(t *testing.T) {
	conf := newConfiguration()
	conf.AllowEmptyClientID = true

	pkt := packet.Connect{Version: packet.MQTT50}
	session, connAck := checkConnect(t, conf, &pkt, true)
	assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
	assert.NotNil(t, connAck.Properties)
	assert.NotNil(t, connAck.Properties.AssignedClientID)
	assert.Equal(t, 20, len(connAck.Properties.AssignedClientID))
	assert.Equal(t, ClientID(connAck.Properties.AssignedClientID),
		session.ClientID)
}

func TestSessionManager_ConnectAssignClientIDWithPrefix(t *testing.T) {
	conf := newConfiguration()
	conf.AllowEmptyClientID = true
	conf.ClientIDPrefix = []byte("MAX-")

	pkt := packet.Connect{Version: packet.MQTT50}
	session, connAck := checkConnect(t, conf, &pkt, true)
	assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
	assert.NotNil(t, connAck.Properties)
	assert.NotNil(t, connAck.Properties.AssignedClientID)
	assert.Equal(t, 24, len(connAck.Properties.AssignedClientID))
	assert.Equal(t, conf.ClientIDPrefix,
		connAck.Properties.AssignedClientID[:4])
	assert.Equal(t, ClientID(connAck.Properties.AssignedClientID),
		session.ClientID)
}

func TestSessionManager_ConnectMaxSessionExpiryInterval(t *testing.T) {
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

	for _, test := range testCases {
		testName := fmt.Sprintf("%v-Max%v", test.interval, test.maxInterval)

		t.Run(testName, func(t *testing.T) {
			conf := newConfiguration()
			conf.MaxSessionExpiryInterval = test.maxInterval

			pkt := packet.Connect{
				ClientID: ClientID{'a'},
				Version:  packet.MQTT50,
				Properties: &packet.Properties{
					SessionExpiryInterval: &test.interval,
				},
			}
			session, connAck := checkConnect(t, conf, &pkt, true)
			assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
			assert.NotNil(t, connAck.Properties)
			props := connAck.Properties

			if test.resp > 0 {
				assert.NotNil(t, props.SessionExpiryInterval)
				assert.Equal(t, test.resp, *props.SessionExpiryInterval)
				assert.Equal(t, test.resp, session.ExpiryInterval)
			} else {
				assert.Nil(t, props.SessionExpiryInterval)
			}
		})
	}
}

func TestSessionManager_ConnectMaxKeepAlive(t *testing.T) {
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

	for _, test := range testCases {
		testName := fmt.Sprintf("v%v-%v/%v", test.version.String(),
			test.keepAlive, test.maxKeepAlive)

		t.Run(testName, func(t *testing.T) {
			conf := newConfiguration()
			conf.MaxKeepAlive = int(test.maxKeepAlive)

			pkt := packet.Connect{ClientID: ClientID{'a'},
				Version: test.version, KeepAlive: test.keepAlive}

			session, connAck := checkConnect(t, conf, &pkt,
				test.code == packet.ReasonCodeV5Success)
			assert.Equal(t, test.code, connAck.ReasonCode)

			if test.version == packet.MQTT50 {
				assert.NotZero(t, session.KeepAlive)
				assert.NotNil(t, connAck.Properties)
				assert.NotNil(t, connAck.Properties.ServerKeepAlive)
				assert.Equal(t, test.maxKeepAlive,
					*connAck.Properties.ServerKeepAlive)
			} else {
				assert.Nil(t, connAck.Properties)
			}
		})
	}
}

func TestSessionManager_ConnectMaxInflightMessages(t *testing.T) {
	testCases := []struct {
		maxInflight uint16
		resp        uint16
	}{
		{maxInflight: 0, resp: 0},
		{maxInflight: 255, resp: 255},
		{maxInflight: 65534, resp: 65534},
		{maxInflight: 65535, resp: 0},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprint(test.maxInflight), func(t *testing.T) {
			conf := newConfiguration()
			conf.MaxInflightMessages = int(test.maxInflight)

			pkt := packet.Connect{ClientID: ClientID{'a'},
				Version: packet.MQTT50, Properties: &packet.Properties{}}

			_, connAck := checkConnect(t, conf, &pkt, true)
			assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
			assert.NotNil(t, connAck.Properties)

			if test.resp > 0 {
				assert.NotNil(t, connAck.Properties)
				assert.NotNil(t, connAck.Properties.ReceiveMaximum)
				assert.Equal(t, test.resp, *connAck.Properties.ReceiveMaximum)
			} else {
				assert.Nil(t, connAck.Properties.ReceiveMaximum)
			}
		})
	}
}

func TestSessionManager_ConnectMaxPacketSize(t *testing.T) {
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

	for _, test := range testCases {
		t.Run(fmt.Sprint(test.maxSize), func(t *testing.T) {
			conf := newConfiguration()
			conf.MaxPacketSize = int(test.maxSize)

			pkt := packet.Connect{ClientID: ClientID{'a'},
				Version: packet.MQTT50}

			_, connAck := checkConnect(t, conf, &pkt, true)
			assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
			assert.NotNil(t, connAck.Properties)

			if test.resp > 0 {
				assert.NotNil(t, connAck.Properties)
				assert.NotNil(t, connAck.Properties.MaximumPacketSize)
				assert.Equal(t, test.resp,
					*connAck.Properties.MaximumPacketSize)
			} else {
				assert.Nil(t, connAck.Properties.MaximumPacketSize)
			}
		})
	}
}

func TestSessionManager_ConnectMaximumQoS(t *testing.T) {
	testCases := []struct{ maxQoS packet.QoS }{
		{maxQoS: packet.QoS0},
		{maxQoS: packet.QoS1},
		{maxQoS: packet.QoS2},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprint(test.maxQoS), func(t *testing.T) {
			conf := newConfiguration()
			conf.MaximumQoS = int(test.maxQoS)

			pkt := packet.Connect{ClientID: ClientID{'a'},
				Version: packet.MQTT50}

			_, connAck := checkConnect(t, conf, &pkt, true)
			assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
			assert.NotNil(t, connAck.Properties)

			if test.maxQoS < packet.QoS2 {
				assert.NotNil(t, connAck.Properties)
				assert.NotNil(t, connAck.Properties.MaximumQoS)
				assert.Equal(t, byte(test.maxQoS),
					*connAck.Properties.MaximumQoS)
			} else {
				assert.Nil(t, connAck.Properties.MaximumQoS)
			}
		})
	}
}

func TestSessionManager_ConnectTopicAliasMaximum(t *testing.T) {
	testCases := []struct{ maxAlias uint16 }{
		{maxAlias: 0},
		{maxAlias: 255},
		{maxAlias: 65535},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprint(test.maxAlias), func(t *testing.T) {
			conf := newConfiguration()
			conf.MaxTopicAlias = int(test.maxAlias)

			pkt := packet.Connect{ClientID: ClientID{'a'},
				Version: packet.MQTT50}

			_, connAck := checkConnect(t, conf, &pkt, true)
			assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
			assert.NotNil(t, connAck.Properties)

			if test.maxAlias > 0 {
				assert.NotNil(t, connAck.Properties)
				assert.NotNil(t, connAck.Properties.TopicAliasMaximum)
				assert.Equal(t, test.maxAlias,
					*connAck.Properties.TopicAliasMaximum)
			} else {
				assert.Nil(t, connAck.Properties.TopicAliasMaximum)
			}
		})
	}
}

func TestSessionManager_ConnectRetainAvailable(t *testing.T) {
	testCases := []struct{ available bool }{
		{available: false},
		{available: true},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprint(test.available), func(t *testing.T) {
			conf := newConfiguration()
			conf.RetainAvailable = test.available

			pkt := packet.Connect{ClientID: ClientID{'a'},
				Version: packet.MQTT50}

			_, connAck := checkConnect(t, conf, &pkt, true)
			assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
			assert.NotNil(t, connAck.Properties)

			if !test.available {
				assert.NotNil(t, connAck.Properties)
				assert.NotNil(t, connAck.Properties.RetainAvailable)
				assert.Equal(t, byte(0), *connAck.Properties.RetainAvailable)
			} else {
				assert.Nil(t, connAck.Properties.RetainAvailable)
			}
		})
	}
}

func TestSessionManager_ConnectWildcardSubsAvailable(t *testing.T) {
	testCases := []struct{ available bool }{
		{available: false},
		{available: true},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprint(test.available), func(t *testing.T) {
			conf := newConfiguration()
			conf.WildcardSubscriptionAvailable = test.available

			pkt := packet.Connect{ClientID: ClientID{'a'},
				Version: packet.MQTT50}

			_, connAck := checkConnect(t, conf, &pkt, true)
			assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
			assert.NotNil(t, connAck.Properties)

			if !test.available {
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

func TestSessionManager_ConnectSubscriptionIDAvailable(t *testing.T) {
	testCases := []struct{ available bool }{
		{available: false},
		{available: true},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprint(test.available), func(t *testing.T) {
			conf := newConfiguration()
			conf.SubscriptionIDAvailable = test.available

			pkt := packet.Connect{ClientID: ClientID{'a'},
				Version: packet.MQTT50}

			_, connAck := checkConnect(t, conf, &pkt, true)
			assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
			assert.NotNil(t, connAck.Properties)

			if !test.available {
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

func TestSessionManager_ConnectSharedSubscriptionAvailable(t *testing.T) {
	testCases := []struct{ available bool }{
		{available: false},
		{available: true},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprint(test.available), func(t *testing.T) {
			conf := newConfiguration()
			conf.SharedSubscriptionAvailable = test.available

			pkt := packet.Connect{ClientID: ClientID{'a'},
				Version: packet.MQTT50}

			_, connAck := checkConnect(t, conf, &pkt, true)
			assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
			assert.NotNil(t, connAck.Properties)

			if !test.available {
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

func TestSessionManager_ConnectUserProperty(t *testing.T) {
	conf := newConfiguration()
	conf.UserProperties = map[string]string{"k1": "v1"}

	pkt := packet.Connect{ClientID: ClientID{'a'}, Version: packet.MQTT50}
	_, connAck := checkConnect(t, conf, &pkt, true)
	assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
	assert.NotNil(t, connAck.Properties)
	assert.Equal(t, 1, len(connAck.Properties.UserProperties))
	assert.Equal(t, []byte("k1"), connAck.Properties.UserProperties[0].Key)
	assert.Equal(t, []byte("v1"), connAck.Properties.UserProperties[0].Value)
}

func TestSessionManager_ConnectWithInflightMessages(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)

	session := &Session{}
	sm.store.sessions["a"] = session
	inflightMsgList := make([]*inflightMessage, 0)

	for i := 0; i < 10; i++ {
		topic := fmt.Sprintf("data/%v", i)
		newPub := packet.NewPublish(packet.ID(i+1), packet.MQTT311, topic,
			packet.QoS1, 0, 0, nil, nil)
		msg := inflightMessage{packetID: newPub.PacketID,
			messageID: messageID(newPub.PacketID), packet: &newPub}
		session.inflightMessages.add(&msg)
		inflightMsgList = append(inflightMsgList, &msg)
	}

	connect := packet.Connect{ClientID: ClientID{'a'},
		Version: packet.MQTT311}

	session, replies, err := sm.handlePacket(nil, &connect)
	require.Nil(t, err)
	require.Len(t, replies, 11)
	require.NotNil(t, session)

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

func TestSessionManager_PingReq(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)

	session, err := connectClient(sm, packet.MQTT311, false, nil)
	require.Nil(t, err)

	pingReq := packet.PingReq{}
	session, replies, err := sm.handlePacket(session, &pingReq)
	require.Nil(t, err)
	require.Len(t, replies, 1)
	require.NotNil(t, session)

	reply := replies[0]
	assert.Equal(t, packet.PINGRESP, reply.Type())
}

func TestSessionManager_PingReqWithoutConnect(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)

	pkt := packet.PingReq{}
	session, reply, err := sm.handlePacket(nil, &pkt)
	assert.NotNil(t, err)
	assert.Nil(t, reply)
	assert.Nil(t, session)
}

func TestSessionManager_Subscribe(t *testing.T) {
	testCases := []struct {
		id      packet.ID
		version packet.MQTTVersion
		topic   string
		qos     packet.QoS
	}{
		{id: 1, version: packet.MQTT31, topic: "data", qos: packet.QoS0},
		{id: 2, version: packet.MQTT311, topic: "data/temp",
			qos: packet.QoS1},
		{id: 3, version: packet.MQTT50, topic: "data/temp/#",
			qos: packet.QoS2},
	}

	conf := newConfiguration()

	for _, test := range testCases {
		testName := fmt.Sprintf("%v-%v-%s-%v", test.id, test.version.String(),
			test.topic, test.qos)

		t.Run(testName, func(t *testing.T) {
			sm := createSessionManager(conf)

			session, err := connectClient(sm, test.version, false, nil)
			require.Nil(t, err)

			sub := packet.Subscribe{PacketID: test.id, Version: test.version,
				Topics: []packet.Topic{{Name: test.topic, QoS: test.qos}}}

			session, replies, err := sm.handlePacket(session, &sub)
			require.Nil(t, err)
			require.Len(t, replies, 1)
			require.NotNil(t, session)

			reply := replies[0]
			require.Equal(t, packet.SUBACK, reply.Type())

			subAck := reply.(*packet.SubAck)
			assert.Equal(t, test.id, subAck.PacketID)
			assert.Equal(t, test.version, subAck.Version)
			assert.Len(t, subAck.ReasonCodes, 1)
			assert.Equal(t, packet.ReasonCode(test.qos), subAck.ReasonCodes[0])
		})
	}
}

func TestSessionManager_SubscribeCleanSession(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)

	session, err := connectClient(sm, packet.MQTT311, true, nil)
	require.Nil(t, err)

	sub := packet.Subscribe{PacketID: 1, Version: packet.MQTT311,
		Topics: []packet.Topic{{Name: "data/#"}}}

	session, replies, err := sm.handlePacket(session, &sub)
	require.Nil(t, err)
	require.Len(t, replies, 1)
	require.NotNil(t, session)

	reply := replies[0]
	require.Equal(t, packet.SUBACK, reply.Type())

	subAck := reply.(*packet.SubAck)
	assert.Equal(t, sub.PacketID, subAck.PacketID)
	assert.Equal(t, sub.Version, subAck.Version)
	assert.Len(t, subAck.ReasonCodes, 1)
	assert.Equal(t, packet.ReasonCodeV3GrantedQoS0, subAck.ReasonCodes[0])
}

func TestSessionManager_SubscribeError(t *testing.T) {
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

	for _, test := range testCases {
		testName := fmt.Sprintf("%v-%v-%s-%v", test.id, test.version.String(),
			test.topic, test.qos)

		t.Run(testName, func(t *testing.T) {
			sm := createSessionManager(conf)

			session, err := connectClient(sm, test.version, false, nil)
			require.Nil(t, err)

			sub := packet.Subscribe{PacketID: test.id, Version: test.version,
				Topics: []packet.Topic{{Name: test.topic, QoS: test.qos}}}

			session, replies, err := sm.handlePacket(session, &sub)
			require.Nil(t, err)
			require.Len(t, replies, 1)
			require.NotNil(t, session)

			reply := replies[0]
			require.Equal(t, packet.SUBACK, reply.Type())

			subAck := reply.(*packet.SubAck)
			assert.Equal(t, test.id, subAck.PacketID)
			assert.Equal(t, test.version, subAck.Version)
			assert.Len(t, subAck.ReasonCodes, 1)
			assert.Equal(t, packet.ReasonCodeV3Failure, subAck.ReasonCodes[0])
		})
	}
}

func TestSessionManager_SubscribeMultipleTopics(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)

	session, err := connectClient(sm, packet.MQTT311, false, nil)
	require.Nil(t, err)

	sub := packet.Subscribe{PacketID: 5, Version: packet.MQTT311,
		Topics: []packet.Topic{
			{Name: "data/temp/0", QoS: packet.QoS0},
			{Name: "data/temp#", QoS: packet.QoS0},
			{Name: "data/temp/1", QoS: packet.QoS1},
			{Name: "data/temp/2", QoS: packet.QoS2},
		},
	}

	session, replies, err := sm.handlePacket(session, &sub)
	require.Nil(t, err)
	require.Len(t, replies, 1)
	require.NotNil(t, session)

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

func TestSessionManager_SubscribeWithSubID(t *testing.T) {
	conf := newConfiguration()
	conf.SubscriptionIDAvailable = true

	sm := createSessionManager(conf)

	session, err := connectClient(sm, packet.MQTT50, false, nil)
	require.Nil(t, err)

	props := &packet.Properties{}
	props.SubscriptionIdentifier = new(int)
	*props.SubscriptionIdentifier = 1

	sub := packet.Subscribe{PacketID: 2, Version: packet.MQTT50,
		Properties: props, Topics: []packet.Topic{{Name: "topic"}}}

	_, reply, err := sm.handlePacket(session, &sub)
	assert.Nil(t, err)
	require.NotNil(t, reply)
}

func TestSessionManager_SubscribeWithSubIDError(t *testing.T) {
	conf := newConfiguration()
	conf.SubscriptionIDAvailable = false

	sm := createSessionManager(conf)

	session, err := connectClient(sm, packet.MQTT50, false, nil)
	require.Nil(t, err)

	props := &packet.Properties{}
	props.SubscriptionIdentifier = new(int)
	*props.SubscriptionIdentifier = 1

	sub := packet.Subscribe{PacketID: 2, Version: packet.MQTT50,
		Properties: props, Topics: []packet.Topic{{Name: "topic"}}}

	session, replies, err := sm.handlePacket(session, &sub)
	require.NotNil(t, err)
	require.Len(t, replies, 1)
	require.NotNil(t, session)

	reply := replies[0]
	require.Equal(t, packet.DISCONNECT, reply.Type())

	disconnect := reply.(*packet.Disconnect)
	assert.Equal(t, packet.MQTT50, disconnect.Version)
	assert.Equal(t, packet.ReasonCodeV5SubscriptionIDNotSupported,
		disconnect.ReasonCode)
}

func TestSessionManager_Unsubscribe(t *testing.T) {
	testCases := []struct {
		id      packet.ID
		version packet.MQTTVersion
		topic   string
		codes   []packet.ReasonCode
	}{
		{id: 1, version: packet.MQTT31, topic: "data/0"},
		{id: 2, version: packet.MQTT311, topic: "data/1"},
		{id: 3, version: packet.MQTT50, topic: "data/2",
			codes: []packet.ReasonCode{packet.ReasonCodeV5Success}},
	}

	conf := newConfiguration()

	for _, test := range testCases {
		testName := fmt.Sprintf("%v-%v-%s", test.id, test.version.String(),
			test.topic)

		t.Run(testName, func(t *testing.T) {
			sm := createSessionManager(conf)

			session, err := connectClient(sm, test.version, false, nil)
			require.Nil(t, err)

			topics := []packet.Topic{{Name: test.topic}}
			err = subscribe(sm, session, topics, test.version)
			require.Nil(t, err)

			unsub := packet.Unsubscribe{PacketID: test.id,
				Version: test.version, Topics: []string{test.topic}}

			session, replies, err := sm.handlePacket(session, &unsub)
			require.Nil(t, err)
			require.Len(t, replies, 1)
			require.NotNil(t, session)

			reply := replies[0]
			require.Equal(t, packet.UNSUBACK, reply.Type())

			unsubAck := reply.(*packet.UnsubAck)
			assert.Equal(t, unsub.PacketID, unsubAck.PacketID)
			assert.Equal(t, unsub.Version, unsubAck.Version)

			if test.version == packet.MQTT50 {
				assert.Equal(t, test.codes, unsubAck.ReasonCodes)
			} else {
				assert.Empty(t, unsubAck.ReasonCodes)
			}
		})
	}
}

func TestSessionManager_UnsubscribeCleanSession(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)

	session, err := connectClient(sm, packet.MQTT311, true, nil)
	require.Nil(t, err)

	topics := []packet.Topic{{Name: "test"}}
	err = subscribe(sm, session, topics, packet.MQTT311)
	require.Nil(t, err)

	unsub := packet.Unsubscribe{PacketID: 1, Version: packet.MQTT311,
		Topics: []string{"test"}}

	session, replies, err := sm.handlePacket(session, &unsub)
	require.Nil(t, err)
	require.Len(t, replies, 1)
	require.NotNil(t, session)

	reply := replies[0]
	require.Equal(t, packet.UNSUBACK, reply.Type())

	unsubAck := reply.(*packet.UnsubAck)
	assert.Equal(t, unsub.PacketID, unsubAck.PacketID)
	assert.Equal(t, unsub.Version, unsubAck.Version)
	assert.Empty(t, unsubAck.ReasonCodes)
}

func TestSessionManager_UnsubscribeMissingSubscription(t *testing.T) {
	testCases := []struct {
		id      packet.ID
		version packet.MQTTVersion
		topic   string
		codes   []packet.ReasonCode
	}{
		{id: 1, version: packet.MQTT31, topic: "data/0"},
		{id: 2, version: packet.MQTT311, topic: "data/1"},
		{id: 3, version: packet.MQTT50, topic: "data/2",
			codes: []packet.ReasonCode{
				packet.ReasonCodeV5NoSubscriptionExisted}},
	}

	conf := newConfiguration()

	for _, test := range testCases {
		testName := fmt.Sprintf("%v-%v-%s", test.id, test.version.String(),
			test.topic)

		t.Run(testName, func(t *testing.T) {
			sm := createSessionManager(conf)

			session, err := connectClient(sm, test.version, false, nil)
			require.Nil(t, err)

			unsub := packet.Unsubscribe{PacketID: test.id,
				Version: test.version, Topics: []string{test.topic}}

			session, replies, err := sm.handlePacket(session, &unsub)
			require.Nil(t, err)
			require.Len(t, replies, 1)
			require.NotNil(t, session)

			reply := replies[0]
			require.Equal(t, packet.UNSUBACK, reply.Type())

			unsubAck := reply.(*packet.UnsubAck)
			assert.Equal(t, unsub.PacketID, unsubAck.PacketID)
			assert.Equal(t, unsub.Version, unsubAck.Version)

			if test.version == packet.MQTT50 {
				assert.Equal(t, test.codes, unsubAck.ReasonCodes)
			} else {
				assert.Empty(t, unsubAck.ReasonCodes)
			}
		})
	}
}

func TestSessionManager_UnsubscribeMultipleTopics(t *testing.T) {
	testCases := []struct {
		id      packet.ID
		version packet.MQTTVersion
		codes   []packet.ReasonCode
	}{
		{id: 1, version: packet.MQTT31},
		{id: 2, version: packet.MQTT311},
		{id: 3, version: packet.MQTT50, codes: []packet.ReasonCode{
			packet.ReasonCodeV5Success,
			packet.ReasonCodeV5Success,
			packet.ReasonCodeV5NoSubscriptionExisted}},
	}

	conf := newConfiguration()

	for _, test := range testCases {
		testName := fmt.Sprintf("%v-%v", test.id, test.version.String())

		t.Run(testName, func(t *testing.T) {
			sm := createSessionManager(conf)

			session, err := connectClient(sm, test.version, false, nil)
			require.Nil(t, err)

			topics := []packet.Topic{
				{Name: "data/temp/0", QoS: packet.QoS0},
				{Name: "data/temp/1", QoS: packet.QoS1},
				{Name: "data/temp/#", QoS: packet.QoS2},
			}

			err = subscribe(sm, session, topics, test.version)
			require.Nil(t, err)

			topicNames := []string{"data/temp/0", "data/temp/#", "data/temp/2"}
			unsub := packet.Unsubscribe{PacketID: test.id,
				Version: test.version, Topics: topicNames}

			session, replies, err := sm.handlePacket(session, &unsub)
			require.Nil(t, err)
			require.Len(t, replies, 1)
			require.NotNil(t, session)

			reply := replies[0]
			require.Equal(t, packet.UNSUBACK, reply.Type())

			unsubAck := reply.(*packet.UnsubAck)
			assert.Equal(t, unsub.PacketID, unsubAck.PacketID)
			assert.Equal(t, unsub.Version, unsubAck.Version)

			if test.version == packet.MQTT50 {
				assert.Equal(t, test.codes, unsubAck.ReasonCodes)
			} else {
				assert.Empty(t, unsubAck.ReasonCodes)
			}
		})
	}
}

func TestSessionManager_Publish(t *testing.T) {
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

	for _, test := range testCases {
		name := fmt.Sprintf("%v-%v-%v", test.qos, test.version.String(),
			test.id)
		t.Run(name, func(t *testing.T) {
			sm := createSessionManager(conf)

			session, err := connectClient(sm, test.version, false, nil)
			require.Nil(t, err)

			idGen := sm.pubSub.idGen.(*idGeneratorMock)
			idGen.On("NextID").Return(3)

			pubPkt := packet.NewPublish(test.id, test.version, test.topic,
				test.qos, 0, 0, []byte(test.payload), nil)

			session, replies, err := sm.handlePacket(session, &pubPkt)
			require.Nil(t, err)
			require.NotNil(t, session)

			if test.qos > packet.QoS0 {
				require.Len(t, replies, 1)
				reply := replies[0]
				require.Equal(t, packet.PUBACK, reply.Type())

				pubAck := reply.(*packet.PubAck)
				assert.Equal(t, test.id, pubAck.PacketID)
				assert.Equal(t, test.version, pubAck.Version)
				assert.Equal(t, packet.ReasonCodeV5Success, pubAck.ReasonCode)
			} else {
				require.Empty(t, replies)
			}
		})
	}
}

func TestSessionManager_PubAckQoS1(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)

	pkt := packet.NewPublish(1, packet.MQTT311, "data", packet.QoS1,
		0, 0, nil, nil)
	msg := message{id: 1, packet: &pkt}

	session, err := connectClient(sm, packet.MQTT311, false, nil)
	require.Nil(t, err)

	inflightMsg := &inflightMessage{messageID: msg.id, packetID: 1,
		packet: &pkt}
	inflightMsg.lastSent = time.Now().UnixMicro()
	inflightMsg.tries = 1
	session.inflightMessages.add(inflightMsg)

	pubAck := packet.NewPubAck(pkt.PacketID, pkt.Version,
		packet.ReasonCodeV5Success, nil)

	session, replies, err := sm.handlePacket(session, &pubAck)
	require.Nil(t, err)
	assert.Empty(t, replies)
	require.NotNil(t, session)
	assert.Zero(t, session.inflightMessages.len())

	s := sm.store.sessions[string(session.ClientID)]
	assert.Zero(t, s.inflightMessages.len())
}

func TestSessionManager_PubAckUnknownMessage(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)

	session, err := connectClient(sm, packet.MQTT311, false, nil)
	require.Nil(t, err)

	pubAck := packet.NewPubAck(10, packet.MQTT311,
		packet.ReasonCodeV5Success, nil)

	session, replies, err := sm.handlePacket(session, &pubAck)
	require.NotNil(t, err)
	require.Empty(t, replies)
	require.NotNil(t, session)
}

func TestSessionManager_Disconnect(t *testing.T) {
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

	for _, test := range testCases {
		name := fmt.Sprintf("%v-%v", test.version.String(), test.cleanSession)
		t.Run(name, func(t *testing.T) {
			sm := createSessionManager(conf)

			session, err := connectClient(sm, test.version, test.cleanSession,
				nil)
			require.Nil(t, err)

			sub, err := sm.pubSub.subscribe(session,
				packet.Topic{Name: "test"}, 1)
			require.Nil(t, err)
			session.Subscriptions["test"] = sub

			disconnect := packet.Disconnect{Properties: &packet.Properties{}}
			session, replies, err := sm.handlePacket(session, &disconnect)
			assert.Nil(t, err)
			require.Empty(t, replies)
			require.NotNil(t, session)
			assert.False(t, session.connected)

			if test.cleanSession {
				assert.Empty(t, sm.pubSub.tree.root.children)
			} else {
				assert.NotEmpty(t, sm.pubSub.tree.root.children)
			}
		})
	}
}

func TestSessionManager_DisconnectExpiryInterval(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)

	props := &packet.Properties{}
	props.SessionExpiryInterval = new(uint32)
	*props.SessionExpiryInterval = 600

	session, err := connectClient(sm, packet.MQTT50, true, props)
	require.Nil(t, err)
	require.Equal(t, uint32(600), session.ExpiryInterval)

	props.SessionExpiryInterval = new(uint32)
	*props.SessionExpiryInterval = 300

	disconnect := packet.Disconnect{Properties: props}
	session, replies, err := sm.handlePacket(session, &disconnect)
	assert.Nil(t, err)
	assert.Empty(t, replies)
	require.NotNil(t, session)
	assert.Equal(t, uint32(300), session.ExpiryInterval)
}

func TestSessionManager_DisconnectInvalidExpiryInterval(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)

	session, err := connectClient(sm, packet.MQTT50, false, nil)
	require.Nil(t, err)

	props := &packet.Properties{}
	props.SessionExpiryInterval = new(uint32)
	*props.SessionExpiryInterval = 300

	disconnect := packet.Disconnect{Properties: props}
	session, replies, err := sm.handlePacket(session, &disconnect)
	require.NotNil(t, err)
	require.Len(t, replies, 1)
	require.NotNil(t, session)

	reply := replies[0]
	assert.Equal(t, packet.DISCONNECT, reply.Type())

	discReply := reply.(*packet.Disconnect)
	assert.Equal(t, packet.MQTT50, discReply.Version)
	assert.Equal(t, packet.ReasonCodeV5ProtocolError, discReply.ReasonCode)
}

func TestSessionManager_DisconnectErrorOnSaveSessionIsOkay(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)

	props := &packet.Properties{}
	props.SessionExpiryInterval = new(uint32)
	*props.SessionExpiryInterval = 600

	session, err := connectClient(sm, packet.MQTT50, true, props)
	require.Nil(t, err)
	require.Equal(t, uint32(600), session.ExpiryInterval)

	props.SessionExpiryInterval = new(uint32)
	*props.SessionExpiryInterval = 300

	disconnect := packet.Disconnect{Properties: props}
	session, replies, err := sm.handlePacket(session, &disconnect)
	assert.Nil(t, err)
	assert.Empty(t, replies)
	require.NotNil(t, session)
	assert.Equal(t, uint32(300), session.ExpiryInterval)
}

func TestSessionManager_DisconnectFailedToDeleteSession(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)

	session, err := connectClient(sm, packet.MQTT311, true, nil)
	require.Nil(t, err)

	disconnect := packet.Disconnect{}
	session, replies, err := sm.handlePacket(session, &disconnect)
	assert.Nil(t, err)
	assert.Empty(t, replies)
	require.NotNil(t, session)
}

func TestSessionManager_InvalidPacket(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)

	session, err := connectClient(sm, packet.MQTT311, false, nil)
	require.Nil(t, err)

	pkt := packet.PingResp{}
	_, reply, err := sm.handlePacket(session, &pkt)
	assert.NotNil(t, err)
	assert.Nil(t, reply)
}

func TestSessionManager_PublishMessage(t *testing.T) {
	testCases := []struct {
		id  packet.ID
		qos packet.QoS
	}{
		{id: 10, qos: packet.QoS0},
		{id: 20, qos: packet.QoS1},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprintf("QoS%v", test.qos), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)

			session := &Session{}
			session.ClientID = ClientID("client-1")
			session.Version = packet.MQTT311
			session.connected = true

			pkt := packet.NewPublish(test.id, packet.MQTT311, "data",
				test.qos, 0, 0, nil, nil)
			msg := message{id: messageID(test.id), packet: &pkt}

			pktDeliverer := sm.deliverer.(*delivererMock)
			pktDeliverer.On("deliverPacket", session.SessionID, mock.Anything).
				Return(nil)

			err := sm.publishMessage(session, &msg)
			assert.Nil(t, err)
			pub := pktDeliverer.Calls[0].Arguments.Get(1).(*packet.Publish)

			if test.qos == 0 {
				assert.Zero(t, session.inflightMessages.len())
				assert.Equal(t, &pkt, pub)
			} else {
				assert.Equal(t, 1, session.inflightMessages.len())

				inflightMsg := session.inflightMessages.front()
				require.NotNil(t, inflightMsg)
				assert.Equal(t, messageID(test.id), inflightMsg.messageID)
				assert.Equal(t, packet.ID(session.packetID),
					inflightMsg.packetID)
				assert.Equal(t, msg.packet.QoS, inflightMsg.packet.QoS)
				assert.Equal(t, msg.packet.Version, inflightMsg.packet.Version)
				assert.Equal(t, msg.packet.TopicName,
					inflightMsg.packet.TopicName)
				assert.Equal(t, msg.packet.Payload, inflightMsg.packet.Payload)
				assert.NotEqual(t, inflightMsg.packet.PacketID,
					msg.packet.PacketID)
				assert.NotZero(t, inflightMsg.lastSent)
				assert.Equal(t, 1, inflightMsg.tries)
				assert.Equal(t, inflightMsg.packet, pub)

				s := sm.store.sessions[string(session.ClientID)]
				assert.Equal(t, 1, s.inflightMessages.len())
				m := s.inflightMessages.front()
				assert.Equal(t, inflightMsg, m)
			}
			pktDeliverer.AssertExpectations(t)
		})
	}
}

func TestSessionManager_PublishMessageDifferentVersion(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)

	session := &Session{}
	session.ClientID = ClientID("client-1")
	session.Version = packet.MQTT50
	session.connected = true

	pkt := packet.NewPublish(10, packet.MQTT311, "data",
		packet.QoS0, 0, 0, nil, nil)
	msg := message{id: 50, packet: &pkt}

	pktDeliverer := sm.deliverer.(*delivererMock)
	pktDeliverer.On("deliverPacket", session.SessionID, &pkt).Return(nil)

	err := sm.publishMessage(session, &msg)
	assert.Nil(t, err)

	pub := pktDeliverer.Calls[0].Arguments.Get(1).(*packet.Publish)
	assert.Equal(t, pkt.PacketID, pub.PacketID)
	assert.Equal(t, pkt.QoS, pub.QoS)
	assert.Equal(t, pkt.TopicName, pub.TopicName)
	assert.Equal(t, session.Version, pub.Version)
	pktDeliverer.AssertExpectations(t)
}

func TestSessionManager_PublishMessageDisconnectedSession(t *testing.T) {
	testCases := []struct {
		id  packet.ID
		qos packet.QoS
	}{
		{id: 10, qos: packet.QoS0},
		{id: 20, qos: packet.QoS1},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprintf("QoS%v", test.qos), func(t *testing.T) {
			conf := newConfiguration()
			sm := createSessionManager(conf)

			session := &Session{}
			session.ClientID = ClientID("client-1")
			session.connected = false

			pkt := packet.NewPublish(test.id, packet.MQTT311, "data",
				test.qos, 0, 0, nil, nil)
			msg := message{id: messageID(test.id), packet: &pkt}

			err := sm.publishMessage(session, &msg)
			assert.Nil(t, err)

			pktDeliverer := sm.deliverer.(*delivererMock)
			assert.Len(t, pktDeliverer.Calls, 0)
			pktDeliverer.AssertExpectations(t)
		})
	}
}

func TestSessionManager_PublishMessageError(t *testing.T) {
	conf := newConfiguration()
	sm := createSessionManager(conf)

	session := &Session{}
	session.ClientID = ClientID("client-1")
	session.Version = packet.MQTT311
	session.connected = true

	pkt := packet.NewPublish(10, packet.MQTT311, "data",
		packet.QoS0, 0, 0, nil, nil)
	msg := message{id: 50, packet: &pkt}

	pktDeliverer := sm.deliverer.(*delivererMock)
	pktDeliverer.On("deliverPacket", session.SessionID, &pkt).
		Return(errors.New("failed to deliver message"))

	err := sm.publishMessage(session, &msg)
	assert.NotNil(t, err)
	pktDeliverer.AssertExpectations(t)
}
