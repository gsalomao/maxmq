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
	"github.com/gsalomao/maxmq/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type sessionStoreMock struct {
	mock.Mock
}

func (s *sessionStoreMock) GetSession(id ClientID) (*Session, error) {
	args := s.Called(id)
	ss := args.Get(0)
	if ss == nil {
		return &Session{}, args.Error(1)
	}
	return ss.(*Session), args.Error(1)
}

func (s *sessionStoreMock) SaveSession(session *Session) error {
	args := s.Called(session)
	return args.Error(0)
}

func (s *sessionStoreMock) DeleteSession(session *Session) error {
	args := s.Called(session)
	return args.Error(0)
}

func createSessionManager(conf Configuration) sessionManager {
	logger := mocks.NewLoggerStub()
	store := &sessionStoreMock{}
	store.On("GetSession", mock.Anything).Return(nil, ErrSessionNotFound)
	store.On("SaveSession", mock.Anything).Return(nil)

	userProps := make([]packet.UserProperty, 0, len(conf.UserProperties))
	for k, v := range conf.UserProperties {
		userProps = append(userProps,
			packet.UserProperty{Key: []byte(k), Value: []byte(v)})
	}

	m := newMetrics(true, logger.Logger())
	return newSessionManager(&conf, m, userProps, store, logger.Logger())
}

func checkConnect(t *testing.T, conf Configuration, pkt *packet.Connect,
	success bool) *packet.ConnAck {

	s := newSession(conf.ConnectTimeout)
	sm := createSessionManager(conf)

	reply, err := sm.handlePacket(&s, pkt)
	if success {
		assert.Nil(t, err)
	} else {
		assert.NotNil(t, err)
	}

	assert.NotNil(t, reply)
	assert.Equal(t, packet.CONNACK, reply.Type())

	connAck := reply.(*packet.ConnAck)
	assert.Equal(t, pkt.Version, connAck.Version)
	return connAck
}

func connectClient(m *sessionManager, s *Session, v packet.MQTTVersion) error {
	pkt := packet.Connect{ClientID: ClientID{'a'}, Version: v}
	_, err := m.handlePacket(s, &pkt)
	return err
}

func TestSessionManager_HandleConnectNewSession(t *testing.T) {
	testCases := []struct {
		version packet.MQTTVersion
		code    packet.ReasonCode
	}{
		{version: packet.MQTT31, code: packet.ReasonCodeV3ConnectionAccepted},
		{version: packet.MQTT311, code: packet.ReasonCodeV3ConnectionAccepted},
		{version: packet.MQTT50, code: packet.ReasonCodeV5Success},
	}

	for _, test := range testCases {
		t.Run(test.version.String(), func(t *testing.T) {
			conf := newConfiguration()
			pkt := packet.Connect{ClientID: ClientID{'a'},
				Version: test.version, Properties: &packet.Properties{}}

			connAck := checkConnect(t, conf, &pkt, true)
			assert.Equal(t, test.code, connAck.ReasonCode)
			assert.False(t, connAck.SessionPresent)
		})
	}
}

func TestSessionManager_HandleConnectExistingSession(t *testing.T) {
	testCases := []struct {
		version packet.MQTTVersion
		code    packet.ReasonCode
	}{
		{version: packet.MQTT31, code: packet.ReasonCodeV3ConnectionAccepted},
		{version: packet.MQTT311, code: packet.ReasonCodeV3ConnectionAccepted},
		{version: packet.MQTT50, code: packet.ReasonCodeV5Success},
	}

	for _, test := range testCases {
		t.Run(test.version.String(), func(t *testing.T) {
			conf := newConfiguration()
			clientID := ClientID{'a'}
			pkt := packet.Connect{ClientID: clientID, Version: test.version}

			s := newSession(conf.ConnectTimeout)
			sm := createSessionManager(conf)

			store := &sessionStoreMock{}
			session := Session{
				ClientID:       clientID,
				ConnectedAt:    time.Now().Add(-1 * time.Minute).Unix(),
				ExpiryInterval: conf.MaxSessionExpiryInterval,
				Version:        test.version,
			}

			store.On("GetSession", clientID).Return(&session, nil)
			store.On("SaveSession", mock.Anything).Return(nil)
			sm.store = store

			reply, err := sm.handlePacket(&s, &pkt)
			assert.Nil(t, err)
			assert.NotNil(t, reply)
			assert.Equal(t, packet.CONNACK, reply.Type())

			connAck := reply.(*packet.ConnAck)
			assert.Equal(t, test.version, connAck.Version)
			assert.Equal(t, test.code, connAck.ReasonCode)
			assert.True(t, connAck.SessionPresent)
		})
	}
}

func TestSessionManager_HandleConnectFailedToGetSession(t *testing.T) {
	testCases := []struct {
		version packet.MQTTVersion
	}{
		{version: packet.MQTT31},
		{version: packet.MQTT311},
		{version: packet.MQTT50},
	}

	for _, test := range testCases {
		t.Run(test.version.String(), func(t *testing.T) {
			conf := newConfiguration()
			clientID := ClientID{'a'}
			s := newSession(conf.ConnectTimeout)
			sm := createSessionManager(conf)

			store := &sessionStoreMock{}
			store.On("GetSession", mock.Anything).Return(nil,
				errors.New("failed to get session"))
			sm.store = store

			pkt := packet.Connect{ClientID: clientID, Version: test.version}

			reply, err := sm.handlePacket(&s, &pkt)
			assert.NotNil(t, err)
			assert.Nil(t, reply)
		})
	}
}

func TestSessionManager_HandleConnectFailedToSaveSession(t *testing.T) {
	testCases := []struct {
		version packet.MQTTVersion
	}{
		{version: packet.MQTT31},
		{version: packet.MQTT311},
		{version: packet.MQTT50},
	}

	for _, test := range testCases {
		t.Run(test.version.String(), func(t *testing.T) {
			conf := newConfiguration()
			clientID := ClientID{'a'}
			s := newSession(conf.ConnectTimeout)
			sm := createSessionManager(conf)

			store := &sessionStoreMock{}
			store.On("GetSession", mock.Anything).Return(nil,
				ErrSessionNotFound)
			store.On("SaveSession",
				mock.Anything).Return(errors.New("failed to save session"))
			sm.store = store

			pkt := packet.Connect{ClientID: clientID, Version: test.version}

			reply, err := sm.handlePacket(&s, &pkt)
			assert.NotNil(t, err)
			assert.Nil(t, reply)
		})
	}
}

func TestSessionManager_HandleConnectClientIDTooBig(t *testing.T) {
	testCases := []struct {
		id       []byte
		maxIDLen int
		version  packet.MQTTVersion
		code     packet.ReasonCode
	}{
		{id: []byte("012345678901234567890123"), version: packet.MQTT31,
			maxIDLen: 65535, code: packet.ReasonCodeV3IdentifierRejected,
		},
		{id: []byte("0123456789012345678901234567890"), version: packet.MQTT311,
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
			connAck := checkConnect(t, conf, &pkt, false)
			assert.Equal(t, test.code, connAck.ReasonCode)
		})
	}
}

func TestSessionManager_HandleConnectAllowEmptyClientID(t *testing.T) {
	testCases := []struct {
		version packet.MQTTVersion
		code    packet.ReasonCode
	}{
		{version: packet.MQTT31, code: packet.ReasonCodeV3ConnectionAccepted},
		{version: packet.MQTT311, code: packet.ReasonCodeV3ConnectionAccepted},
		{version: packet.MQTT50, code: packet.ReasonCodeV5Success},
	}

	for _, test := range testCases {
		t.Run(test.version.String(), func(t *testing.T) {
			conf := newConfiguration()
			conf.AllowEmptyClientID = true

			pkt := packet.Connect{Version: test.version}
			connAck := checkConnect(t, conf, &pkt, true)
			assert.Equal(t, test.code, connAck.ReasonCode)
		})
	}
}

func TestSessionManager_HandleConnectDenyEmptyClientID(t *testing.T) {
	testCases := []struct {
		version packet.MQTTVersion
		code    packet.ReasonCode
	}{
		{version: packet.MQTT31, code: packet.ReasonCodeV3IdentifierRejected},
		{version: packet.MQTT311, code: packet.ReasonCodeV3IdentifierRejected},
		{version: packet.MQTT50, code: packet.ReasonCodeV5InvalidClientID},
	}

	for _, test := range testCases {
		t.Run(test.version.String(), func(t *testing.T) {
			conf := newConfiguration()
			conf.AllowEmptyClientID = false

			pkt := packet.Connect{Version: test.version}
			connAck := checkConnect(t, conf, &pkt, false)
			assert.Equal(t, test.code, connAck.ReasonCode)
		})
	}
}

func TestSessionManager_HandleConnectAssignClientID(t *testing.T) {
	conf := newConfiguration()
	conf.AllowEmptyClientID = true

	pkt := packet.Connect{Version: packet.MQTT50}
	connAck := checkConnect(t, conf, &pkt, true)
	assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
	assert.NotNil(t, connAck.Properties)
	assert.NotNil(t, connAck.Properties.AssignedClientID)
	assert.Equal(t, 20, len(connAck.Properties.AssignedClientID))
}

func TestSessionManager_HandleConnectAssignClientIDWithPrefix(t *testing.T) {
	conf := newConfiguration()
	conf.AllowEmptyClientID = true
	conf.ClientIDPrefix = []byte("MAX-")

	pkt := packet.Connect{Version: packet.MQTT50}
	connAck := checkConnect(t, conf, &pkt, true)
	assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
	assert.NotNil(t, connAck.Properties)
	assert.NotNil(t, connAck.Properties.AssignedClientID)
	assert.Equal(t, 24, len(connAck.Properties.AssignedClientID))
	assert.Equal(t, conf.ClientIDPrefix,
		connAck.Properties.AssignedClientID[:4])
}

func TestSessionManager_HandleConnectMaxSessionExpiryInterval(t *testing.T) {
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
			connAck := checkConnect(t, conf, &pkt, true)
			assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
			assert.NotNil(t, connAck.Properties)
			props := connAck.Properties

			if test.resp > 0 {
				assert.NotNil(t, props.SessionExpiryInterval)
				assert.Equal(t, test.resp, *props.SessionExpiryInterval)
			} else {
				assert.Nil(t, props.SessionExpiryInterval)
			}
		})
	}
}

func TestSessionManager_HandleConnectMaxKeepAlive(t *testing.T) {
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

			connAck := checkConnect(t, conf, &pkt,
				test.code == packet.ReasonCodeV5Success)
			assert.Equal(t, test.code, connAck.ReasonCode)

			if test.version == packet.MQTT50 {
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

func TestSessionManager_HandleConnectMaxInflightMessages(t *testing.T) {
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

			connAck := checkConnect(t, conf, &pkt, true)
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

func TestSessionManager_HandleConnectMaxPacketSize(t *testing.T) {
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

			connAck := checkConnect(t, conf, &pkt, true)
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

func TestSessionManager_HandleConnectMaximumQoS(t *testing.T) {
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

			connAck := checkConnect(t, conf, &pkt, true)
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

func TestSessionManager_HandleConnectTopicAliasMaximum(t *testing.T) {
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

			connAck := checkConnect(t, conf, &pkt, true)
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

func TestSessionManager_HandleConnectRetainAvailable(t *testing.T) {
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

			connAck := checkConnect(t, conf, &pkt, true)
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

func TestSessionManager_HandleConnectWildcardSubsAvailable(t *testing.T) {
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

			connAck := checkConnect(t, conf, &pkt, true)
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

func TestSessionManager_HandleConnectSubscriptionIDAvailable(t *testing.T) {
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

			connAck := checkConnect(t, conf, &pkt, true)
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

func TestSessionManager_HandleConnectSharedSubscriptionAvailable(t *testing.T) {
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

			connAck := checkConnect(t, conf, &pkt, true)
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

func TestSessionManager_HandleConnectUserProperty(t *testing.T) {
	conf := newConfiguration()
	conf.UserProperties = map[string]string{"k1": "v1"}

	pkt := packet.Connect{ClientID: ClientID{'a'}, Version: packet.MQTT50}
	connAck := checkConnect(t, conf, &pkt, true)
	assert.Equal(t, packet.ReasonCodeV5Success, connAck.ReasonCode)
	assert.NotNil(t, connAck.Properties)
	assert.Equal(t, 1, len(connAck.Properties.UserProperties))
	assert.Equal(t, []byte("k1"), connAck.Properties.UserProperties[0].Key)
	assert.Equal(t, []byte("v1"), connAck.Properties.UserProperties[0].Value)
}

func TestSessionManager_HandlePingReq(t *testing.T) {
	conf := newConfiguration()
	s := newSession(conf.ConnectTimeout)
	sm := createSessionManager(conf)

	err := connectClient(&sm, &s, packet.MQTT311)
	require.Nil(t, err)

	pingReq := packet.PingReq{}
	reply, err := sm.handlePacket(&s, &pingReq)
	assert.Nil(t, err)
	assert.NotNil(t, reply)
	assert.Equal(t, packet.PINGRESP, reply.Type())
}

func TestSessionManager_HandlePingReqWithoutConnect(t *testing.T) {
	conf := newConfiguration()
	s := newSession(conf.ConnectTimeout)
	sm := createSessionManager(conf)

	pkt := packet.PingReq{}
	reply, err := sm.handlePacket(&s, &pkt)
	assert.NotNil(t, err)
	assert.Nil(t, reply)
}

func TestSessionManager_HandleDisconnect(t *testing.T) {
	conf := newConfiguration()
	s := newSession(conf.ConnectTimeout)
	sm := createSessionManager(conf)

	err := connectClient(&sm, &s, packet.MQTT50)
	require.Nil(t, err)

	store := sm.store.(*sessionStoreMock)
	store.On("DeleteSession", mock.Anything).Return(nil)

	disconnect := packet.Disconnect{}
	reply, err := sm.handlePacket(&s, &disconnect)
	assert.Nil(t, err)
	assert.Nil(t, reply)
}

func TestSessionManager_HandleDisconnectFailedToDeleteSession(t *testing.T) {
	conf := newConfiguration()
	s := newSession(conf.ConnectTimeout)
	sm := createSessionManager(conf)

	err := connectClient(&sm, &s, packet.MQTT311)
	require.Nil(t, err)

	store := sm.store.(*sessionStoreMock)
	store.On("DeleteSession",
		mock.Anything).Return(errors.New("failed to delete session"))

	disconnect := packet.Disconnect{}
	reply, err := sm.handlePacket(&s, &disconnect)
	assert.Nil(t, err)
	assert.Nil(t, reply)
}

func TestSessionManager_HandleHandleInvalidPacket(t *testing.T) {
	conf := newConfiguration()
	s := newSession(conf.ConnectTimeout)
	sm := createSessionManager(conf)

	err := connectClient(&sm, &s, packet.MQTT311)
	require.Nil(t, err)

	pkt := packet.PingResp{}
	reply, err := sm.handlePacket(&s, &pkt)
	assert.NotNil(t, err)
	assert.Nil(t, reply)
}
