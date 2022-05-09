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

package mqtt_test

import (
	"fmt"
	"math"
	"net"
	"testing"
	"time"

	"github.com/gsalomao/maxmq/mocks"
	"github.com/gsalomao/maxmq/mqtt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type sessionStoreMock struct {
	mock.Mock
}

func (s *sessionStoreMock) GetSession(id mqtt.ClientID) (mqtt.Session, error) {
	args := s.Called(id)
	ss := args.Get(0)
	if ss == nil {
		return mqtt.Session{}, args.Error(1)
	}
	return ss.(mqtt.Session), args.Error(1)
}

func (s *sessionStoreMock) SaveSession(session mqtt.Session) error {
	args := s.Called(session)
	return args.Error(0)
}

func (s *sessionStoreMock) DeleteSession(session mqtt.Session) error {
	args := s.Called(session)
	return args.Error(0)
}

func newConfiguration() mqtt.Configuration {
	return mqtt.Configuration{
		TCPAddress:                    ":1883",
		ConnectTimeout:                5,
		BufferSize:                    1024,
		MaxPacketSize:                 268435456,
		MaxKeepAlive:                  0,
		MaxSessionExpiryInterval:      0,
		MaxInflightMessages:           0,
		MaximumQoS:                    2,
		MaxTopicAlias:                 0,
		RetainAvailable:               true,
		WildcardSubscriptionAvailable: true,
		SubscriptionIDAvailable:       true,
		SharedSubscriptionAvailable:   true,
		MaxClientIDLen:                65535,
		AllowEmptyClientID:            true,
		UserProperties:                map[string]string{},
	}
}

func TestConnectionManager_ConnectV3(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	conf := newConfiguration()
	conf.MaxKeepAlive = 600
	conf.MetricsEnabled = true

	store := &sessionStoreMock{}
	store.On("GetSession", mock.Anything).Return(mqtt.Session{},
		mqtt.ErrSessionNotFound)
	store.On("SaveSession", mock.Anything).Return(nil)

	cm := mqtt.NewConnectionManager(conf, store, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	msg := []byte{
		0x10, 13, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 10, // variable header
		0, 1, 'a', // client ID
	}

	_, err := conn.Write(msg)
	require.Nil(t, err)

	out := make([]byte, 4)
	_, err = conn.Read(out)
	require.Nil(t, err)

	connAck := []byte{0x20, 2, 0, 0}
	assert.Equal(t, connAck, out)

	_ = conn.Close()
	<-done
}

func TestConnectionManager_ConnectV3NewSession(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	conf := newConfiguration()
	conf.MaxSessionExpiryInterval = 7200

	clientId := mqtt.ClientID{'a'}
	store := &sessionStoreMock{}
	store.On("GetSession", clientId).Return(mqtt.Session{},
		mqtt.ErrSessionNotFound)
	store.On("SaveSession",
		mock.MatchedBy(func(s mqtt.Session) bool {
			connectedAt := time.Unix(s.ConnectedAt, 0)
			assert.True(t,
				time.Now().Add(-1*time.Second).Before(connectedAt))
			assert.Equal(t, uint32(math.MaxUint32), s.ExpiryInterval)
			return true
		}),
	).Return(nil)

	cm := mqtt.NewConnectionManager(conf, store, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	msg := []byte{
		0x10, 13, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 10, // variable header
		0, byte(len(clientId)), // client ID
	}
	msg = append(msg, clientId...)

	_, err := conn.Write(msg)
	require.Nil(t, err)

	_ = conn.Close()
	<-done
}

func TestConnectionManager_ConnectV3ExistingSession(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	conf := newConfiguration()
	conf.MaxSessionExpiryInterval = 900

	clientId := mqtt.ClientID{'a'}
	session := mqtt.Session{
		ConnectedAt:    time.Now().Add(-1 * time.Minute).Unix(),
		ExpiryInterval: conf.MaxSessionExpiryInterval,
	}
	store := &sessionStoreMock{}
	store.On("GetSession", clientId).Return(session, nil)
	store.On("SaveSession",
		mock.MatchedBy(func(s mqtt.Session) bool {
			connectedAt := time.Unix(s.ConnectedAt, 0)
			assert.True(t,
				time.Now().Add(-1*time.Second).Before(connectedAt))
			assert.Equal(t, conf.MaxSessionExpiryInterval, s.ExpiryInterval)
			return true
		}),
	).Return(nil)

	cm := mqtt.NewConnectionManager(conf, store, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	msg := []byte{
		0x10, 13, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 10, // variable header
		0, byte(len(clientId)), // client ID
	}
	msg = append(msg, clientId...)

	_, err := conn.Write(msg)
	require.Nil(t, err)

	_ = conn.Close()
	<-done
}

func TestConnectionManager_ConnectV31ClientIDTooBig(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	store := &sessionStoreMock{}

	cm := mqtt.NewConnectionManager(mqtt.Configuration{
		MaxClientIDLen: 65535,
	}, store, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	clientId := []byte("012345678901234567890123")
	msg := []byte{
		0x10, byte(len(clientId)) + 14, // fixed header
		0, 6, 'M', 'Q', 'I', 's', 'd', 'p', 3, 0, 0, 0, // variable header
		0, byte(len(clientId)),
	}
	msg = append(msg, clientId...)

	_, err := conn.Write(msg)
	require.Nil(t, err)

	out := make([]byte, 4)
	_, err = conn.Read(out)
	require.Nil(t, err)

	connAck := []byte{0x20, 2, 0, 2} // identifier rejected
	assert.Equal(t, connAck, out)

	_ = conn.Close()
	<-done
}

func TestConnectionManager_ConnectV311ClientIDTooBig(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	store := &sessionStoreMock{}

	cm := mqtt.NewConnectionManager(mqtt.Configuration{
		MaxClientIDLen: 30,
	}, store, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	clientId := []byte("0123456789012345678901234567890")
	msg := []byte{
		0x10, byte(len(clientId)) + 12, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 0, // variable header
		0, byte(len(clientId)),
	}
	msg = append(msg, clientId...)

	_, err := conn.Write(msg)
	require.Nil(t, err)

	out := make([]byte, 4)
	_, err = conn.Read(out)
	require.Nil(t, err)

	connAck := []byte{0x20, 2, 0, 2} // identifier rejected
	assert.Equal(t, connAck, out)

	_ = conn.Close()
	<-done
}

func TestConnectionManager_ConnectV311AllowEmptyClientID(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	store := &sessionStoreMock{}
	store.On("GetSession", mock.Anything).Return(mqtt.Session{},
		mqtt.ErrSessionNotFound)
	store.On("SaveSession", mock.Anything).Return(nil)

	cm := mqtt.NewConnectionManager(mqtt.Configuration{
		AllowEmptyClientID: true,
	}, store, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	msg := []byte{
		0x10, 12, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 4, 2, 0, 0, // variable header
		0, 0, // client ID
	}

	_, err := conn.Write(msg)
	require.Nil(t, err)

	out := make([]byte, 4)
	_, err = conn.Read(out)
	require.Nil(t, err)

	connAck := []byte{0x20, 2, 0, 0}
	assert.Equal(t, connAck, out)

	_ = conn.Close()
	<-done
}

func TestConnectionManager_ConnectV311DenyEmptyClientID(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	store := &sessionStoreMock{}

	cm := mqtt.NewConnectionManager(mqtt.Configuration{
		AllowEmptyClientID: false,
	}, store, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	msg := []byte{
		0x10, 12, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 4, 2, 0, 0, // variable header
		0, 0, // client ID
	}

	_, err := conn.Write(msg)
	require.Nil(t, err)

	out := make([]byte, 4)
	_, err = conn.Read(out)
	require.Nil(t, err)

	connAck := []byte{0x20, 2, 0, 2} // identifier rejected
	assert.Equal(t, connAck, out)

	_ = conn.Close()
	<-done
}

func TestConnectionManager_ConnectV5(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	store := &sessionStoreMock{}
	store.On("GetSession", mock.Anything).Return(mqtt.Session{},
		mqtt.ErrSessionNotFound)
	store.On("SaveSession", mock.Anything).Return(nil)

	cm := mqtt.NewConnectionManager(mqtt.Configuration{
		MaximumQoS:                    3,     // invalid: will be changed to 2
		MaxTopicAlias:                 65536, // invalid: will be changed to 0
		MaxInflightMessages:           65536, // invalid: will be changed to 0
		MaxSessionExpiryInterval:      900,
		RetainAvailable:               true,
		WildcardSubscriptionAvailable: true,
		SubscriptionIDAvailable:       true,
		SharedSubscriptionAvailable:   true,
	}, store, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	msg := []byte{
		0x10, 19, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 60, // variable header
		5,                // property length
		17, 0, 0, 0, 200, // SessionExpiryInterval
		0, 1, 'a', // client ID
	}

	_, err := conn.Write(msg)
	require.Nil(t, err)

	out := make([]byte, 5)
	_, err = conn.Read(out)
	require.Nil(t, err)

	connAck := []byte{0x20, 3, 0, 0, 0}
	assert.Equal(t, connAck, out)

	_ = conn.Close()
	<-done
}

func TestConnectionManager_ConnectV5WithSessionExpiryInterval(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	conf := newConfiguration()
	conf.MaxSessionExpiryInterval = 100

	store := &sessionStoreMock{}
	store.On("GetSession", mock.Anything).Return(mqtt.Session{},
		mqtt.ErrSessionNotFound)
	store.On("SaveSession",
		mock.MatchedBy(func(s mqtt.Session) bool {
			assert.Equal(t, uint32(100), s.ExpiryInterval)
			return true
		}),
	).Return(nil)

	cm := mqtt.NewConnectionManager(conf, store, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	msg := []byte{
		0x10, 19, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 60, // variable header
		5,                // property length
		17, 0, 0, 0, 200, // SessionExpiryInterval
		0, 1, 'a', // client ID
	}

	_, err := conn.Write(msg)
	require.Nil(t, err)

	_ = conn.Close()
	<-done
}

func TestConnectionManager_ConnectV5WithoutSessionExpiryInterval(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	conf := newConfiguration()
	conf.MaxSessionExpiryInterval = 100

	store := &sessionStoreMock{}
	store.On("GetSession", mock.Anything).Return(mqtt.Session{},
		mqtt.ErrSessionNotFound)
	store.On("SaveSession",
		mock.MatchedBy(func(s mqtt.Session) bool {
			assert.Equal(t, uint32(0), s.ExpiryInterval)
			return true
		}),
	).Return(nil)

	cm := mqtt.NewConnectionManager(conf, store, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	msg := []byte{
		0x10, 14, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 60, // variable header
		0,         // property length
		0, 1, 'a', // client ID
	}

	_, err := conn.Write(msg)
	require.Nil(t, err)

	_ = conn.Close()
	<-done
}

func TestConnectionManager_ConnectV5ClientIDTooBig(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	store := &sessionStoreMock{}
	conf := newConfiguration()
	conf.MaxClientIDLen = 30

	cm := mqtt.NewConnectionManager(conf, store, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	clientId := []byte("0123456789012345678901234567890")
	msg := []byte{
		0x10, byte(len(clientId)) + 13, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 0, // variable header
		0, // property length
		0, byte(len(clientId)),
	}
	msg = append(msg, clientId...)

	_, err := conn.Write(msg)
	require.Nil(t, err)

	out := make([]byte, 5)
	_, err = conn.Read(out)
	require.Nil(t, err)

	connAck := []byte{0x20, 3, 0, 0x85, 0} // client ID not valid
	assert.Equal(t, connAck, out)

	_ = conn.Close()
	<-done
}

func TestConnectionManager_ConnectV5DenyEmptyClientID(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	store := &sessionStoreMock{}
	conf := newConfiguration()
	conf.AllowEmptyClientID = false

	cm := mqtt.NewConnectionManager(conf, store, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	msg := []byte{
		0x10, 13, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 5, 2, 0, 0, // variable header
		0,    // property length
		0, 0, // client ID
	}

	_, err := conn.Write(msg)
	require.Nil(t, err)

	out := make([]byte, 5)
	_, err = conn.Read(out)
	require.Nil(t, err)

	connAck := []byte{0x20, 3, 0, 0x85, 0} // client ID not valid
	assert.Equal(t, connAck, out)

	_ = conn.Close()
	<-done
}

func TestConnectionManager_ConnectV5AssignClientID(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	conf := newConfiguration()
	conf.AllowEmptyClientID = true

	store := &sessionStoreMock{}
	store.On("GetSession", mock.Anything).Return(mqtt.Session{},
		mqtt.ErrSessionNotFound)
	store.On("SaveSession", mock.Anything).Return(nil)

	cm := mqtt.NewConnectionManager(conf, store, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	msg := []byte{
		0x10, 13, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 5, 2, 0, 0, // variable header
		0,    // property length
		0, 0, // client ID
	}

	_, err := conn.Write(msg)
	require.Nil(t, err)

	out := make([]byte, 8)
	_, err = conn.Read(out)
	require.Nil(t, err)

	connAck := []byte{0x20, 26, 0, 0, 23, 18, 0, 20}
	assert.Equal(t, connAck, out)

	_ = conn.Close()
	<-done
}

func TestConnectionManager_ConnectV5AssignClientIDWithPrefix(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	conf := newConfiguration()
	conf.AllowEmptyClientID = true
	conf.ClientIDPrefix = []byte("AUTO-")

	store := &sessionStoreMock{}
	store.On("GetSession", mock.Anything).Return(mqtt.Session{},
		mqtt.ErrSessionNotFound)
	store.On("SaveSession", mock.Anything).Return(nil)

	cm := mqtt.NewConnectionManager(conf, store, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	msg := []byte{
		0x10, 13, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 5, 2, 0, 0, // variable header
		0,    // property length
		0, 0, // client ID
	}

	_, err := conn.Write(msg)
	require.Nil(t, err)

	out := make([]byte, 13)
	_, err = conn.Read(out)
	require.Nil(t, err)

	connAck := []byte{0x20, 31, 0, 0, 28, 18, 0, 25, 'A', 'U', 'T', 'O', '-'}
	assert.Equal(t, connAck, out)

	_ = conn.Close()
	<-done
}

func TestConnectionManager_ConnectV3KeepAliveExceeded(t *testing.T) {
	testCases := []struct {
		keepAlive    uint16
		maxKeepAlive int
	}{
		{keepAlive: 0, maxKeepAlive: 1},
		{keepAlive: 2, maxKeepAlive: 1},
		{keepAlive: 501, maxKeepAlive: 500},
		{keepAlive: 65535, maxKeepAlive: 65534},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprintf("%v-%v", test.keepAlive, test.maxKeepAlive),
			func(t *testing.T) {
				logStub := mocks.NewLoggerStub()
				store := &sessionStoreMock{}
				conf := newConfiguration()
				conf.MaxKeepAlive = test.maxKeepAlive

				cm := mqtt.NewConnectionManager(conf, store, logStub.Logger())

				conn, sConn := net.Pipe()

				done := make(chan bool)
				go func() {
					c := cm.NewConnection(sConn)
					cm.Handle(c)
					done <- true
				}()

				keepAliveMSB := byte(test.keepAlive >> 8)
				keepAliveLSB := byte(test.keepAlive & 0xFF)
				msg := []byte{
					0x10, 13, // fixed header
					0, 4, 'M', 'Q', 'T', 'T', 4, 0, keepAliveMSB, keepAliveLSB,
					0, 1, 'a', // client ID
				}

				_, err := conn.Write(msg)
				require.Nil(t, err)

				out := make([]byte, 4)
				_, err = conn.Read(out)
				require.Nil(t, err)

				connAck := []byte{0x20, 2, 0, 2} // identifier rejected
				assert.Equal(t, connAck, out)

				_ = conn.Close()
				<-done
			})
	}
}

func TestConnectionManager_ConnectV5MaxKeepAlive(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	conf := newConfiguration()
	conf.MaxKeepAlive = 1

	store := &sessionStoreMock{}
	store.On("GetSession", mock.Anything).Return(mqtt.Session{},
		mqtt.ErrSessionNotFound)
	store.On("SaveSession", mock.Anything).Return(nil)

	cm := mqtt.NewConnectionManager(conf, store, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	msg := []byte{
		0x10, 14, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 10, // variable header
		0,         // property length
		0, 1, 'a', // client ID
	}

	_, err := conn.Write(msg)
	require.Nil(t, err)

	out := make([]byte, 8)
	_, err = conn.Read(out)
	require.Nil(t, err)

	connAck := []byte{0x20, 6, 0, 0, 3, 19, 0, 1} // accepted - ServerKeepAlive
	assert.Equal(t, connAck, out)

	_ = conn.Close()
	<-done
}

func TestConnectionManager_ConnectV5MaxSessionExpiryInterval(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	conf := newConfiguration()
	conf.MaxSessionExpiryInterval = 10

	store := &sessionStoreMock{}
	store.On("GetSession", mock.Anything).Return(mqtt.Session{},
		mqtt.ErrSessionNotFound)
	store.On("SaveSession", mock.Anything).Return(nil)

	cm := mqtt.NewConnectionManager(conf, store, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	msg := []byte{
		0x10, 19, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 10, // variable header
		5,               // property length
		17, 0, 0, 0, 11, // SessionExpiryInterval
		0, 1, 'a', // client ID
	}

	_, err := conn.Write(msg)
	require.Nil(t, err)

	out := make([]byte, 10)
	_, err = conn.Read(out)
	require.Nil(t, err)

	connAck := []byte{
		0x20, 8, 0, 0, // fixed header
		5,               // property length
		17, 0, 0, 0, 10, // SessionExpiryInterval
	}
	assert.Equal(t, connAck, out)

	_ = conn.Close()
	<-done
}

func TestConnectionManager_ConnectV5ReceiveMaximum(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	conf := newConfiguration()
	conf.MaxInflightMessages = 20

	store := &sessionStoreMock{}
	store.On("GetSession", mock.Anything).Return(mqtt.Session{},
		mqtt.ErrSessionNotFound)
	store.On("SaveSession", mock.Anything).Return(nil)

	cm := mqtt.NewConnectionManager(conf, store, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	msg := []byte{
		0x10, 14, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 10, // variable header
		0,         // property length
		0, 1, 'a', // client ID
	}

	_, err := conn.Write(msg)
	require.Nil(t, err)

	out := make([]byte, 8)
	_, err = conn.Read(out)
	require.Nil(t, err)

	connAck := []byte{
		0x20, 6, 0, 0, // fixed header
		3,         // property length
		33, 0, 20, // ReceiveMaximum
	}
	assert.Equal(t, connAck, out)

	_ = conn.Close()
	<-done
}

func TestConnectionManager_ConnectV5ReceiveMaximumZero(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	conf := newConfiguration()
	conf.MaxInflightMessages = 0

	store := &sessionStoreMock{}
	store.On("GetSession", mock.Anything).Return(mqtt.Session{},
		mqtt.ErrSessionNotFound)
	store.On("SaveSession", mock.Anything).Return(nil)

	cm := mqtt.NewConnectionManager(conf, store, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	msg := []byte{
		0x10, 14, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 10, // variable header
		0,         // property length
		0, 1, 'a', // client ID
	}

	_, err := conn.Write(msg)
	require.Nil(t, err)

	out := make([]byte, 5)
	_, err = conn.Read(out)
	require.Nil(t, err)

	connAck := []byte{0x20, 3, 0, 0, 0}
	assert.Equal(t, connAck, out)

	_ = conn.Close()
	<-done
}

func TestConnectionManager_ConnectV5ReceiveMaximumMax(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	conf := newConfiguration()
	conf.MaxInflightMessages = 65535

	store := &sessionStoreMock{}
	store.On("GetSession", mock.Anything).Return(mqtt.Session{},
		mqtt.ErrSessionNotFound)
	store.On("SaveSession", mock.Anything).Return(nil)

	cm := mqtt.NewConnectionManager(conf, store, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	msg := []byte{
		0x10, 14, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 10, // variable header
		0,         // property length
		0, 1, 'a', // client ID
	}

	_, err := conn.Write(msg)
	require.Nil(t, err)

	out := make([]byte, 5)
	_, err = conn.Read(out)
	require.Nil(t, err)

	connAck := []byte{0x20, 3, 0, 0, 0}
	assert.Equal(t, connAck, out)

	_ = conn.Close()
	<-done
}

func TestConnectionManager_ConnectV5MaxPacketSize(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	conf := newConfiguration()
	conf.MaxPacketSize = 65536

	store := &sessionStoreMock{}
	store.On("GetSession", mock.Anything).Return(mqtt.Session{},
		mqtt.ErrSessionNotFound)
	store.On("SaveSession", mock.Anything).Return(nil)

	cm := mqtt.NewConnectionManager(conf, store, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	msg := []byte{
		0x10, 14, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 10, // variable header
		0,         // property length
		0, 1, 'a', // client ID
	}

	_, err := conn.Write(msg)
	require.Nil(t, err)

	out := make([]byte, 10)
	_, err = conn.Read(out)
	require.Nil(t, err)

	connAck := []byte{0x20, 8, 0, 0, 5, 39, 0, 1, 0, 0} // MaxPacketSize
	assert.Equal(t, connAck, out)

	_ = conn.Close()
	<-done
}

func TestConnectionManager_ConnectV5MaximumQoS(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	conf := newConfiguration()
	conf.MaximumQoS = 1

	store := &sessionStoreMock{}
	store.On("GetSession", mock.Anything).Return(mqtt.Session{},
		mqtt.ErrSessionNotFound)
	store.On("SaveSession", mock.Anything).Return(nil)

	cm := mqtt.NewConnectionManager(conf, store, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	msg := []byte{
		0x10, 14, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 10, // variable header
		0,         // property length
		0, 1, 'a', // client ID
	}

	_, err := conn.Write(msg)
	require.Nil(t, err)

	out := make([]byte, 7)
	_, err = conn.Read(out)
	require.Nil(t, err)

	connAck := []byte{0x20, 5, 0, 0, 2, 36, 1} // MaximumQoS
	assert.Equal(t, connAck, out)

	_ = conn.Close()
	<-done
}

func TestConnectionManager_ConnectV5TopicAliasMaximum(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	conf := newConfiguration()
	conf.MaxTopicAlias = 10

	store := &sessionStoreMock{}
	store.On("GetSession", mock.Anything).Return(mqtt.Session{},
		mqtt.ErrSessionNotFound)
	store.On("SaveSession", mock.Anything).Return(nil)

	cm := mqtt.NewConnectionManager(conf, store, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	msg := []byte{
		0x10, 14, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 10, // variable header
		0,         // property length
		0, 1, 'a', // client ID
	}

	_, err := conn.Write(msg)
	require.Nil(t, err)

	out := make([]byte, 8)
	_, err = conn.Read(out)
	require.Nil(t, err)

	connAck := []byte{0x20, 6, 0, 0, 3, 34, 0, 10} // TopicAliasMaximum
	assert.Equal(t, connAck, out)

	_ = conn.Close()
	<-done
}

func TestConnectionManager_ConnectV5RetainAvailable(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	conf := newConfiguration()
	conf.RetainAvailable = false

	store := &sessionStoreMock{}
	store.On("GetSession", mock.Anything).Return(mqtt.Session{},
		mqtt.ErrSessionNotFound)
	store.On("SaveSession", mock.Anything).Return(nil)

	cm := mqtt.NewConnectionManager(conf, store, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	msg := []byte{
		0x10, 14, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 10, // variable header
		0,         // property length
		0, 1, 'a', // client ID
	}

	_, err := conn.Write(msg)
	require.Nil(t, err)

	out := make([]byte, 7)
	_, err = conn.Read(out)
	require.Nil(t, err)

	connAck := []byte{0x20, 5, 0, 0, 2, 37, 0} // RetainAvailable
	assert.Equal(t, connAck, out)

	_ = conn.Close()
	<-done
}

func TestConnectionManager_ConnectV5WildcardSubsAvailable(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	conf := newConfiguration()
	conf.WildcardSubscriptionAvailable = false

	store := &sessionStoreMock{}
	store.On("GetSession", mock.Anything).Return(mqtt.Session{},
		mqtt.ErrSessionNotFound)
	store.On("SaveSession", mock.Anything).Return(nil)

	cm := mqtt.NewConnectionManager(conf, store, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	msg := []byte{
		0x10, 14, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 10, // variable header
		0,         // property length
		0, 1, 'a', // client ID
	}

	_, err := conn.Write(msg)
	require.Nil(t, err)

	out := make([]byte, 7)
	_, err = conn.Read(out)
	require.Nil(t, err)

	connAck := []byte{0x20, 5, 0, 0, 2, 40, 0} // WildcardSubscriptionAvailable
	assert.Equal(t, connAck, out)

	_ = conn.Close()
	<-done
}

func TestConnectionManager_ConnectV5SubscriptionIDAvailable(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	conf := newConfiguration()
	conf.SubscriptionIDAvailable = false

	store := &sessionStoreMock{}
	store.On("GetSession", mock.Anything).Return(mqtt.Session{},
		mqtt.ErrSessionNotFound)
	store.On("SaveSession", mock.Anything).Return(nil)

	cm := mqtt.NewConnectionManager(conf, store, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	msg := []byte{
		0x10, 14, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 10, // variable header
		0,         // property length
		0, 1, 'a', // client ID
	}

	_, err := conn.Write(msg)
	require.Nil(t, err)

	out := make([]byte, 7)
	_, err = conn.Read(out)
	require.Nil(t, err)

	connAck := []byte{0x20, 5, 0, 0, 2, 41, 0} // SubscriptionIDAvailable
	assert.Equal(t, connAck, out)

	_ = conn.Close()
	<-done
}

func TestConnectionManager_ConnectV5SharedSubscriptionAvailable(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	conf := newConfiguration()
	conf.SharedSubscriptionAvailable = false

	store := &sessionStoreMock{}
	store.On("GetSession", mock.Anything).Return(mqtt.Session{},
		mqtt.ErrSessionNotFound)
	store.On("SaveSession", mock.Anything).Return(nil)

	cm := mqtt.NewConnectionManager(conf, store, logStub.Logger())
	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	msg := []byte{
		0x10, 14, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 10, // variable header
		0,         // property length
		0, 1, 'a', // client ID
	}

	_, err := conn.Write(msg)
	require.Nil(t, err)

	out := make([]byte, 7)
	_, err = conn.Read(out)
	require.Nil(t, err)

	connAck := []byte{0x20, 5, 0, 0, 2, 42, 0} // SharedSubscriptionAvailable
	assert.Equal(t, connAck, out)

	_ = conn.Close()
	<-done
}

func TestConnectionManager_ConnectV5UserProperty(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	conf := newConfiguration()
	conf.UserProperties = map[string]string{"k1": "v1"}

	store := &sessionStoreMock{}
	store.On("GetSession", mock.Anything).Return(mqtt.Session{},
		mqtt.ErrSessionNotFound)
	store.On("SaveSession", mock.Anything).Return(nil)

	cm := mqtt.NewConnectionManager(conf, store, logStub.Logger())
	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	msg := []byte{
		0x10, 14, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 10, // variable header
		0,         // property length
		0, 1, 'a', // client ID
	}

	_, err := conn.Write(msg)
	require.Nil(t, err)

	out := make([]byte, 14)
	_, err = conn.Read(out)
	require.Nil(t, err)

	connAck := []byte{
		0x20, 12, 0, 0, 9,
		38, 0, 2, 'k', '1', 0, 2, 'v', '1',
	} // RetainAvailable
	assert.Equal(t, connAck, out)

	_ = conn.Close()
	<-done
}

func TestConnectionManager_PingReq(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	conf := newConfiguration()
	conf.MetricsEnabled = true

	store := &sessionStoreMock{}
	store.On("GetSession", mock.Anything).Return(mqtt.Session{},
		mqtt.ErrSessionNotFound)
	store.On("SaveSession", mock.Anything).Return(nil)

	cm := mqtt.NewConnectionManager(conf, store, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	connPkt := []byte{0x10, 13, 0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 5, 0, 1, 'a'}
	_, _ = conn.Write(connPkt)
	out := make([]byte, 4)
	_, err := conn.Read(out)
	require.Nil(t, err)
	require.Equal(t, byte(0x20), out[0])

	pingReq := []byte{0xC0, 0}
	_, _ = conn.Write(pingReq)

	out = make([]byte, 2)
	_, err = conn.Read(out)
	require.Nil(t, err)

	pingResp := []byte{0xD0, 0}
	assert.Equal(t, pingResp, out)

	_ = conn.Close()
	<-done
}

func TestConnectionManager_PingReqWithoutConnect(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	store := &sessionStoreMock{}
	conf := newConfiguration()

	cm := mqtt.NewConnectionManager(conf, store, logStub.Logger())

	conn, sConn := net.Pipe()
	defer func() { _ = conn.Close() }()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	pingReq := []byte{0xC0, 0}
	_, _ = conn.Write(pingReq)

	<-done
}

func TestConnectionManager_Disconnect(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	conf := newConfiguration()
	conf.MetricsEnabled = true

	store := &sessionStoreMock{}
	store.On("GetSession", mock.Anything).Return(mqtt.Session{},
		mqtt.ErrSessionNotFound)
	store.On("SaveSession", mock.Anything).Return(nil)
	store.On("DeleteSession", mock.Anything).Return(nil)

	cm := mqtt.NewConnectionManager(conf, store, logStub.Logger())

	conn, sConn := net.Pipe()
	defer func() { _ = conn.Close() }()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	connPkt := []byte{0x10, 13, 0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 5, 0, 1, 'a'}
	_, _ = conn.Write(connPkt)
	out := make([]byte, 4)
	_, err := conn.Read(out)
	require.Nil(t, err)
	require.Equal(t, byte(0x20), out[0])

	discPkt := []byte{0xE0, 0}
	_, _ = conn.Write(discPkt)

	<-done
}

func TestConnectionManager_NetConnClosed(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	store := &sessionStoreMock{}
	cm := mqtt.NewConnectionManager(newConfiguration(), store, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	<-time.After(time.Millisecond)
	_ = conn.Close()

	<-done
	assert.Contains(t, logStub.String(), "Connection was closed")
}

func TestConnectionManager_SetDeadlineFailure(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	store := &sessionStoreMock{}
	cm := mqtt.NewConnectionManager(newConfiguration(), store, logStub.Logger())

	conn, sConn := net.Pipe()
	_ = conn.Close()

	c := cm.NewConnection(sConn)
	cm.Handle(c)
	assert.Contains(t, logStub.String(), "Failed to set read deadline")
}

func TestConnectionManager_ReadFailure(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	store := &sessionStoreMock{}
	cm := mqtt.NewConnectionManager(newConfiguration(), store, logStub.Logger())

	conn, sConn := net.Pipe()
	defer func() { _ = conn.Close() }()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	_, err := conn.Write([]byte{0x15, 13})
	require.Nil(t, err)

	<-done
	assert.Contains(t, logStub.String(), "Failed to read packet")
}

func TestConnectionManager_ReadTimeout(t *testing.T) {
	logStub := mocks.NewLoggerStub()

	store := &sessionStoreMock{}
	store.On("GetSession", mock.Anything).Return(mqtt.Session{},
		mqtt.ErrSessionNotFound)
	store.On("SaveSession", mock.Anything).Return(nil)

	cm := mqtt.NewConnectionManager(newConfiguration(), store, logStub.Logger())

	conn, sConn := net.Pipe()
	defer func() { _ = conn.Close() }()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	msg := []byte{
		0x10, 13, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 1, // variable header
		0, 1, 'a', // client ID
	}

	_, err := conn.Write(msg)
	require.Nil(t, err)

	out := make([]byte, 4)
	_, err = conn.Read(out)
	require.Nil(t, err)

	connAck := []byte{0x20, 2, 0, 0}
	assert.Equal(t, connAck, out)

	<-done
	assert.Contains(t, logStub.String(), "Timeout - No packet received")
}

func TestConnectionManager_Close(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	store := &sessionStoreMock{}
	cm := mqtt.NewConnectionManager(newConfiguration(), store, logStub.Logger())

	lsn, err := net.Listen("tcp", "")
	require.Nil(t, err)

	done := make(chan bool)
	go func() {
		defer func() {
			done <- true
		}()

		tcpConn, err := lsn.Accept()
		require.Nil(t, err)

		conn := cm.NewConnection(tcpConn)
		cm.Close(&conn, true)
		cm.Close(&conn, false)
	}()

	conn, err := net.Dial("tcp", lsn.Addr().String())
	require.Nil(t, err)
	defer func() { _ = conn.Close() }()

	<-done
	assert.Contains(t, logStub.String(), "Closing connection")
}