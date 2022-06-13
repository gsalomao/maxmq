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
	"net"
	"testing"
	"time"

	"github.com/gsalomao/maxmq/mocks"
	"github.com/gsalomao/maxmq/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func createClient(nc net.Conn, w *packet.Writer, conf Configuration) client {
	logStub := mocks.NewLoggerStub()
	store := &sessionStoreMock{}
	store.On("GetSession", mock.Anything).Return(Session{},
		ErrSessionNotFound)
	store.On("SaveSession", mock.Anything).Return(nil)

	userProps := make([]packet.UserProperty, 0, len(conf.UserProperties))
	for k, v := range conf.UserProperties {
		userProps = append(userProps,
			packet.UserProperty{Key: []byte(k), Value: []byte(v)})
	}

	return newClient(clientOptions{
		log:            logStub.Logger(),
		metrics:        newMetrics(true, logStub.Logger()),
		conf:           &conf,
		sessionStore:   store,
		netConn:        nc,
		writer:         w,
		userProperties: userProps,
	})
}

func checkPacketConnect(t *testing.T, conf Configuration, pkt packet.Connect,
	resp []byte, hasError bool) {

	wr := packet.NewWriter(1024)
	conn, sConn := net.Pipe()
	done := make(chan bool)

	go func() {
		out := make([]byte, len(resp))
		_, err := conn.Read(out)
		assert.Nil(t, err)
		assert.Equal(t, resp, out)
		done <- true
	}()

	cl := createClient(sConn, &wr, conf)
	err := cl.handlePacket(&pkt)
	if hasError {
		assert.NotNil(t, err)
	} else {
		assert.Nil(t, err)
	}

	<-done
	_ = conn.Close()
}

func connectClient(c *client, pkt packet.Connect) (net.Conn, error) {
	conn, sConn := net.Pipe()
	done := make(chan bool)

	go func() {
		respLen := 4
		if pkt.Version == packet.MQTT50 {
			respLen = 5
		}

		out := make([]byte, respLen)
		_, _ = conn.Read(out)
		done <- true
	}()

	c.netConn = sConn
	return conn, c.handlePacket(&pkt)
}

func TestClient_ConnectNewSession(t *testing.T) {
	testCases := []struct {
		version packet.MQTTVersion
		resp    []byte
	}{
		{version: packet.MQTT31, resp: []byte{0x20, 2, 0, 0}},
		{version: packet.MQTT311, resp: []byte{0x20, 2, 0, 0}},
		{version: packet.MQTT50, resp: []byte{0x20, 3, 0, 0, 0}},
	}

	for _, test := range testCases {
		t.Run(test.version.String(), func(t *testing.T) {
			conf := newConfiguration()
			pkt := packet.Connect{ClientID: ClientID{'a'},
				Version: test.version, Properties: &packet.Properties{}}

			checkPacketConnect(t, conf, pkt, test.resp, false)
		})
	}
}

func TestClient_ConnectExistingSession(t *testing.T) {
	testCases := []struct {
		version packet.MQTTVersion
		resp    []byte
	}{
		{version: packet.MQTT31, resp: []byte{0x20, 2, 0, 0}},
		{version: packet.MQTT311, resp: []byte{0x20, 2, 1, 0}},
		{version: packet.MQTT50, resp: []byte{0x20, 3, 1, 0, 0}},
	}

	for _, test := range testCases {
		t.Run(test.version.String(), func(t *testing.T) {
			conf := newConfiguration()
			wr := packet.NewWriter(1024)
			conn, sConn := net.Pipe()
			done := make(chan bool)

			go func() {
				out := make([]byte, len(test.resp))
				_, err := conn.Read(out)
				assert.Nil(t, err)
				assert.Equal(t, test.resp, out)
				done <- true
			}()

			clientID := ClientID{'a'}
			pkt := packet.Connect{ClientID: clientID, Version: test.version}

			c := createClient(sConn, &wr, conf)
			session := Session{
				ConnectedAt:    time.Now().Add(-1 * time.Minute).Unix(),
				ExpiryInterval: conf.MaxSessionExpiryInterval,
			}

			store := &sessionStoreMock{}
			store.On("GetSession", clientID).Return(session, nil)
			store.On("SaveSession", mock.Anything).Return(nil)
			c.sessionStore = store

			err := c.handlePacket(&pkt)
			assert.Nil(t, err)

			<-done
			_ = conn.Close()
		})
	}
}

func TestClient_ConnectFailedToGetSession(t *testing.T) {
	conf := newConfiguration()
	wr := packet.NewWriter(1024)
	_, sConn := net.Pipe()
	c := createClient(sConn, &wr, conf)

	store := &sessionStoreMock{}
	store.On("GetSession", mock.Anything).Return(Session{},
		errors.New("failed to get session"))
	c.sessionStore = store

	pkt := packet.Connect{ClientID: ClientID{'a'}, Version: packet.MQTT311}
	err := c.handlePacket(&pkt)
	assert.NotNil(t, err)
}

func TestClient_ConnectFailedToSaveSession(t *testing.T) {
	conf := newConfiguration()
	wr := packet.NewWriter(1024)
	_, sConn := net.Pipe()
	c := createClient(sConn, &wr, conf)

	store := &sessionStoreMock{}
	store.On("GetSession", mock.Anything).Return(Session{},
		ErrSessionNotFound)
	store.On("SaveSession",
		mock.Anything).Return(errors.New("failed to save session"))
	c.sessionStore = store

	pkt := packet.Connect{ClientID: ClientID{'a'}, Version: packet.MQTT311}
	err := c.handlePacket(&pkt)
	assert.NotNil(t, err)
}

func TestClient_ConnectFailedToSendConnAck(t *testing.T) {
	conf := newConfiguration()
	wr := packet.NewWriter(1024)
	_, sConn := net.Pipe()
	c := createClient(sConn, &wr, conf)

	store := c.sessionStore.(*sessionStoreMock)
	store.On("DeleteSession", mock.Anything).Return(nil)

	_ = sConn.Close()

	pkt := packet.Connect{ClientID: ClientID{'a'}, Version: packet.MQTT311}
	err := c.handlePacket(&pkt)
	assert.NotNil(t, err)
}

func TestClient_ConnectFailedToSendConnAckAndDelete(t *testing.T) {
	conf := newConfiguration()
	wr := packet.NewWriter(1024)
	_, sConn := net.Pipe()
	c := createClient(sConn, &wr, conf)

	store := c.sessionStore.(*sessionStoreMock)
	store.On("DeleteSession",
		mock.Anything).Return(errors.New("failed to delete session"))

	_ = sConn.Close()

	pkt := packet.Connect{ClientID: ClientID{'a'}, Version: packet.MQTT311}
	err := c.handlePacket(&pkt)
	assert.NotNil(t, err)
}

func TestClient_ConnectClientIDTooBig(t *testing.T) {
	testCases := []struct {
		id       ClientID
		maxIDLen int
		version  packet.MQTTVersion
		resp     []byte
	}{
		{id: ClientID("012345678901234567890123"), maxIDLen: 65535,
			version: packet.MQTT31, resp: []byte{0x20, 2, 0, 2},
		},
		{id: ClientID("0123456789012345678901234567890"), maxIDLen: 30,
			version: packet.MQTT311, resp: []byte{0x20, 2, 0, 2},
		},
		{id: ClientID("0123456789012345678901234567890"), maxIDLen: 30,
			version: packet.MQTT50, resp: []byte{0x20, 3, 0, 0x85, 0},
		},
	}

	for _, test := range testCases {
		t.Run(test.version.String(), func(t *testing.T) {
			conf := newConfiguration()
			conf.MaxClientIDLen = test.maxIDLen

			clientID := []byte(test.id)
			pkt := packet.Connect{ClientID: clientID, Version: test.version}

			checkPacketConnect(t, conf, pkt, test.resp, true)
		})
	}
}

func TestClient_ConnectAllowEmptyClientID(t *testing.T) {
	testCases := []struct{ version packet.MQTTVersion }{
		{version: packet.MQTT31},
		{version: packet.MQTT311},
	}

	for _, test := range testCases {
		t.Run(test.version.String(), func(t *testing.T) {
			conf := newConfiguration()
			conf.AllowEmptyClientID = true

			pkt := packet.Connect{Version: test.version}
			expected := []byte{0x20, 2, 0, 0}
			checkPacketConnect(t, conf, pkt, expected, false)
		})
	}
}

func TestClient_ConnectDenyEmptyClientID(t *testing.T) {
	testCases := []struct {
		version packet.MQTTVersion
		resp    []byte
	}{
		{version: packet.MQTT31, resp: []byte{0x20, 2, 0, 2}},
		{version: packet.MQTT311, resp: []byte{0x20, 2, 0, 2}},
		{version: packet.MQTT50, resp: []byte{0x20, 3, 0, 0x85, 0}},
	}

	for _, test := range testCases {
		t.Run(test.version.String(), func(t *testing.T) {
			conf := newConfiguration()
			conf.AllowEmptyClientID = false

			pkt := packet.Connect{Version: test.version}
			checkPacketConnect(t, conf, pkt, test.resp, true)
		})
	}
}

func TestClient_ConnectAssignClientID(t *testing.T) {
	conf := newConfiguration()
	conf.AllowEmptyClientID = true

	wr := packet.NewWriter(1024)
	conn, sConn := net.Pipe()
	done := make(chan bool)

	go func() {
		out := make([]byte, 28)
		_, err := conn.Read(out)
		assert.Nil(t, err)

		resp := []byte{0x20, 26, 0, 0, 23, 18, 0, 20} // assigned ID
		assert.Equal(t, resp, out[:len(resp)])
		done <- true
	}()

	pkt := packet.Connect{Version: packet.MQTT50}

	c := createClient(sConn, &wr, conf)
	err := c.handlePacket(&pkt)
	assert.Nil(t, err)

	<-done
	_ = conn.Close()
}

func TestClient_ConnectAssignClientIDWithPrefix(t *testing.T) {
	conf := newConfiguration()
	conf.AllowEmptyClientID = true
	conf.ClientIDPrefix = []byte("MAX-")

	wr := packet.NewWriter(1024)
	conn, sConn := net.Pipe()
	done := make(chan bool)

	go func() {
		out := make([]byte, 32)
		_, err := conn.Read(out)
		assert.Nil(t, err)

		resp := []byte{0x20, 30, 0, 0, 27, 18, 0, 24, 'M', 'A', 'X', '-'}
		assert.Equal(t, resp, out[:len(resp)])
		done <- true
	}()

	pkt := packet.Connect{Version: packet.MQTT50}

	c := createClient(sConn, &wr, conf)
	err := c.handlePacket(&pkt)
	assert.Nil(t, err)

	<-done
	_ = conn.Close()
}

func TestClient_ConnectMaxSessionExpiryInterval(t *testing.T) {
	testCases := []struct {
		interval    uint32
		maxInterval uint32
		resp        []byte
	}{
		{interval: 0, maxInterval: 100, resp: []byte{0x20, 3, 0, 0, 0}},
		{interval: 100, maxInterval: 100, resp: []byte{0x20, 3, 0, 0, 0}},
		{interval: 101, maxInterval: 100,
			resp: []byte{0x20, 8, 0, 0, 5, 17, 0, 0, 0, 100}},
		{interval: 2000, maxInterval: 1000,
			resp: []byte{0x20, 8, 0, 0, 5, 17, 0, 0, 0x03, 0xE8}},
		{interval: 100000, maxInterval: 80000,
			resp: []byte{0x20, 8, 0, 0, 5, 17, 0, 1, 0x38, 0x80},
		},
		{interval: 50000000, maxInterval: 32000000,
			resp: []byte{0x20, 8, 0, 0, 5, 17, 1, 0xE8, 0x48, 0}},
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

			checkPacketConnect(t, conf, pkt, test.resp, false)
		})
	}
}

func TestClient_ConnectMaxKeepAlive(t *testing.T) {
	testCases := []struct {
		version      packet.MQTTVersion
		keepAlive    uint16
		maxKeepAlive int
		resp         []byte
		invalid      bool
	}{
		{version: packet.MQTT31, keepAlive: 0, maxKeepAlive: 100,
			resp: []byte{0x20, 2, 0, 2}, invalid: true,
		},
		{version: packet.MQTT311, keepAlive: 100, maxKeepAlive: 100,
			resp: []byte{0x20, 2, 0, 0}, invalid: false,
		},
		{version: packet.MQTT31, keepAlive: 101, maxKeepAlive: 100,
			resp: []byte{0x20, 2, 0, 2}, invalid: true,
		},
		{version: packet.MQTT311, keepAlive: 501, maxKeepAlive: 500,
			resp: []byte{0x20, 2, 0, 2}, invalid: true,
		},
		{version: packet.MQTT311, keepAlive: 65535, maxKeepAlive: 65534,
			resp: []byte{0x20, 2, 0, 2}, invalid: true,
		},
		{version: packet.MQTT50, keepAlive: 200, maxKeepAlive: 100,
			resp: []byte{0x20, 6, 0, 0, 3, 19, 0, 100}, invalid: false,
		},
	}

	for _, test := range testCases {
		testName := fmt.Sprintf("v%v-%v/%v", test.version.String(),
			test.keepAlive, test.maxKeepAlive)

		t.Run(testName, func(t *testing.T) {
			conf := newConfiguration()
			conf.MaxKeepAlive = test.maxKeepAlive

			pkt := packet.Connect{ClientID: ClientID{'a'},
				Version: test.version, KeepAlive: test.keepAlive}

			checkPacketConnect(t, conf, pkt, test.resp, test.invalid)
		})
	}
}

func TestClient_ConnectMaxInflightMessages(t *testing.T) {
	testCases := []struct {
		maxInflight int
		resp        []byte
	}{
		{maxInflight: 0, resp: []byte{0x20, 3, 0, 0, 0}},
		{maxInflight: 255, resp: []byte{0x20, 6, 0, 0, 3, 33, 0, 255}},
		{maxInflight: 65534, resp: []byte{0x20, 6, 0, 0, 3, 33, 0xFF, 0xFE}},
		{maxInflight: 65535, resp: []byte{0x20, 3, 0, 0, 0}},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprint(test.maxInflight), func(t *testing.T) {
			conf := newConfiguration()
			conf.MaxInflightMessages = test.maxInflight

			pkt := packet.Connect{ClientID: ClientID{'a'},
				Version: packet.MQTT50, Properties: &packet.Properties{}}

			checkPacketConnect(t, conf, pkt, test.resp, false)
		})
	}
}

func TestClient_ConnectMaxPacketSize(t *testing.T) {
	testCases := []struct {
		maxSize int
		resp    []byte
	}{
		{maxSize: 0, resp: []byte{0x20, 3, 0, 0, 0}},
		{maxSize: 255, resp: []byte{0x20, 8, 0, 0, 5, 39, 0, 0, 0, 255}},
		{maxSize: 65535, resp: []byte{0x20, 8, 0, 0, 5, 39, 0, 0, 0xFF, 0xFF}},
		{maxSize: 16777215,
			resp: []byte{0x20, 8, 0, 0, 5, 39, 0, 0xFF, 0xFF, 0xFF}},
		{maxSize: 268435455,
			resp: []byte{0x20, 8, 0, 0, 5, 39, 0x0F, 0xFF, 0xFF, 0xFF}},
		{maxSize: 268435456,
			resp: []byte{0x20, 3, 0, 0, 0}},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprint(test.maxSize), func(t *testing.T) {
			conf := newConfiguration()
			conf.MaxPacketSize = test.maxSize

			pkt := packet.Connect{ClientID: ClientID{'a'},
				Version: packet.MQTT50}

			checkPacketConnect(t, conf, pkt, test.resp, false)
		})
	}
}

func TestClient_ConnectMaximumQoS(t *testing.T) {
	testCases := []struct {
		maxQoS packet.QoS
		resp   []byte
	}{
		{maxQoS: packet.QoS0, resp: []byte{0x20, 5, 0, 0, 2, 36, 0}},
		{maxQoS: packet.QoS1, resp: []byte{0x20, 5, 0, 0, 2, 36, 1}},
		{maxQoS: packet.QoS2, resp: []byte{0x20, 3, 0, 0, 0}},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprint(test.maxQoS), func(t *testing.T) {
			conf := newConfiguration()
			conf.MaximumQoS = int(test.maxQoS)

			pkt := packet.Connect{ClientID: ClientID{'a'},
				Version: packet.MQTT50}

			checkPacketConnect(t, conf, pkt, test.resp, false)
		})
	}
}

func TestClient_ConnectTopicAliasMaximum(t *testing.T) {
	testCases := []struct {
		maxAlias int
		resp     []byte
	}{
		{maxAlias: 0, resp: []byte{0x20, 3, 0, 0, 0}},
		{maxAlias: 255, resp: []byte{0x20, 6, 0, 0, 3, 34, 0, 255}},
		{maxAlias: 65535, resp: []byte{0x20, 6, 0, 0, 3, 34, 0xFF, 0xFF}},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprint(test.maxAlias), func(t *testing.T) {
			conf := newConfiguration()
			conf.MaxTopicAlias = test.maxAlias

			pkt := packet.Connect{ClientID: ClientID{'a'},
				Version: packet.MQTT50}

			checkPacketConnect(t, conf, pkt, test.resp, false)
		})
	}
}

func TestClient_ConnectRetainAvailable(t *testing.T) {
	testCases := []struct {
		available bool
		resp      []byte
	}{
		{available: false, resp: []byte{0x20, 5, 0, 0, 2, 37, 0}},
		{available: true, resp: []byte{0x20, 3, 0, 0, 0}},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprint(test.available), func(t *testing.T) {
			conf := newConfiguration()
			conf.RetainAvailable = test.available

			pkt := packet.Connect{ClientID: ClientID{'a'},
				Version: packet.MQTT50}

			checkPacketConnect(t, conf, pkt, test.resp, false)
		})
	}
}

func TestClient_ConnectWildcardSubscriptionAvailable(t *testing.T) {
	testCases := []struct {
		available bool
		resp      []byte
	}{
		{available: false, resp: []byte{0x20, 5, 0, 0, 2, 40, 0}},
		{available: true, resp: []byte{0x20, 3, 0, 0, 0}},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprint(test.available), func(t *testing.T) {
			conf := newConfiguration()
			conf.WildcardSubscriptionAvailable = test.available

			pkt := packet.Connect{ClientID: ClientID{'a'},
				Version: packet.MQTT50}

			checkPacketConnect(t, conf, pkt, test.resp, false)
		})
	}
}

func TestClient_ConnectSubscriptionIDAvailable(t *testing.T) {
	testCases := []struct {
		available bool
		resp      []byte
	}{
		{available: false, resp: []byte{0x20, 5, 0, 0, 2, 41, 0}},
		{available: true, resp: []byte{0x20, 3, 0, 0, 0}},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprint(test.available), func(t *testing.T) {
			conf := newConfiguration()
			conf.SubscriptionIDAvailable = test.available

			pkt := packet.Connect{ClientID: ClientID{'a'},
				Version: packet.MQTT50}

			checkPacketConnect(t, conf, pkt, test.resp, false)
		})
	}
}

func TestClient_ConnectSharedSubscriptionAvailable(t *testing.T) {
	testCases := []struct {
		available bool
		resp      []byte
	}{
		{available: false, resp: []byte{0x20, 5, 0, 0, 2, 42, 0}},
		{available: true, resp: []byte{0x20, 3, 0, 0, 0}},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprint(test.available), func(t *testing.T) {
			conf := newConfiguration()
			conf.SharedSubscriptionAvailable = test.available

			pkt := packet.Connect{ClientID: ClientID{'a'},
				Version: packet.MQTT50, Properties: &packet.Properties{}}

			checkPacketConnect(t, conf, pkt, test.resp, false)
		})
	}
}

func TestClient_ConnectUserProperty(t *testing.T) {
	conf := newConfiguration()
	conf.UserProperties = map[string]string{"k1": "v1"}

	pkt := packet.Connect{ClientID: ClientID{'a'}, Version: packet.MQTT50}
	expected := []byte{0x20, 12, 0, 0, 9, 38, 0, 2, 'k', '1', 0, 2, 'v', '1'}
	checkPacketConnect(t, conf, pkt, expected, false)
}

func TestClient_PingReq(t *testing.T) {
	conf := newConfiguration()
	wr := packet.NewWriter(1024)
	cl := createClient(nil, &wr, conf)

	connect := packet.Connect{ClientID: ClientID{'a'}, Version: packet.MQTT311}
	conn, err := connectClient(&cl, connect)
	require.Nil(t, err)

	done := make(chan bool)
	go func() {
		expected := []byte{0xD0, 0}
		out := make([]byte, len(expected))
		_, err := conn.Read(out)
		assert.Nil(t, err)
		assert.Equal(t, expected, out)
		done <- true
	}()

	pkt := packet.PingReq{}
	err = cl.handlePacket(&pkt)
	assert.Nil(t, err)

	<-done
	_ = conn.Close()
}

func TestClient_PingReqWithoutConnect(t *testing.T) {
	conf := newConfiguration()
	wr := packet.NewWriter(1024)
	cl := createClient(nil, &wr, conf)
	conn, _ := net.Pipe()

	pkt := packet.PingReq{}
	err := cl.handlePacket(&pkt)
	assert.NotNil(t, err)
	_ = conn.Close()
}

func TestClient_FailedToSendPingResp(t *testing.T) {
	conf := newConfiguration()
	wr := packet.NewWriter(1024)
	cl := createClient(nil, &wr, conf)

	connect := packet.Connect{ClientID: ClientID{'a'}, Version: packet.MQTT311}
	conn, err := connectClient(&cl, connect)
	require.Nil(t, err)

	_ = conn.Close()

	pkt := packet.PingReq{}
	err = cl.handlePacket(&pkt)
	assert.NotNil(t, err)
}

func TestClient_Disconnect(t *testing.T) {
	conf := newConfiguration()
	wr := packet.NewWriter(1024)
	cl := createClient(nil, &wr, conf)

	connect := packet.Connect{ClientID: ClientID{'a'}, Version: packet.MQTT311}
	conn, err := connectClient(&cl, connect)
	require.Nil(t, err)

	store := cl.sessionStore.(*sessionStoreMock)
	store.On("DeleteSession", mock.Anything).Return(nil)

	disconnect := packet.Disconnect{}
	err = cl.handlePacket(&disconnect)
	assert.Nil(t, err)
	_ = conn.Close()
}

func TestClient_DisconnectFailedToDeleteSession(t *testing.T) {
	conf := newConfiguration()
	wr := packet.NewWriter(1024)
	cl := createClient(nil, &wr, conf)

	connect := packet.Connect{ClientID: ClientID{'a'}, Version: packet.MQTT311}
	conn, err := connectClient(&cl, connect)
	require.Nil(t, err)

	store := cl.sessionStore.(*sessionStoreMock)
	store.On("DeleteSession",
		mock.Anything).Return(errors.New("failed to delete session"))

	disconnect := packet.Disconnect{}
	err = cl.handlePacket(&disconnect)
	assert.Nil(t, err)
	_ = conn.Close()
}

func TestClient_HandleInvalidPacket(t *testing.T) {
	conf := newConfiguration()
	wr := packet.NewWriter(1024)
	_, sConn := net.Pipe()
	cl := createClient(sConn, &wr, conf)

	connect := packet.Connect{ClientID: ClientID{'a'}, Version: packet.MQTT311}
	_, err := connectClient(&cl, connect)
	require.Nil(t, err)

	pkt := packet.PingResp{}
	err = cl.handlePacket(&pkt)
	assert.NotNil(t, err)
}
