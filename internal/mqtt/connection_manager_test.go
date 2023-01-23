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
	"net"
	"testing"
	"time"

	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const sessionIDTest = 1

func newConfiguration() Configuration {
	return Configuration{
		TCPAddress:                    ":1883",
		ConnectTimeout:                5,
		BufferSize:                    1024,
		DefaultVersion:                4,
		MaxPacketSize:                 268435456,
		MaxKeepAlive:                  0,
		MaxSessionExpiryInterval:      0,
		MaxInflightMessages:           0,
		MaxInflightRetries:            0,
		MaximumQoS:                    2,
		MaxTopicAlias:                 0,
		RetainAvailable:               true,
		WildcardSubscriptionAvailable: true,
		SubscriptionIDAvailable:       true,
		SharedSubscriptionAvailable:   true,
		MaxClientIDLen:                65535,
		AllowEmptyClientID:            true,
		UserProperties:                map[string]string{},
		MetricsEnabled:                true,
	}
}

func createConnectionManager(conf Configuration) *connectionManager {
	log := newLogger()
	idGen := &idGeneratorMock{}
	idGen.On("NextID").Return(sessionIDTest)

	mt := newMetrics(conf.MetricsEnabled, &log)
	cm := newConnectionManager(&conf, mt, &log)
	ps := newPubSubManager(mt, &log)
	sm := newSessionManager(&conf, idGen, mt, nil, &log)

	ps.publisher = sm
	sm.pubSub = ps
	sm.deliverer = cm
	cm.sessionManager = sm
	return cm
}

func TestConnectionManagerDefaultValues(t *testing.T) {
	conf := newConfiguration()
	conf.BufferSize = 0
	conf.MaxPacketSize = 0
	conf.ConnectTimeout = 0
	conf.DefaultVersion = 0
	conf.MaximumQoS = 3
	conf.MaxTopicAlias = 1000000
	conf.MaxInflightMessages = 1000000
	conf.MaxInflightRetries = 1000000
	conf.MaxClientIDLen = 0

	cm := createConnectionManager(conf)
	assert.Equal(t, 1024, cm.conf.BufferSize)
	assert.Equal(t, 268435456, cm.conf.MaxPacketSize)
	assert.Equal(t, 5, cm.conf.ConnectTimeout)
	assert.Equal(t, 4, cm.conf.DefaultVersion)
	assert.Equal(t, 2, cm.conf.MaximumQoS)
	assert.Equal(t, 0, cm.conf.MaxTopicAlias)
	assert.Equal(t, 0, cm.conf.MaxInflightMessages)
	assert.Equal(t, 0, cm.conf.MaxInflightRetries)
	assert.Equal(t, 23, cm.conf.MaxClientIDLen)
}

func TestConnectionManagerHandlePacket(t *testing.T) {
	conf := newConfiguration()
	conf.UserProperties = map[string]string{"k1": "v1"}

	cm := createConnectionManager(conf)
	require.Empty(t, cm.connections)

	conn, sConn := net.Pipe()
	done := make(chan bool)
	go func() {
		err := cm.handle(sConn)
		assert.Nil(t, err)
		done <- true
	}()

	connect := []byte{0x10, 20, 0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 10, 0, 8,
		'c', 'l', 'i', 'e', 'n', 't', '-', '0'}
	_, err := conn.Write(connect)
	require.Nil(t, err)

	resp := make([]byte, 4)
	_, err = conn.Read(resp)
	require.Nil(t, err)

	connack := []byte{0x20, 2, 0, 0}
	assert.Equal(t, connack, resp)
	<-time.After(10 * time.Millisecond)

	cm.mutex.RLock()
	assert.Equal(t, 1, len(cm.connections))
	c, ok := cm.connections["client-0"]
	cm.mutex.RUnlock()
	require.True(t, ok)
	require.NotNil(t, c)
	assert.True(t, c.connected)
	assert.Equal(t, ClientID("client-0"), c.clientID)
	assert.Equal(t, packet.MQTT311, c.version)
	assert.Equal(t, 10, c.timeout)

	pingReq := []byte{0xC0, 0}
	_, err = conn.Write(pingReq)
	require.Nil(t, err)

	resp = make([]byte, 2)
	_, err = conn.Read(resp)
	require.Nil(t, err)

	pingResp := []byte{0xD0, 0}
	assert.Equal(t, pingResp, resp)

	disconnect := []byte{0xE0, 1, 0}
	_, err = conn.Write(disconnect)
	require.Nil(t, err)

	<-done
	cm.mutex.RLock()
	assert.Empty(t, cm.connections)
	cm.mutex.RUnlock()
	_ = conn.Close()
}

func TestConnectionManagerHandleNetConnClosed(t *testing.T) {
	conf := newConfiguration()
	cm := createConnectionManager(conf)

	conn, sConn := net.Pipe()
	done := make(chan bool)
	go func() {
		err := cm.handle(sConn)
		assert.Nil(t, err)
		done <- true
	}()

	<-time.After(time.Millisecond)
	_ = conn.Close()
	<-done
}

func TestConnectionManagerHandleSetDeadlineFailure(t *testing.T) {
	conf := newConfiguration()
	cm := createConnectionManager(conf)

	conn, sConn := net.Pipe()
	_ = conn.Close()

	err := cm.handle(sConn)
	assert.NotNil(t, err)
}

func TestConnectionManagerHandleReadFailure(t *testing.T) {
	conf := newConfiguration()
	cm := createConnectionManager(conf)

	conn, sConn := net.Pipe()
	defer func() { _ = conn.Close() }()

	done := make(chan bool)
	go func() {
		err := cm.handle(sConn)
		assert.NotNil(t, err)
		done <- true
	}()

	// Invalid packet
	_, err := conn.Write([]byte{0x15, 13})
	require.Nil(t, err)
	<-done
}

func TestConnectionManagerKeepAliveExceeded(t *testing.T) {
	conf := newConfiguration()
	conf.ConnectTimeout = 1
	cm := createConnectionManager(conf)

	conn, sConn := net.Pipe()
	defer func() { _ = conn.Close() }()

	done := make(chan bool)
	go func() {
		err := cm.handle(sConn)
		assert.NotNil(t, err)
		done <- true
	}()

	connect := []byte{0x10, 20, 0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 1, 0, 8,
		'c', 'l', 'i', 'e', 'n', 't', '-', '0'}
	_, err := conn.Write(connect)
	require.Nil(t, err)

	resp := make([]byte, 4)
	_, err = conn.Read(resp)
	require.Nil(t, err)

	connack := []byte{0x20, 2, 0, 0}
	assert.Equal(t, connack, resp)
	<-done
}

func TestConnectionManagerHandleWritePacketFailure(t *testing.T) {
	conf := newConfiguration()
	conf.MetricsEnabled = true

	cm := createConnectionManager(conf)
	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		err := cm.handle(sConn)
		assert.NotNil(t, err)
		done <- true
	}()

	connect := []byte{0x10, 20, 0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 10, 0, 8,
		'c', 'l', 'i', 'e', 'n', 't', '-', '0'}
	_, err := conn.Write(connect)
	_ = conn.Close()
	require.Nil(t, err)
	<-done
}

func TestConnectionManagerHandleInvalidPacket(t *testing.T) {
	conf := newConfiguration()
	conf.MetricsEnabled = true

	cm := createConnectionManager(conf)
	conn, sConn := net.Pipe()
	defer func() { _ = conn.Close() }()

	done := make(chan bool)
	go func() {
		err := cm.handle(sConn)
		assert.NotNil(t, err)
		done <- true
	}()

	pingReq := []byte{0xC0, 0}
	_, err := conn.Write(pingReq)
	require.Nil(t, err)
	<-done
}

func TestConnectionManagerHandleDisconnect(t *testing.T) {
	conf := newConfiguration()
	conf.MetricsEnabled = false
	cm := createConnectionManager(conf)
	conn, sConn := net.Pipe()
	defer func() { _ = conn.Close() }()

	done := make(chan bool)
	go func() {
		err := cm.handle(sConn)
		assert.Nil(t, err)
		done <- true
	}()

	connect := []byte{0x10, 20, 0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 10, 0, 8,
		'c', 'l', 'i', 'e', 'n', 't', '-', '0'}
	_, err := conn.Write(connect)
	require.Nil(t, err)

	resp := make([]byte, 4)
	_, err = conn.Read(resp)
	require.Nil(t, err)

	pingReq := []byte{0xC0, 0}
	_, err = conn.Write(pingReq)
	require.Nil(t, err)

	_, err = conn.Read(resp[:2])
	require.Nil(t, err)

	disconnect := []byte{0xE0, 1, 0}
	_, err = conn.Write(disconnect)
	require.Nil(t, err)
	<-done
}

func TestConnectionManagerDeliverMessage(t *testing.T) {
	conf := newConfiguration()
	conf.MetricsEnabled = false
	cm := createConnectionManager(conf)
	nc, sNc := net.Pipe()
	defer func() { _ = nc.Close() }()

	conn := cm.createConnection(sNc)
	cm.connections["client-a"] = &conn

	go func() {
		pkt := packet.NewPublish(10, packet.MQTT311, "data",
			packet.QoS0, 0, 0, nil, nil)

		err := cm.deliverPacket("client-a", &pkt)
		assert.Nil(t, err)
	}()

	out := make([]byte, 8)
	_, err := nc.Read(out)
	assert.Nil(t, err)

	pub := []byte{0x30, 6, 0, 4, 'd', 'a', 't', 'a'}
	assert.Equal(t, pub, out)
}

func TestConnectionManagerDeliverMessageConnectionNotFound(t *testing.T) {
	conf := newConfiguration()
	conf.MetricsEnabled = false
	cm := createConnectionManager(conf)
	nc, _ := net.Pipe()
	defer func() { _ = nc.Close() }()

	pkt := packet.NewPublish(10, packet.MQTT311, "data",
		packet.QoS0, 0, 0, nil, nil)

	err := cm.deliverPacket("client-b", &pkt)
	assert.NotNil(t, err)
}

func TestConnectionManagerDeliverMessageWriteFailure(t *testing.T) {
	conf := newConfiguration()
	conf.MetricsEnabled = false
	cm := createConnectionManager(conf)
	nc, sNc := net.Pipe()
	_ = nc.Close()

	conn := cm.createConnection(sNc)
	cm.connections["client-a"] = &conn

	pkt := packet.NewPublish(10, packet.MQTT311, "data",
		packet.QoS0, 0, 0, nil, nil)

	err := cm.deliverPacket("client-a", &pkt)
	assert.NotNil(t, err)
}
