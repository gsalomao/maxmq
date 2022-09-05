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
	"net"
	"testing"
	"time"

	"github.com/gsalomao/maxmq/mocks"
	packet "github.com/gsalomao/maxmq/pkg/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newConfiguration() Configuration {
	return Configuration{
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
		MetricsEnabled:                true,
	}
}

func createConnectionManager(conf Configuration) *connectionManager {
	logger := mocks.NewLoggerStub()
	return newConnectionManager(&conf, &idGeneratorMock{}, logger.Logger())
}

func TestConnectionManager_DefaultValues(t *testing.T) {
	conf := newConfiguration()
	conf.BufferSize = 0
	conf.MaxPacketSize = 0
	conf.ConnectTimeout = 0
	conf.MaximumQoS = 3
	conf.MaxTopicAlias = 1000000
	conf.MaxInflightMessages = 1000000
	conf.MaxClientIDLen = 0

	cm := createConnectionManager(conf)
	assert.Equal(t, 1024, cm.conf.BufferSize)
	assert.Equal(t, 268435456, cm.conf.MaxPacketSize)
	assert.Equal(t, 5, cm.conf.ConnectTimeout)
	assert.Equal(t, 2, cm.conf.MaximumQoS)
	assert.Equal(t, 0, cm.conf.MaxTopicAlias)
	assert.Equal(t, 0, cm.conf.MaxInflightMessages)
	assert.Equal(t, 23, cm.conf.MaxClientIDLen)
}

func TestConnectionManager_Handle(t *testing.T) {
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

	msg := []byte{0x10, 13, 0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 10, 0, 1, 'a'}
	_, err := conn.Write(msg)
	require.Nil(t, err)

	out := make([]byte, 4)
	_, err = conn.Read(out)
	require.Nil(t, err)

	resp := []byte{0x20, 2, 0, 0}
	assert.Equal(t, resp, out)

	<-time.After(10 * time.Millisecond)

	cm.mutex.RLock()
	assert.Equal(t, 1, len(cm.connections))
	c, ok := cm.connections["a"]
	cm.mutex.RUnlock()

	require.True(t, ok)
	require.NotNil(t, c)
	assert.Equal(t, ClientID{'a'}, c.session.ClientID)
	assert.Equal(t, packet.MQTT311, c.session.Version)
	assert.True(t, c.session.connected)

	msg = []byte{0xC0, 0}
	_, err = conn.Write(msg)
	require.Nil(t, err)

	out = make([]byte, 2)
	_, err = conn.Read(out)
	require.Nil(t, err)

	resp = []byte{0xD0, 0}
	assert.Equal(t, resp, out)

	msg = []byte{0xE0, 1, 0}
	_, err = conn.Write(msg)
	require.Nil(t, err)

	<-done
	cm.mutex.RLock()
	assert.Empty(t, cm.connections)
	cm.mutex.RUnlock()
	_ = conn.Close()
}

func TestConnectionManager_HandleNetConnClosed(t *testing.T) {
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

func TestConnectionManager_HandleSetDeadlineFailure(t *testing.T) {
	conf := newConfiguration()
	cm := createConnectionManager(conf)

	conn, sConn := net.Pipe()
	_ = conn.Close()

	err := cm.handle(sConn)
	assert.NotNil(t, err)
}

func TestConnectionManager_HandleReadFailure(t *testing.T) {
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

	_, err := conn.Write([]byte{0x15, 13})
	require.Nil(t, err)
	<-done
}

func TestConnectionManager_KeepAliveExceeded(t *testing.T) {
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

	msg := []byte{0x10, 13, 0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 1, 0, 1, 'a'}
	_, err := conn.Write(msg)
	require.Nil(t, err)

	out := make([]byte, 4)
	_, err = conn.Read(out)
	require.Nil(t, err)

	resp := []byte{0x20, 2, 0, 0}
	assert.Equal(t, resp, out)
	<-done
}

func TestConnectionManager_HandleWritePacketFailure(t *testing.T) {
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

	msg := []byte{0x10, 13, 0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 10, 0, 1, 'a'}
	_, err := conn.Write(msg)
	_ = conn.Close()
	require.Nil(t, err)
	<-done
}

func TestConnectionManager_HandleInvalidPacket(t *testing.T) {
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

	msg := []byte{0xC0, 0}
	_, err := conn.Write(msg)
	require.Nil(t, err)
	<-done
}

func TestConnectionManager_HandleDisconnect(t *testing.T) {
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

	connect := []byte{0x10, 13, 0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 10, 0, 1,
		'a'}
	_, err := conn.Write(connect)
	require.Nil(t, err)

	out := make([]byte, 4)
	_, err = conn.Read(out)
	require.Nil(t, err)

	ping := []byte{0xC0, 0}
	_, err = conn.Write(ping)
	require.Nil(t, err)

	_, err = conn.Read(out[:2])
	require.Nil(t, err)

	disconnect := []byte{0xE0, 1, 0}
	_, err = conn.Write(disconnect)
	require.Nil(t, err)
	<-done
}

func TestConnectionManager_DeliverMessage(t *testing.T) {
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

		err := cm.deliverPacket(ClientID("client-a"), &pkt)
		assert.Nil(t, err)
	}()

	out := make([]byte, 8)
	_, err := nc.Read(out)
	assert.Nil(t, err)

	pub := []byte{0x30, 6, 0, 4, 'd', 'a', 't', 'a'}
	assert.Equal(t, pub, out)
}

func TestConnectionManager_DeliverMessageConnectionNotFound(t *testing.T) {
	conf := newConfiguration()
	conf.MetricsEnabled = false
	cm := createConnectionManager(conf)
	nc, _ := net.Pipe()
	defer func() { _ = nc.Close() }()

	pkt := packet.NewPublish(10, packet.MQTT311, "data",
		packet.QoS0, 0, 0, nil, nil)

	err := cm.deliverPacket(ClientID("client-a"), &pkt)
	assert.NotNil(t, err)
}

func TestConnectionManager_DeliverMessageWriteFailure(t *testing.T) {
	conf := newConfiguration()
	conf.MetricsEnabled = false
	cm := createConnectionManager(conf)
	nc, sNc := net.Pipe()
	_ = nc.Close()

	conn := cm.createConnection(sNc)
	cm.connections["client-a"] = &conn

	pkt := packet.NewPublish(10, packet.MQTT311, "data",
		packet.QoS0, 0, 0, nil, nil)

	err := cm.deliverPacket(ClientID("client-a"), &pkt)
	assert.NotNil(t, err)
}
