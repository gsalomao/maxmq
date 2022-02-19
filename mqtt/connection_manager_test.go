/*
 * Copyright 2022 The MaxMQ Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mqtt_test

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/gsalomao/maxmq/mocks"
	"github.com/gsalomao/maxmq/mqtt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
		AllowEmptyClientID:            true,
		UserProperties:                map[string]string{},
	}
}

func TestConnectionManager_ConnectV3(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	cm := mqtt.NewConnectionManager(mqtt.Configuration{}, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		c := cm.NewConnection(sConn)
		cm.Handle(c)
		done <- true
	}()

	msg := []byte{
		0x10, 13, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 0, // variable header
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

func TestConnectionManager_ConnectV311AllowEmptyClientID(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	cm := mqtt.NewConnectionManager(mqtt.Configuration{
		AllowEmptyClientID: true,
	}, logStub.Logger())

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
	cm := mqtt.NewConnectionManager(mqtt.Configuration{
		AllowEmptyClientID: false,
	}, logStub.Logger())

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
	cm := mqtt.NewConnectionManager(mqtt.Configuration{
		MaximumQoS:                    3,     // invalid: will be changed to 2
		MaxTopicAlias:                 65536, // invalid: will be changed to 0
		MaxInflightMessages:           65536, // invalid: will be changed to 0
		RetainAvailable:               true,
		WildcardSubscriptionAvailable: true,
		SubscriptionIDAvailable:       true,
		SharedSubscriptionAvailable:   true,
	}, logStub.Logger())

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

func TestConnectionManager_ConnectV5DenyEmptyClientID(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	cm := mqtt.NewConnectionManager(mqtt.Configuration{
		MaximumQoS:                    3,     // invalid: will be changed to 2
		MaxTopicAlias:                 65536, // invalid: will be changed to 0
		MaxInflightMessages:           65536, // invalid: will be changed to 0
		RetainAvailable:               true,
		WildcardSubscriptionAvailable: true,
		SubscriptionIDAvailable:       true,
		SharedSubscriptionAvailable:   true,
		AllowEmptyClientID:            false,
	}, logStub.Logger())

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

				conf := newConfiguration()
				conf.MaxKeepAlive = test.maxKeepAlive
				cm := mqtt.NewConnectionManager(conf, logStub.Logger())

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
	cm := mqtt.NewConnectionManager(conf, logStub.Logger())

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
	cm := mqtt.NewConnectionManager(conf, logStub.Logger())

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
	cm := mqtt.NewConnectionManager(conf, logStub.Logger())

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
	cm := mqtt.NewConnectionManager(conf, logStub.Logger())

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
	cm := mqtt.NewConnectionManager(conf, logStub.Logger())

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
	cm := mqtt.NewConnectionManager(conf, logStub.Logger())

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
	cm := mqtt.NewConnectionManager(conf, logStub.Logger())

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
	cm := mqtt.NewConnectionManager(conf, logStub.Logger())

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
	cm := mqtt.NewConnectionManager(conf, logStub.Logger())

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
	cm := mqtt.NewConnectionManager(conf, logStub.Logger())

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
	cm := mqtt.NewConnectionManager(conf, logStub.Logger())

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
	cm := mqtt.NewConnectionManager(conf, logStub.Logger())

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
	cm := mqtt.NewConnectionManager(conf, logStub.Logger())

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

func TestConnectionManager_NetConnClosed(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	cm := mqtt.NewConnectionManager(newConfiguration(), logStub.Logger())

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
	cm := mqtt.NewConnectionManager(newConfiguration(), logStub.Logger())

	conn, sConn := net.Pipe()
	_ = conn.Close()

	c := cm.NewConnection(sConn)
	cm.Handle(c)
	assert.Contains(t, logStub.String(), "Failed to set read deadline")
}

func TestConnectionManager_ReadFailure(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	cm := mqtt.NewConnectionManager(newConfiguration(), logStub.Logger())

	conn, sConn := net.Pipe()

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
	cm := mqtt.NewConnectionManager(newConfiguration(), logStub.Logger())

	conn, sConn := net.Pipe()
	defer func(conn net.Conn) {
		_ = conn.Close()
	}(conn)

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
	cm := mqtt.NewConnectionManager(newConfiguration(), logStub.Logger())

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
		cm.Close(conn)
	}()

	conn, err := net.Dial("tcp", lsn.Addr().String())
	require.Nil(t, err)
	defer func() {
		_ = conn.Close()
	}()

	<-done
	assert.Contains(t, logStub.String(), "Closing connection")
}
