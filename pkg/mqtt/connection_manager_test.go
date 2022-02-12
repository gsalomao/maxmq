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
	"net"
	"testing"
	"time"

	"github.com/gsalomao/maxmq/mocks"
	"github.com/gsalomao/maxmq/pkg/mqtt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectionManager_HandleConnectPacket(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	cm := mqtt.NewConnectionManager(mqtt.Configuration{
		ConnectTimeout: 1,
		BufferSize:     1024,
		MaxPacketSize:  65536,
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

func TestConnectionManager_HandleClosed(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	cm := mqtt.NewConnectionManager(mqtt.Configuration{
		ConnectTimeout: 1,
		BufferSize:     1024,
		MaxPacketSize:  65536,
	}, logStub.Logger())

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

func TestConnectionManager_HandleFailedToSetDeadline(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	cm := mqtt.NewConnectionManager(mqtt.Configuration{
		ConnectTimeout: 1,
		BufferSize:     1024,
		MaxPacketSize:  65536,
	}, logStub.Logger())

	conn, sConn := net.Pipe()
	_ = conn.Close()

	c := cm.NewConnection(sConn)
	cm.Handle(c)
	assert.Contains(t, logStub.String(), "Failed to set read deadline")
}

func TestConnectionManager_HandleFailedToRead(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	cm := mqtt.NewConnectionManager(mqtt.Configuration{
		ConnectTimeout: 1,
		BufferSize:     1024,
		MaxPacketSize:  65536,
	}, logStub.Logger())

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

func TestConnectionManager_HandleReadTimeout(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	cm := mqtt.NewConnectionManager(mqtt.Configuration{
		ConnectTimeout: 1,
		BufferSize:     1024,
		MaxPacketSize:  65536,
	}, logStub.Logger())

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
	cm := mqtt.NewConnectionManager(mqtt.Configuration{}, logStub.Logger())

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
