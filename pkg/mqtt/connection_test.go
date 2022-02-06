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
	}, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		cm.Handle(sConn)
		done <- true
	}()

	msg := []byte{
		0x10, 13, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 0, // variable header
		0, 1, 'a', // client ID
	}

	_, err := conn.Write(msg)
	require.Nil(t, err)

	conn.Close()
	<-done
}

func TestConnectionManager_HandleClosed(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	cm := mqtt.NewConnectionManager(mqtt.Configuration{
		ConnectTimeout: 1,
		BufferSize:     1024,
	}, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		cm.Handle(sConn)
		done <- true
	}()

	<-time.After(time.Millisecond)
	conn.Close()

	<-done
	assert.Contains(t, logStub.String(), "Connection was closed")
}

func TestConnectionManager_HandleFailedToSetDeadline(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	cm := mqtt.NewConnectionManager(mqtt.Configuration{
		ConnectTimeout: 1,
		BufferSize:     1024,
	}, logStub.Logger())

	conn, sConn := net.Pipe()
	conn.Close()

	cm.Handle(sConn)
	assert.Contains(t, logStub.String(), "Failed to set read deadline")
}

func TestConnectionManager_HandleFailedToRead(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	cm := mqtt.NewConnectionManager(mqtt.Configuration{
		ConnectTimeout: 1,
		BufferSize:     1024,
	}, logStub.Logger())

	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		cm.Handle(sConn)
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
		ConnectTimeout: 0,
	}, logStub.Logger())

	_, sConn := net.Pipe()
	cm.Handle(sConn)

	assert.Contains(t, logStub.String(), "No CONNECT Packet received")
}

func TestConnectionManager_Close(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	cm := mqtt.NewConnectionManager(mqtt.Configuration{}, logStub.Logger())

	_, sConn := net.Pipe()

	cm.Close(sConn)
	assert.Contains(t, logStub.String(), "Closed connection")
}
