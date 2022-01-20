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
	"errors"
	"net"
	"testing"
	"time"

	"github.com/gsalomao/maxmq/mocks"
	"github.com/gsalomao/maxmq/pkg/mqtt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestConnectionManager_HandleFailedToSetDeadline(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	cm := mqtt.NewConnectionManager(mqtt.Configuration{
		ConnectTimeout: 5,
	}, logStub.Logger())

	mockConn := mocks.NetConnMock{}
	mockConn.On("RemoteAddr").Return(&net.IPAddr{IP: net.IPv4zero})
	mockConn.On("SetReadDeadline", mock.Anything).Return(errors.New("error"))
	mockConn.On("Close", mock.Anything).Return(nil)

	cm.Handle(&mockConn)
	<-time.After(2 * time.Millisecond)

	assert.Contains(t, logStub.String(), "Failed to set read deadline: error")
}

func TestConnectionManager_HandleFailedToRead(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	cm := mqtt.NewConnectionManager(mqtt.Configuration{
		ConnectTimeout: 5,
	}, logStub.Logger())

	mockConn := mocks.NetConnMock{}
	mockConn.On("RemoteAddr").Return(&net.IPAddr{IP: net.IPv4zero})
	mockConn.On("SetReadDeadline", mock.Anything).Return(nil)
	mockConn.On("Read", mock.Anything).Return(0, errors.New("error"))
	mockConn.On("Close", mock.Anything).Return(nil)

	cm.Handle(&mockConn)
	<-time.After(2 * time.Millisecond)

	assert.Contains(t, logStub.String(), "Failed to read data: error")
}

func TestConnectionManager_HandleReadTimeout(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	cm := mqtt.NewConnectionManager(mqtt.Configuration{
		ConnectTimeout: 0,
	}, logStub.Logger())

	mockConn := mocks.NetConnMock{}
	mockConn.On("RemoteAddr").Return(&net.IPAddr{IP: net.IPv4zero})
	mockConn.On("SetReadDeadline", mock.Anything).Return(nil)
	mockConn.On("Read", mock.Anything).Return(0, errors.New("timeout"))
	mockConn.On("Close", mock.Anything).Return(nil)

	cm.Handle(&mockConn)
	<-time.After(2 * time.Millisecond)

	assert.Contains(t, logStub.String(), "Timeout - No CONNECT Packet received")
}

func TestConnectionManager_Close(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	cm := mqtt.NewConnectionManager(mqtt.Configuration{}, logStub.Logger())

	mockConn := mocks.NetConnMock{}
	mockConn.On("RemoteAddr").Return(&net.IPAddr{IP: net.IPv4zero})
	mockConn.On("Close", mock.Anything).Return(nil)

	cm.Close(&mockConn)

	assert.Contains(t, logStub.String(), " was closed")
}
