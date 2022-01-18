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

	"github.com/gsalomao/maxmq/mocks"
	"github.com/gsalomao/maxmq/pkg/mqtt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestListener_NewListener(t *testing.T) {
	t.Run("MissingLogger", func(t *testing.T) {
		mockListener := mocks.NetListenerMock{}
		mockConnHandler := mocks.ConnectionHandlerMock{}

		_, err := mqtt.NewListener(
			mqtt.WithTCPListener(&mockListener),
			mqtt.WithConnectionHandler(&mockConnHandler),
		)

		assert.NotNil(t, err)
		assert.Equal(t, "missing logger", err.Error())
	})

	t.Run("MissingTCPListener", func(t *testing.T) {
		logStub := mocks.NewLoggerStub()
		mockConnHandler := mocks.ConnectionHandlerMock{}

		_, err := mqtt.NewListener(
			mqtt.WithConnectionHandler(&mockConnHandler),
			mqtt.WithLogger(logStub.Logger()),
		)

		assert.NotNil(t, err)
		assert.Equal(t, "missing TCP listener", err.Error())
	})

	t.Run("MissingConnectionHandler", func(t *testing.T) {
		logStub := mocks.NewLoggerStub()
		mockListener := mocks.NetListenerMock{}

		_, err := mqtt.NewListener(
			mqtt.WithTCPListener(&mockListener),
			mqtt.WithLogger(logStub.Logger()),
		)

		assert.NotNil(t, err)
		assert.Equal(t, "missing connection handler", err.Error())
	})
}

func TestListener_RunAndStop(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	mockConnHandler := mocks.ConnectionHandlerMock{}

	accepting := make(chan bool)
	stopped := make(chan bool)

	mockListener := mocks.NetListenerMock{}
	mockListener.On("Addr").Return(&net.TCPAddr{})
	mockListener.On("Accept").Return(func() (net.Conn, error) {
		accepting <- true
		<-stopped
		return &net.TCPConn{}, errors.New("interface closed")
	})
	mockListener.On("Close").Return(func() error {
		stopped <- true
		return nil
	})

	mqtt, err := mqtt.NewListener(
		mqtt.WithTCPListener(&mockListener),
		mqtt.WithConnectionHandler(&mockConnHandler),
		mqtt.WithLogger(logStub.Logger()),
	)
	require.Nil(t, err)

	done := make(chan bool)
	go func() {
		err = mqtt.Run()
		done <- true
	}()

	<-accepting
	assert.Contains(t, logStub.String(), "MQTT listening on")
	mqtt.Stop()

	<-done
	assert.Nil(t, err)
	assert.Contains(t, logStub.String(), "MQTT listener stopped with success")
}

func TestListener_Run_AcceptError(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	mockConnHandler := mocks.ConnectionHandlerMock{}

	accepted := make(chan bool, 1)
	acceptCnt := 0

	mockListener := mocks.NetListenerMock{}
	mockListener.On("Addr").Return(&net.TCPAddr{})
	mockListener.On("Accept").Return(func() (net.Conn, error) {
		accepted <- true
		acceptCnt++
		return &net.TCPConn{}, errors.New("failed to open connection")
	})
	mockListener.On("Close").Return(func() error {
		<-accepted
		return nil
	})

	mqtt, err := mqtt.NewListener(
		mqtt.WithTCPListener(&mockListener),
		mqtt.WithConnectionHandler(&mockConnHandler),
		mqtt.WithLogger(logStub.Logger()),
	)

	done := make(chan bool)
	go func() {
		err = mqtt.Run()
		done <- true
	}()

	<-accepted
	mqtt.Stop()

	<-done
	assert.Nil(t, err)
	assert.Contains(t, logStub.String(), "MQTT failed to accept TCP connection")
	assert.Greater(t, acceptCnt, 1)
}

func TestListener_Run_AcceptSuccess(t *testing.T) {
	logStub := mocks.NewLoggerStub()

	accepted := make(chan bool)
	stopped := make(chan bool)

	mockConn := mocks.NetConnMock{}
	mockConn.On("RemoteAddr").Return(&net.IPAddr{IP: net.IPv4zero})

	handled := make(chan bool)
	mockConnHandler := mocks.ConnectionHandlerMock{}
	mockConnHandler.On("Handle", mock.Anything).
		Return(func() { handled <- true })

	mockListener := mocks.NetListenerMock{}
	mockListener.On("Addr").Return(&net.TCPAddr{})
	mockListener.On("Accept").Return(func() (net.Conn, error) {
		accepted <- true
		return &mockConn, nil
	}).Once()
	mockListener.On("Accept").Return(func() (net.Conn, error) {
		<-stopped
		return &net.TCPConn{}, errors.New("interface closed")
	})
	mockListener.On("Close").Return(func() error {
		stopped <- true
		return nil
	})

	mqtt, err := mqtt.NewListener(
		mqtt.WithTCPListener(&mockListener),
		mqtt.WithConnectionHandler(&mockConnHandler),
		mqtt.WithLogger(logStub.Logger()),
	)

	done := make(chan bool)
	go func() {
		err = mqtt.Run()
		done <- true
	}()

	<-accepted
	<-handled
	mqtt.Stop()
	<-done
	assert.Nil(t, err)
}
