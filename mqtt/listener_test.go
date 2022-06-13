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
	"errors"
	"net"
	"testing"
	"time"

	"github.com/gsalomao/maxmq/mocks"
	"github.com/gsalomao/maxmq/mqtt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type connectionHandlerMock struct {
	mock.Mock
}

func (m *connectionHandlerMock) Handle(nc net.Conn) error {
	ret := m.Called(nc)
	return ret.Error(0)
}

func TestListener_New(t *testing.T) {
	t.Run("MissingConfiguration", func(t *testing.T) {
		logStub := mocks.NewLoggerStub()
		mockConnHandler := connectionHandlerMock{}

		_, err := mqtt.NewListener(
			mqtt.WithConnectionHandler(&mockConnHandler),
			mqtt.WithLogger(logStub.Logger()),
		)

		require.NotNil(t, err)
		assert.Equal(t, "missing configuration", err.Error())
	})

	t.Run("MissingLogger", func(t *testing.T) {
		mockConnHandler := connectionHandlerMock{}

		_, err := mqtt.NewListener(
			mqtt.WithConfiguration(mqtt.Configuration{}),
			mqtt.WithConnectionHandler(&mockConnHandler),
		)

		require.NotNil(t, err)
		assert.Equal(t, "missing logger", err.Error())
	})

	t.Run("MissingConnectionHandler", func(t *testing.T) {
		logStub := mocks.NewLoggerStub()

		_, err := mqtt.NewListener(
			mqtt.WithConfiguration(mqtt.Configuration{}),
			mqtt.WithLogger(logStub.Logger()),
		)

		require.NotNil(t, err)
		assert.Equal(t, "missing connection handler", err.Error())
	})
}

func TestListener_RunInvalidTCPAddress(t *testing.T) {
	mockConnHandler := connectionHandlerMock{}
	logStub := mocks.NewLoggerStub()

	l, err := mqtt.NewListener(
		mqtt.WithConfiguration(mqtt.Configuration{TCPAddress: "."}),
		mqtt.WithConnectionHandler(&mockConnHandler),
		mqtt.WithLogger(logStub.Logger()),
	)
	require.Nil(t, err)
	require.NotNil(t, l)

	err = l.Listen()
	require.NotNil(t, err)
}

func TestListener_RunAndStop(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	mockConnHandler := connectionHandlerMock{}

	l, err := mqtt.NewListener(
		mqtt.WithConfiguration(mqtt.Configuration{TCPAddress: ":1883"}),
		mqtt.WithConnectionHandler(&mockConnHandler),
		mqtt.WithLogger(logStub.Logger()),
	)
	require.Nil(t, err)

	done := make(chan bool)
	go func() {
		err = l.Listen()
		done <- true
	}()

	<-time.After(10 * time.Millisecond)
	assert.Contains(t, logStub.String(), "Listening on [::]:1883")
	l.Stop()

	<-done
	assert.Nil(t, err)
	assert.Contains(t, logStub.String(), "Listener stopped with success")
}

func TestListener_HandleConnection(t *testing.T) {
	logStub := mocks.NewLoggerStub()

	handled := make(chan bool)
	mockConnHandler := connectionHandlerMock{}
	mockConnHandler.On("Handle", mock.Anything).
		Run(func(args mock.Arguments) { handled <- true }).
		Return(nil)

	l, err := mqtt.NewListener(
		mqtt.WithConfiguration(mqtt.Configuration{TCPAddress: ":1883"}),
		mqtt.WithConnectionHandler(&mockConnHandler),
		mqtt.WithLogger(logStub.Logger()),
	)
	require.Nil(t, err)

	done := make(chan bool)
	go func() {
		err = l.Listen()
		done <- true
	}()

	<-time.After(time.Millisecond)
	conn, err := net.Dial("tcp", ":1883")
	require.Nil(t, err)
	defer func() { _ = conn.Close() }()

	<-handled
	l.Stop()
	<-done
	assert.Nil(t, err)
}

func TestListener_HandleConnectionFailure(t *testing.T) {
	logStub := mocks.NewLoggerStub()

	handled := make(chan bool)
	mockConnHandler := connectionHandlerMock{}
	mockConnHandler.On("Handle", mock.Anything).
		Run(func(args mock.Arguments) { handled <- true }).
		Return(errors.New("failed to handle connection"))

	l, err := mqtt.NewListener(
		mqtt.WithConfiguration(mqtt.Configuration{TCPAddress: ":1883"}),
		mqtt.WithConnectionHandler(&mockConnHandler),
		mqtt.WithLogger(logStub.Logger()),
	)
	require.Nil(t, err)

	done := make(chan bool)
	go func() {
		err = l.Listen()
		done <- true
	}()

	<-time.After(time.Millisecond)
	conn, err := net.Dial("tcp", ":1883")
	require.Nil(t, err)
	defer func() { _ = conn.Close() }()

	<-handled
	l.Stop()
	<-done
	assert.Nil(t, err)
}
