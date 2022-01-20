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

package mqtt

import (
	"net"
	"testing"
	"time"

	"github.com/gsalomao/maxmq/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestListener_NewListener(t *testing.T) {
	t.Run("MissingConfiguration", func(t *testing.T) {
		logStub := mocks.NewLoggerStub()
		mockConnHandler := mocks.ConnectionHandlerMock{}

		_, err := NewListener(
			WithConnectionHandler(&mockConnHandler),
			WithLogger(logStub.Logger()),
		)

		require.NotNil(t, err)
		assert.Equal(t, "missing configuration", err.Error())
	})

	t.Run("MissingLogger", func(t *testing.T) {
		mockConnHandler := mocks.ConnectionHandlerMock{}

		_, err := NewListener(
			WithConfiguration(Configuration{}),
			WithConnectionHandler(&mockConnHandler),
		)

		require.NotNil(t, err)
		assert.Equal(t, "missing logger", err.Error())
	})

	t.Run("MissingConnectionHandler", func(t *testing.T) {
		logStub := mocks.NewLoggerStub()

		_, err := NewListener(
			WithConfiguration(Configuration{}),
			WithLogger(logStub.Logger()),
		)

		require.NotNil(t, err)
		assert.Equal(t, "missing connection handler", err.Error())
	})

	t.Run("InvalidTCPAddress", func(t *testing.T) {
		mockConnHandler := mocks.ConnectionHandlerMock{}
		logStub := mocks.NewLoggerStub()

		_, err := NewListener(
			WithConfiguration(Configuration{
				TCPAddress: ":1",
			}),
			WithConnectionHandler(&mockConnHandler),
			WithLogger(logStub.Logger()),
		)

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "bind: permission denied")
	})
}

func TestListener_RunAndStop(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	mockConnHandler := mocks.ConnectionHandlerMock{}

	mqtt, err := NewListener(
		WithConfiguration(Configuration{
			TCPAddress: ":1883",
		}),
		WithConnectionHandler(&mockConnHandler),
		WithLogger(logStub.Logger()),
	)
	require.Nil(t, err)

	done := make(chan bool)
	go func() {
		err = mqtt.Run()
		done <- true
	}()

	<-time.After(time.Millisecond)
	assert.Contains(t, logStub.String(), "MQTT listening on [::]:1883")
	mqtt.Stop()

	<-done
	assert.Nil(t, err)
	assert.Contains(t, logStub.String(), "MQTT listener stopped with success")
}

func TestListener_Run_Accept(t *testing.T) {
	logStub := mocks.NewLoggerStub()

	handled := make(chan bool)
	mockConnHandler := mocks.ConnectionHandlerMock{}
	mockConnHandler.On("Handle", mock.Anything).
		Return(func() { handled <- true })

	mqtt, err := NewListener(
		WithConfiguration(Configuration{
			TCPAddress: ":1883",
		}),
		WithConnectionHandler(&mockConnHandler),
		WithLogger(logStub.Logger()),
	)
	require.Nil(t, err)

	done := make(chan bool)
	go func() {
		err = mqtt.Run()
		done <- true
	}()

	<-time.After(time.Millisecond)
	conn, err := net.Dial("tcp", ":1883")
	require.Nil(t, err)
	defer conn.Close()

	<-handled
	mqtt.Stop()
	<-done
	assert.Nil(t, err)
}

func TestListener_AcceptError(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	mockConnHandler := mocks.ConnectionHandlerMock{}

	mqtt, err := NewListener(
		WithConfiguration(Configuration{
			TCPAddress: ":1883",
		}),
		WithConnectionHandler(&mockConnHandler),
		WithLogger(logStub.Logger()),
	)
	require.Nil(t, err)

	done := make(chan bool)
	go func() {
		err = mqtt.Run()
		done <- true
	}()

	<-time.After(time.Millisecond)
	mqtt.tcp.Close()
	mqtt.Stop()

	<-done
	assert.Nil(t, err)
	assert.Contains(t, logStub.String(), "MQTT failed to accept TCP connection")
}
