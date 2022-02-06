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

func TestRunner_NewRunner(t *testing.T) {
	t.Run("MissingConfiguration", func(t *testing.T) {
		logStub := mocks.NewLoggerStub()
		mockConnHandler := mocks.ConnectionHandlerMock{}

		_, err := NewRunner(
			WithConnectionHandler(&mockConnHandler),
			WithLogger(logStub.Logger()),
		)

		require.NotNil(t, err)
		assert.Equal(t, "missing configuration", err.Error())
	})

	t.Run("MissingLogger", func(t *testing.T) {
		mockConnHandler := mocks.ConnectionHandlerMock{}

		_, err := NewRunner(
			WithConfiguration(Configuration{}),
			WithConnectionHandler(&mockConnHandler),
		)

		require.NotNil(t, err)
		assert.Equal(t, "missing logger", err.Error())
	})

	t.Run("MissingConnectionHandler", func(t *testing.T) {
		logStub := mocks.NewLoggerStub()

		_, err := NewRunner(
			WithConfiguration(Configuration{}),
			WithLogger(logStub.Logger()),
		)

		require.NotNil(t, err)
		assert.Equal(t, "missing connection handler", err.Error())
	})

	t.Run("InvalidTCPAddress", func(t *testing.T) {
		mockConnHandler := mocks.ConnectionHandlerMock{}
		logStub := mocks.NewLoggerStub()

		_, err := NewRunner(
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

func TestRunner_RunAndStop(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	mockConnHandler := mocks.ConnectionHandlerMock{}

	mqtt, err := NewRunner(
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

	<-time.After(5 * time.Millisecond)
	assert.Contains(t, logStub.String(), "Listening on [::]:1883")
	mqtt.Stop()

	<-done
	assert.Nil(t, err)
	assert.Contains(t, logStub.String(), "Runner stopped with success")
}

func TestRunner_Run_Accept(t *testing.T) {
	logStub := mocks.NewLoggerStub()

	handled := make(chan bool)
	mockConnHandler := mocks.ConnectionHandlerMock{}
	mockConnHandler.On("Handle", mock.Anything).
		Return(func() { handled <- true })

	mqtt, err := NewRunner(
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

func TestRunner_AcceptError(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	mockConnHandler := mocks.ConnectionHandlerMock{}

	mqtt, err := NewRunner(
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
	mqtt.tcpLsn.Close()
	mqtt.Stop()

	<-done
	assert.Nil(t, err)
	assert.Contains(t, logStub.String(), "Failed to accept TCP connection")
}
