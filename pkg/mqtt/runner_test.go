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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestRunner_NewRunner(t *testing.T) {
	t.Run("MissingConfiguration", func(t *testing.T) {
		logStub := mocks.NewLoggerStub()
		mockConnHandler := mocks.ConnectionHandlerMock{}

		_, err := mqtt.NewRunner(
			mqtt.WithConnectionHandler(&mockConnHandler),
			mqtt.WithLogger(logStub.Logger()),
		)

		require.NotNil(t, err)
		assert.Equal(t, "missing configuration", err.Error())
	})

	t.Run("MissingLogger", func(t *testing.T) {
		mockConnHandler := mocks.ConnectionHandlerMock{}

		_, err := mqtt.NewRunner(
			mqtt.WithConfiguration(mqtt.Configuration{}),
			mqtt.WithConnectionHandler(&mockConnHandler),
		)

		require.NotNil(t, err)
		assert.Equal(t, "missing logger", err.Error())
	})

	t.Run("MissingConnectionHandler", func(t *testing.T) {
		logStub := mocks.NewLoggerStub()

		_, err := mqtt.NewRunner(
			mqtt.WithConfiguration(mqtt.Configuration{}),
			mqtt.WithLogger(logStub.Logger()),
		)

		require.NotNil(t, err)
		assert.Equal(t, "missing connection handler", err.Error())
	})

	t.Run("InvalidTCPAddress", func(t *testing.T) {
		mockConnHandler := mocks.ConnectionHandlerMock{}
		logStub := mocks.NewLoggerStub()

		_, err := mqtt.NewRunner(
			mqtt.WithConfiguration(mqtt.Configuration{
				TCPAddress: ":1",
			}),
			mqtt.WithConnectionHandler(&mockConnHandler),
			mqtt.WithLogger(logStub.Logger()),
		)

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "bind: permission denied")
	})
}

func TestRunner_RunAndStop(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	mockConnHandler := mocks.ConnectionHandlerMock{}

	r, err := mqtt.NewRunner(
		mqtt.WithConfiguration(mqtt.Configuration{
			TCPAddress: ":1883",
		}),
		mqtt.WithConnectionHandler(&mockConnHandler),
		mqtt.WithLogger(logStub.Logger()),
	)
	require.Nil(t, err)

	done := make(chan bool)
	go func() {
		err = r.Run()
		done <- true
	}()

	<-time.After(5 * time.Millisecond)
	assert.Contains(t, logStub.String(), "Listening on [::]:1883")
	r.Stop()

	<-done
	assert.Nil(t, err)
	assert.Contains(t, logStub.String(), "Runner stopped with success")
}

func TestRunner_Run_Accept(t *testing.T) {
	logStub := mocks.NewLoggerStub()

	handled := make(chan bool)
	mockConnHandler := mocks.ConnectionHandlerMock{}
	mockConnHandler.On("NewConnection", mock.Anything)
	mockConnHandler.On("Handle", mock.Anything).
		Return(func() { handled <- true })

	r, err := mqtt.NewRunner(
		mqtt.WithConfiguration(mqtt.Configuration{
			TCPAddress: ":1883",
		}),
		mqtt.WithConnectionHandler(&mockConnHandler),
		mqtt.WithLogger(logStub.Logger()),
	)
	require.Nil(t, err)

	done := make(chan bool)
	go func() {
		err = r.Run()
		done <- true
	}()

	<-time.After(time.Millisecond)
	conn, err := net.Dial("tcp", ":1883")
	require.Nil(t, err)
	defer func() {
		_ = conn.Close()
	}()

	<-handled
	r.Stop()
	<-done
	assert.Nil(t, err)
}
