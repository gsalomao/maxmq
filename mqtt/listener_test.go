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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListener_New(t *testing.T) {
	t.Run("MissingConfiguration", func(t *testing.T) {
		logger := mocks.NewLoggerStub()
		store := sessionStoreMock{}

		_, err := NewListener(
			WithStore(&store),
			WithLogger(logger.Logger()),
		)

		require.NotNil(t, err)
		assert.Equal(t, "missing configuration", err.Error())
	})

	t.Run("MissingLogger", func(t *testing.T) {
		store := sessionStoreMock{}

		_, err := NewListener(
			WithConfiguration(Configuration{}),
			WithStore(&store),
		)

		require.NotNil(t, err)
		assert.Equal(t, "missing logger", err.Error())
	})

	t.Run("MissingSessionStore", func(t *testing.T) {
		logger := mocks.NewLoggerStub()

		_, err := NewListener(
			WithConfiguration(Configuration{}),
			WithLogger(logger.Logger()),
		)

		require.NotNil(t, err)
		assert.Equal(t, "missing session store", err.Error())
	})
}

func TestListener_RunInvalidTCPAddress(t *testing.T) {
	store := sessionStoreMock{}
	logger := mocks.NewLoggerStub()

	l, err := NewListener(
		WithConfiguration(Configuration{TCPAddress: "."}),
		WithStore(&store),
		WithLogger(logger.Logger()),
	)
	require.Nil(t, err)
	require.NotNil(t, l)

	err = l.Listen()
	require.NotNil(t, err)
}

func TestListener_RunAndStop(t *testing.T) {
	store := sessionStoreMock{}
	logger := mocks.NewLoggerStub()

	l, err := NewListener(
		WithConfiguration(Configuration{TCPAddress: ":1883"}),
		WithStore(&store),
		WithLogger(logger.Logger()),
	)
	require.Nil(t, err)

	done := make(chan bool)
	go func() {
		err = l.Listen()
		done <- true
	}()

	<-time.After(10 * time.Millisecond)
	assert.Contains(t, logger.String(), "Listening on [::]:1883")
	l.Stop()

	<-done
	assert.Nil(t, err)
	assert.Contains(t, logger.String(), "Listener stopped with success")
}

func TestListener_HandleConnection(t *testing.T) {
	logger := mocks.NewLoggerStub()
	store := sessionStoreMock{}

	l, err := NewListener(
		WithConfiguration(Configuration{TCPAddress: ":1883"}),
		WithStore(&store),
		WithLogger(logger.Logger()),
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

	l.Stop()
	<-done
	assert.Nil(t, err)
}

func TestListener_HandleConnectionFailure(t *testing.T) {
	logger := mocks.NewLoggerStub()
	store := sessionStoreMock{}

	l, err := NewListener(
		WithConfiguration(Configuration{TCPAddress: ":1883"}),
		WithStore(&store),
		WithLogger(logger.Logger()),
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

	l.Stop()
	<-done
	assert.Nil(t, err)
}
