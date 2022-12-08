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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type idGeneratorMock struct {
	mock.Mock
}

func (g *idGeneratorMock) NextID() uint64 {
	args := g.Called()
	return uint64(args.Int(0))
}

func TestListener_New(t *testing.T) {
	t.Run("MissingConfiguration", func(t *testing.T) {
		logger := mocks.NewLoggerStub()
		idGen := &idGeneratorMock{}

		_, err := NewListener(
			WithLogger(logger.Logger()),
			WithIDGenerator(idGen),
		)

		require.NotNil(t, err)
		assert.Equal(t, "missing configuration", err.Error())
	})

	t.Run("MissingLogger", func(t *testing.T) {
		idGen := &idGeneratorMock{}

		_, err := NewListener(
			WithConfiguration(Configuration{}),
			WithIDGenerator(idGen),
		)

		require.NotNil(t, err)
		assert.Equal(t, "missing logger", err.Error())
	})

	t.Run("MissingIDGenerator", func(t *testing.T) {
		logger := mocks.NewLoggerStub()

		_, err := NewListener(
			WithConfiguration(Configuration{}),
			WithLogger(logger.Logger()),
		)

		require.NotNil(t, err)
		assert.Equal(t, "missing ID generator", err.Error())
	})
}

func TestListener_RunInvalidTCPAddress(t *testing.T) {
	logger := mocks.NewLoggerStub()
	idGen := &idGeneratorMock{}

	l, err := NewListener(
		WithConfiguration(Configuration{TCPAddress: "."}),
		WithLogger(logger.Logger()),
		WithIDGenerator(idGen),
	)
	require.Nil(t, err)
	require.NotNil(t, l)

	err = l.Listen()
	require.NotNil(t, err)
}

func TestListener_RunAndStop(t *testing.T) {
	logger := mocks.NewLoggerStub()
	idGen := &idGeneratorMock{}

	l, err := NewListener(
		WithConfiguration(Configuration{TCPAddress: ":1883"}),
		WithLogger(logger.Logger()),
		WithIDGenerator(idGen),
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
	idGen := &idGeneratorMock{}
	idGen.On("NextID").Return(1)

	l, err := NewListener(
		WithConfiguration(Configuration{TCPAddress: ":1883"}),
		WithLogger(logger.Logger()),
		WithIDGenerator(idGen),
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
	idGen := &idGeneratorMock{}
	idGen.On("NextID").Return(1)

	l, err := NewListener(
		WithConfiguration(Configuration{TCPAddress: ":1883"}),
		WithLogger(logger.Logger()),
		WithIDGenerator(idGen),
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