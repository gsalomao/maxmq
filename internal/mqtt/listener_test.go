// Copyright 2022-2023 The MaxMQ Authors
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
	"bytes"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/mqtt/handler"
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

type logBuffer struct {
	b bytes.Buffer
	m sync.Mutex
}

func (b *logBuffer) Write(p []byte) (n int, err error) {
	b.m.Lock()
	defer b.m.Unlock()
	return b.b.Write(p)
}

func newLogger() *logger.Logger {
	out := logBuffer{}
	idGen := &idGeneratorMock{}
	idGen.On("NextID").Return(0)

	return logger.New(&out, idGen, logger.LogFormatJson)
}

func TestListenerNewListener(t *testing.T) {
	log := newLogger()

	t.Run("MissingConfiguration", func(t *testing.T) {
		idGen := &idGeneratorMock{}

		_, err := NewListener(
			WithLogger(log),
			WithIDGenerator(idGen),
		)

		assert.ErrorContains(t, err, "missing configuration")
	})

	t.Run("MissingLogger", func(t *testing.T) {
		idGen := &idGeneratorMock{}

		_, err := NewListener(
			WithConfiguration(handler.Configuration{}),
			WithIDGenerator(idGen),
		)

		assert.ErrorContains(t, err, "missing logger")
	})

	t.Run("MissingIDGenerator", func(t *testing.T) {
		_, err := NewListener(
			WithConfiguration(handler.Configuration{}),
			WithLogger(log),
		)

		assert.ErrorContains(t, err, "missing ID generator")
	})
}

func TestListenerRunInvalidTCPAddress(t *testing.T) {
	log := newLogger()
	idGen := &idGeneratorMock{}

	l, err := NewListener(
		WithConfiguration(handler.Configuration{TCPAddress: "."}),
		WithLogger(log),
		WithIDGenerator(idGen),
	)
	require.Nil(t, err)
	require.NotNil(t, l)

	err = l.Listen()
	require.NotNil(t, err)
}

func TestListenerRunAndStop(t *testing.T) {
	log := newLogger()
	idGen := &idGeneratorMock{}

	l, err := NewListener(
		WithConfiguration(handler.Configuration{TCPAddress: ":1883"}),
		WithLogger(log),
		WithIDGenerator(idGen),
	)
	require.Nil(t, err)

	done := make(chan bool)
	go func() {
		err = l.Listen()
		done <- true
	}()

	<-time.After(10 * time.Millisecond)
	l.Stop()

	<-done
	assert.Nil(t, err)
}

func TestListenerHandleConnection(t *testing.T) {
	log := newLogger()
	idGen := &idGeneratorMock{}
	idGen.On("NextID").Return(1)

	l, err := NewListener(
		WithConfiguration(handler.Configuration{TCPAddress: ":1883"}),
		WithLogger(log),
		WithIDGenerator(idGen),
	)
	require.Nil(t, err)

	done := make(chan bool)
	go func() {
		err = l.Listen()
		done <- true
	}()

	<-time.After(time.Millisecond)
	conn, err := net.Dial("tcp" /*network*/, ":1883" /*address*/)
	require.Nil(t, err)
	defer func() { _ = conn.Close() }()

	l.Stop()
	<-done
	assert.Nil(t, err)
}

func TestListenerHandleConnectionFailure(t *testing.T) {
	log := newLogger()
	idGen := &idGeneratorMock{}
	idGen.On("NextID").Return(1)

	l, err := NewListener(
		WithConfiguration(handler.Configuration{TCPAddress: ":1883"}),
		WithLogger(log),
		WithIDGenerator(idGen),
	)
	require.Nil(t, err)

	done := make(chan bool)
	go func() {
		err = l.Listen()
		done <- true
	}()

	<-time.After(time.Millisecond)
	conn, err := net.Dial("tcp" /*network*/, ":1883" /*address*/)
	require.Nil(t, err)
	defer func() { _ = conn.Close() }()

	l.Stop()
	<-done
	assert.Nil(t, err)
}
