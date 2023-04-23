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
	return logger.New(&out, nil, logger.Json)
}

func TestListenerNewListener(t *testing.T) {
	t.Run("MissingLogger", func(t *testing.T) {
		idGen := &idGeneratorMock{}

		_, err := NewListener(handler.Configuration{}, idGen, nil)
		assert.ErrorContains(t, err, "missing logger")
	})

	t.Run("MissingIDGenerator", func(t *testing.T) {
		log := newLogger()

		_, err := NewListener(handler.Configuration{}, nil, log)
		assert.ErrorContains(t, err, "missing ID generator")
	})
}

func TestListenerStartInvalidTCPAddress(t *testing.T) {
	log := newLogger()
	idGen := &idGeneratorMock{}

	l, err := NewListener(handler.Configuration{TCPAddress: "."}, idGen, log)
	require.Nil(t, err)
	require.NotNil(t, l)

	err = l.Start()
	require.NotNil(t, err)
}

func TestListenerStartAndStop(t *testing.T) {
	log := newLogger()
	idGen := &idGeneratorMock{}

	l, err := NewListener(handler.Configuration{TCPAddress: ":1883"}, idGen, log)
	require.Nil(t, err)
	require.NotNil(t, l)

	err = l.Start()
	require.Nil(t, err)

	l.Stop()
}

func TestListenerHandleConnection(t *testing.T) {
	log := newLogger()
	idGen := &idGeneratorMock{}
	idGen.On("NextID").Return(1)

	l, err := NewListener(handler.Configuration{TCPAddress: ":1883"}, idGen, log)
	require.Nil(t, err)

	err = l.Start()
	require.Nil(t, err)

	var conn net.Conn
	conn, err = net.Dial("tcp", ":1883")
	require.Nil(t, err)

	_ = conn.Close()
	l.Stop()
}
