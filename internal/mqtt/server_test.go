// Copyright 2023 The MaxMQ Authors
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
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type idGeneratorMock struct {
	mock.Mock
}

func newIDGeneratorMock(ids ...interface{}) *idGeneratorMock {
	gen := &idGeneratorMock{}
	for _, id := range ids {
		gen.On("NextID").Return(id).Once()
	}
	return gen
}

func (g *idGeneratorMock) NextID() uint64 {
	args := g.Called()
	if len(args) > 0 {
		return uint64(args.Int(0))
	}
	return 0
}

type listenerNewMock struct {
	mock.Mock
	stream chan net.Conn
}

func (l *listenerNewMock) Name() string {
	return "mock"
}

func (l *listenerNewMock) Listen() (c <-chan net.Conn, err error) {
	args := l.Called()
	if len(args) > 0 && args.Get(0) != nil {
		l.stream = args.Get(0).(chan net.Conn)
		c = l.stream
	}
	if len(args) > 1 {
		err = args.Error(1)
	}
	return c, err
}

func (l *listenerNewMock) Close() error {
	args := l.Called()
	close(l.stream)
	if len(args) > 0 {
		return args.Error(0)
	}
	return nil
}

type logBuffer struct {
	buffer bytes.Buffer
	mutex  sync.Mutex
}

func (b *logBuffer) Write(p []byte) (n int, err error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.Write(p)
}

func newLogger() *logger.Logger {
	out := &logBuffer{}
	return logger.New(out, nil, logger.Json)
}

func newServer() *Server {
	log := newLogger()
	gen := newIDGeneratorMock()

	return NewServer(newDefaultConfig(), gen, log)
}

func TestNewServer(t *testing.T) {
	s := newServer()
	assert.NotNil(t, s)
}

func TestServerStartWithoutListener(t *testing.T) {
	s := newServer()
	err := s.Start()
	assert.Nil(t, err)
}

func TestServerStartWithListener(t *testing.T) {
	s := newServer()
	l := &listenerNewMock{}

	l.On("Listen").Return(make(chan net.Conn))
	s.AddListener(l)

	err := s.Start()
	assert.Nil(t, err)
	l.AssertExpectations(t)
}

func TestServerStartMultipleListeners(t *testing.T) {
	s := newServer()
	l1 := &listenerNewMock{}
	l2 := &listenerNewMock{}

	l1.On("Listen").Return(make(chan net.Conn))
	s.AddListener(l1)

	l2.On("Listen").Return(make(chan net.Conn))
	s.AddListener(l2)

	err := s.Start()
	assert.Nil(t, err)
	l1.AssertExpectations(t)
	l2.AssertExpectations(t)
}

func TestServerStartFail(t *testing.T) {
	s := newServer()
	l := &listenerNewMock{}

	l.On("Listen").Return(nil, errors.New("failed"))
	s.AddListener(l)

	err := s.Start()
	assert.NotNil(t, err)
	l.AssertExpectations(t)
}

func TestServerStartMultipleListenersFail(t *testing.T) {
	s := newServer()
	l1 := &listenerNewMock{}
	l2 := &listenerNewMock{}

	l1.On("Listen").Return(make(chan net.Conn))
	l1.On("Close")
	s.AddListener(l1)

	l2.On("Listen").Return(nil, errors.New("failed"))
	s.AddListener(l2)

	err := s.Start()
	assert.NotNil(t, err)
	l1.AssertExpectations(t)
	l2.AssertExpectations(t)
}

func TestServerShutdown(t *testing.T) {
	s := newServer()
	l := &listenerNewMock{}

	l.On("Listen").Return(make(chan net.Conn))
	s.AddListener(l)

	err := s.Start()
	require.Nil(t, err)

	l.On("Close")
	err = s.Shutdown(context.Background())
	assert.Nil(t, err)
	l.AssertExpectations(t)
}

func TestServerShutdownMultipleListeners(t *testing.T) {
	s := newServer()
	l1 := &listenerNewMock{}
	l2 := &listenerNewMock{}

	l1.On("Listen").Return(make(chan net.Conn))
	s.AddListener(l1)

	l2.On("Listen").Return(make(chan net.Conn))
	s.AddListener(l2)

	err := s.Start()
	require.Nil(t, err)

	l1.On("Close").Return(errors.New("failed"))
	l2.On("Close")
	err = s.Shutdown(context.Background())
	assert.Nil(t, err)
	l1.AssertExpectations(t)
	l2.AssertExpectations(t)
}

func TestServerStop(t *testing.T) {
	s := newServer()
	l := &listenerNewMock{}

	l.On("Listen").Return(make(chan net.Conn))
	s.AddListener(l)

	err := s.Start()
	require.Nil(t, err)

	l.On("Close")
	s.Stop()
	l.AssertExpectations(t)
}

func TestServerHandleNewConnection(t *testing.T) {
	s := newServer()
	l := &listenerNewMock{}

	connStream := make(chan net.Conn)

	l.On("Listen").Return(connStream, nil)
	s.AddListener(l)

	err := s.Start()
	require.Nil(t, err)

	c1, c2 := net.Pipe()

	var wg sync.WaitGroup
	starting := make(chan bool)
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 1)

		close(starting)
		_, readErr := c1.Read(buf)
		assert.ErrorIs(t, readErr, io.EOF)
	}()

	<-starting
	connStream <- c2

	l.On("Close")
	err = s.Shutdown(context.Background())
	assert.Nil(t, err)

	_ = c1.Close()
	wg.Wait()
	l.AssertExpectations(t)
}
