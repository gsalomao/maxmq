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

package server_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type listenerMock struct {
	mock.Mock
	runningCh chan bool
	stopCh    chan bool
	err       error
}

func newListenerMock() *listenerMock {
	return &listenerMock{
		runningCh: make(chan bool),
		stopCh:    make(chan bool),
	}
}

func (l *listenerMock) Listen() error {
	l.Called()
	l.runningCh <- true
	<-l.stopCh
	return l.err
}

func (l *listenerMock) Stop() {
	l.Called()
	l.stopCh <- true
}

type logIDGenStub struct {
}

func (m *logIDGenStub) NextID() uint64 {
	return 0
}

func newLogger() *logger.Logger {
	out := bytes.NewBufferString("")
	return logger.New(out, &logIDGenStub{}, logger.Json)
}

func TestServerStart(t *testing.T) {
	log := newLogger()
	s := server.New(log)

	lsn := newListenerMock()
	lsn.On("Listen")
	s.AddListener(lsn)

	done := make(chan bool)
	go func() {
		defer close(done)
		err := s.Start()
		assert.Nil(t, err)
	}()

	<-lsn.runningCh
	lsn.stopCh <- true
	<-done
}

func TestServerStartWithoutListener(t *testing.T) {
	log := newLogger()
	s := server.New(log)

	err := s.Start()
	assert.ErrorContains(t, err, "no available listener")
}

func TestServerStop(t *testing.T) {
	log := newLogger()
	s := server.New(log)

	lsn := newListenerMock()
	lsn.On("Listen")
	lsn.On("Stop")
	s.AddListener(lsn)

	err := s.Start()
	require.Nil(t, err)

	done := make(chan bool)
	go func() {
		defer close(done)
		err = s.Wait()
		assert.Nil(t, err)
	}()

	<-lsn.runningCh
	s.Stop()
	<-done
}

func TestServerListenerError(t *testing.T) {
	log := newLogger()
	s := server.New(log)

	lsn := newListenerMock()
	lsn.On("Listen")
	lsn.On("Stop")
	s.AddListener(lsn)

	err := s.Start()
	require.Nil(t, err)

	done := make(chan bool)
	go func() {
		defer close(done)
		err = s.Wait()
		assert.NotNil(t, err)
	}()

	<-lsn.runningCh
	lsn.err = errors.New("any failure")
	s.Stop()
	<-done
}
