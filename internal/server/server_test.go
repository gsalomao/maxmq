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

package server

import (
	"bytes"
	"errors"
	"testing"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type listenerMock struct {
	mock.Mock
	running chan bool
}

func newListenerMock() *listenerMock {
	return &listenerMock{running: make(chan bool)}
}

func (l *listenerMock) Listen() error {
	args := l.Called()
	l.running <- true
	<-l.running
	return args.Error(0)
}

func (l *listenerMock) Stop() {
	l.Called()
	l.running <- false
}

func newLogger() *logger.Logger {
	out := bytes.NewBufferString("")
	return logger.New(out, nil, logger.Json)
}

func TestServerStart(t *testing.T) {
	log := newLogger()
	s := New(log)

	l := newListenerMock()
	l.On("Listen").Return(nil)
	s.AddListener(l)

	err := s.Start()
	require.Nil(t, err)

	<-l.running
	l.running <- false
	err = s.Wait()
	assert.Nil(t, err)
}

func TestServerStartWithoutListener(t *testing.T) {
	log := newLogger()
	s := New(log)

	err := s.Start()
	assert.ErrorContains(t, err, "no available listener")
}

func TestServerStop(t *testing.T) {
	log := newLogger()
	s := New(log)

	l := newListenerMock()
	l.On("Listen").Return(nil)
	l.On("Stop")
	s.AddListener(l)

	err := s.Start()
	require.Nil(t, err)

	<-l.running
	s.Stop()
	err = s.Wait()
	assert.Nil(t, err)
}

func TestServerListenerError(t *testing.T) {
	log := newLogger()
	s := New(log)

	l := newListenerMock()
	l.On("Listen").Return(errors.New("any failure"))
	l.On("Stop")
	s.AddListener(l)

	err := s.Start()
	require.Nil(t, err)

	<-l.running
	s.Stop()
	err = s.Wait()
	assert.NotNil(t, err)
}
