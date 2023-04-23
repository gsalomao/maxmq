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
	"sync"
	"testing"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type listenerMock struct {
	mock.Mock
	wg sync.WaitGroup
}

func (l *listenerMock) Start() error {
	args := l.Called()
	l.wg.Add(1)
	return args.Error(0)
}

func (l *listenerMock) Stop() {
	l.Called()
	l.wg.Done()
}

func (l *listenerMock) Wait() {
	l.Called()
	l.wg.Wait()
}

func newLogger() *logger.Logger {
	out := bytes.NewBufferString("")
	return logger.New(out, nil, logger.Json)
}

func TestServerStart(t *testing.T) {
	log := newLogger()
	s := New(log)

	l := &listenerMock{}
	l.On("Start").Return(nil)
	s.AddListener(l)

	err := s.Start()
	require.Nil(t, err)
	l.AssertExpectations(t)
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

	l := &listenerMock{}
	l.On("Start").Return(nil)
	s.AddListener(l)

	err := s.Start()
	require.Nil(t, err)

	l.On("Stop")
	l.On("Wait")
	s.Stop()
	l.AssertExpectations(t)
}

func TestServerStartWithListenerError(t *testing.T) {
	log := newLogger()
	s := New(log)

	l := &listenerMock{}
	l.On("Start").Return(errors.New("any failure"))
	s.AddListener(l)

	err := s.Start()
	require.NotNil(t, err)
	l.AssertExpectations(t)
}

func TestServerStartWithListenerErrorMultipleListeners(t *testing.T) {
	log := newLogger()
	s := New(log)

	l1 := &listenerMock{}
	l1.On("Start").Return(nil)
	l1.On("Wait")
	l1.On("Stop")
	s.AddListener(l1)

	l2 := &listenerMock{}
	l2.On("Start").Return(errors.New("any failure"))
	s.AddListener(l2)

	err := s.Start()
	require.NotNil(t, err)

	l1.AssertExpectations(t)
	l2.AssertExpectations(t)
}
