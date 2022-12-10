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

package broker_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/gsalomao/maxmq/internal/broker"
	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type listenerMock struct {
	mock.Mock
	RunningCh chan bool
	StopCh    chan bool
	Err       error
}

func newListenerMock() *listenerMock {
	return &listenerMock{
		RunningCh: make(chan bool),
		StopCh:    make(chan bool),
	}
}

func (l *listenerMock) Listen() error {
	l.Called()
	l.RunningCh <- true
	<-l.StopCh
	return l.Err
}

func (l *listenerMock) Stop() {
	l.Called()
	l.StopCh <- true
}

type logIDGenStub struct {
}

func (m *logIDGenStub) NextID() uint64 {
	return 0
}

func newLogger() logger.Logger {
	out := bytes.NewBufferString("")
	return logger.New(out, &logIDGenStub{})
}

func TestBrokerStart(t *testing.T) {
	log := newLogger()
	b := broker.New(&log)

	mockLsn := newListenerMock()
	mockLsn.On("Listen")
	b.AddListener(mockLsn)

	err := b.Start()
	assert.Nil(t, err)
}

func TestBrokerStartWithoutListener(t *testing.T) {
	log := newLogger()
	b := broker.New(&log)

	err := b.Start()
	assert.ErrorContains(t, err, "no available listener")
}

func TestBrokerStop(t *testing.T) {
	log := newLogger()
	b := broker.New(&log)

	mockLsn := newListenerMock()
	mockLsn.On("Listen")
	mockLsn.On("Stop")
	b.AddListener(mockLsn)

	err := b.Start()
	require.Nil(t, err)

	done := make(chan bool)
	go func() {
		err = b.Wait()
		done <- true
	}()

	<-mockLsn.RunningCh
	b.Stop()

	<-done
	assert.Nil(t, err)
}

func TestBrokerListenerError(t *testing.T) {
	log := newLogger()
	b := broker.New(&log)

	mockLsn := newListenerMock()
	mockLsn.On("Listen")
	mockLsn.On("Stop")
	b.AddListener(mockLsn)

	err := b.Start()
	require.Nil(t, err)

	done := make(chan bool)
	go func() {
		err = b.Wait()
		done <- true
	}()

	<-mockLsn.RunningCh
	mockLsn.Err = errors.New("any failure")
	b.Stop()

	<-done
	assert.NotNil(t, err)
}
