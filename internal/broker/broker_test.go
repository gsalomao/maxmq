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
	"errors"
	"testing"

	"github.com/gsalomao/maxmq/internal/broker"
	"github.com/gsalomao/maxmq/mocks"
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

func TestBroker_Start(t *testing.T) {
	log := mocks.NewLoggerStub()
	b := broker.New(log.Logger())

	mockLsn := newListenerMock()
	mockLsn.On("Listen")
	b.AddListener(mockLsn)

	err := b.Start()
	assert.Nil(t, err)
	assert.Contains(t, log.String(), "Broker started with success")
}

func TestBroker_StartWithoutListener(t *testing.T) {
	log := mocks.NewLoggerStub()
	b := broker.New(log.Logger())

	err := b.Start()
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "no available listener")
}

func TestBroker_Stop(t *testing.T) {
	log := mocks.NewLoggerStub()
	b := broker.New(log.Logger())

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

func TestBroker_ListenerError(t *testing.T) {
	log := mocks.NewLoggerStub()
	b := broker.New(log.Logger())

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
