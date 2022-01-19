/*
 * Copyright 2022 The MaxMQ Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package broker_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/gsalomao/maxmq/pkg/broker"
	"github.com/gsalomao/maxmq/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type listenerStub struct {
	stop    chan bool
	running chan bool
	err     error
}

func newListenerStub() *listenerStub {
	return &listenerStub{
		stop:    make(chan bool),
		running: make(chan bool),
	}
}

func (l *listenerStub) Run() error {
	l.running <- true
	<-l.stop
	return l.err
}

func (l *listenerStub) Stop() {
	l.stop <- true
}

func TestBroker_Start(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out)

	b, err := broker.New(&log)
	require.Nil(t, err)

	l := newListenerStub()
	b.AddListener(l)

	err = b.Start()
	assert.Nil(t, err)
	assert.Contains(t, out.String(), "Broker started with success")
}

func TestBroker_StartWithNoListener(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out)

	b, err := broker.New(&log)
	require.Nil(t, err)

	err = b.Start()
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "no available listener")
}

func TestBroker_Stop(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out)

	b, err := broker.New(&log)
	require.Nil(t, err)

	l := newListenerStub()
	b.AddListener(l)

	err = b.Start()
	require.Nil(t, err)

	done := make(chan bool)
	go func() {
		err = b.Wait()
		done <- true
	}()

	<-l.running
	b.Stop()

	<-done
	assert.Nil(t, err)
}

func TestBroker_ListenerError(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out)

	b, err := broker.New(&log)
	require.Nil(t, err)

	l := newListenerStub()
	b.AddListener(l)

	err = b.Start()
	require.Nil(t, err)

	done := make(chan bool)
	go func() {
		err = b.Wait()
		done <- true
	}()

	<-l.running
	l.err = errors.New("any failure")
	b.Stop()

	<-done
	assert.NotNil(t, err)
}
