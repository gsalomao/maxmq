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

	"github.com/gsalomao/maxmq/mocks"
	"github.com/gsalomao/maxmq/pkg/broker"
	"github.com/gsalomao/maxmq/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBroker_Start(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out)
	b := broker.New(&log)

	mockRunner := mocks.NewRunnerMock()
	mockRunner.On("Run")
	b.AddRunner(mockRunner)

	err := b.Start()
	assert.Nil(t, err)
	assert.Contains(t, out.String(), "Broker started with success")
}

func TestBroker_StartWithNoRunner(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out)

	b := broker.New(&log)

	err := b.Start()
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "no available runner")
}

func TestBroker_Stop(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out)
	b := broker.New(&log)

	mockRunner := mocks.NewRunnerMock()
	mockRunner.On("Run")
	mockRunner.On("Stop")
	b.AddRunner(mockRunner)

	err := b.Start()
	require.Nil(t, err)

	done := make(chan bool)
	go func() {
		err = b.Wait()
		done <- true
	}()

	<-mockRunner.RunningCh
	b.Stop()

	<-done
	assert.Nil(t, err)
}

func TestBroker_RunnerError(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out)
	b := broker.New(&log)

	mockRunner := mocks.NewRunnerMock()
	mockRunner.On("Run")
	mockRunner.On("Stop")
	b.AddRunner(mockRunner)

	err := b.Start()
	require.Nil(t, err)

	done := make(chan bool)
	go func() {
		err = b.Wait()
		done <- true
	}()

	<-mockRunner.RunningCh
	mockRunner.Err = errors.New("any failure")
	b.Stop()

	<-done
	assert.NotNil(t, err)
}
