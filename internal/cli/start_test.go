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

package cli

import (
	"os"
	"testing"

	"github.com/gsalomao/maxmq/internal/broker"
	"github.com/gsalomao/maxmq/internal/config"
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

func TestCLI_NewBroker(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		logStub := mocks.NewLoggerStub()
		conf := config.Config{
			MQTTTCPAddress:   ":1883",
			MetricsEnabled:   true,
			MetricsAddress:   ":8888",
			MetricsPath:      "/metrics",
			MetricsProfiling: true,
		}

		b, err := newBroker(conf, logStub.Logger(), 0)
		require.Nil(t, err)
		require.NotNil(t, b)
	})

	t.Run("InvalidMetrics", func(t *testing.T) {
		logStub := mocks.NewLoggerStub()
		conf := config.Config{
			MQTTTCPAddress: ":1883",
			MetricsEnabled: true,
			MetricsAddress: "",
			MetricsPath:    "",
		}

		b, err := newBroker(conf, logStub.Logger(), 0)
		require.NotNil(t, err)
		require.Nil(t, b)
	})
}

func TestCLI_RunBroker(t *testing.T) {
	logStub := mocks.NewLoggerStub()

	mockLsn := newListenerMock()
	mockLsn.On("Listen")
	mockLsn.On("Stop")

	b := broker.New(logStub.Logger())
	b.AddListener(mockLsn)

	done := make(chan bool)
	go func() {
		defer func() {
			done <- true
		}()

		runBroker(&b, logStub.Logger())
		assert.Contains(t, logStub.String(), "Starting broker")
	}()

	<-mockLsn.RunningCh
	b.Stop()
	<-done
}

func TestCLI_LoadConfig(t *testing.T) {
	logStub := mocks.NewLoggerStub()

	c, err := loadConfig(logStub.Logger())
	require.Nil(t, err)
	assert.Equal(t, "info", c.LogLevel)
}

func TestCLI_StartStopCPUProfile(t *testing.T) {
	f, err := startCPUProfile()
	require.Nil(t, err)

	stopCPUProfile()
	err = f.Close()
	require.Nil(t, err)

	err = os.Remove(f.Name())
	require.Nil(t, err)
}

func TestCLI_SaveHeapProfile(t *testing.T) {
	err := saveHeapProfile()
	require.Nil(t, err)

	err = os.Remove("heap.prof")
	require.Nil(t, err)
}
