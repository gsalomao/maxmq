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

package cli

import (
	"testing"

	"github.com/gsalomao/maxmq/broker"
	"github.com/gsalomao/maxmq/config"
	"github.com/gsalomao/maxmq/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCLI_NewBroker(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		logStub := mocks.NewLoggerStub()
		conf := config.Config{
			MQTTTCPAddress: ":1883",
			MetricsEnabled: true,
			MetricsAddress: ":8888",
			MetricsPath:    "/metrics",
		}
		brk, err := newBroker(conf, logStub.Logger())
		require.Nil(t, err)
		require.NotNil(t, brk)
	})

	t.Run("InvalidMetrics", func(t *testing.T) {
		logStub := mocks.NewLoggerStub()
		conf := config.Config{
			MQTTTCPAddress: ":1883",
			MetricsEnabled: true,
			MetricsAddress: "",
			MetricsPath:    "",
		}
		brk, err := newBroker(conf, logStub.Logger())
		require.NotNil(t, err)
		require.Nil(t, brk)
	})
}

func TestCLI_RunBroker(t *testing.T) {
	logStub := mocks.NewLoggerStub()

	mockRunner := mocks.NewRunnerMock()
	mockRunner.On("Run")
	mockRunner.On("Stop")

	brk := broker.New(logStub.Logger())
	brk.AddRunner(mockRunner)

	done := make(chan bool)
	go func() {
		defer func() {
			done <- true
		}()

		runBroker(&brk, logStub.Logger())
		assert.Contains(t, logStub.String(), "Starting broker")
	}()

	<-mockRunner.RunningCh
	brk.Stop()
	<-done
}

func TestCLI_LoadConfig(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	conf, err := loadConfig(logStub.Logger())
	require.Nil(t, err)
	assert.Equal(t, "info", conf.LogLevel)
}
