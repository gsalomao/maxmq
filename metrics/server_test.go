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

package metrics_test

import (
	"testing"
	"time"

	"github.com/gsalomao/maxmq/metrics"
	"github.com/gsalomao/maxmq/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetrics_NewServer(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		logStub := mocks.NewLoggerStub()
		conf := metrics.Configuration{Address: ":8888", Path: "/metrics"}

		p, err := metrics.NewServer(conf, logStub.Logger())
		assert.Nil(t, err)
		assert.NotNil(t, p)
	})

	t.Run("MissingAddress", func(t *testing.T) {
		logStub := mocks.NewLoggerStub()
		conf := metrics.Configuration{Address: "", Path: "/metrics"}

		p, err := metrics.NewServer(conf, logStub.Logger())
		assert.NotNil(t, err)
		assert.Nil(t, p)
		assert.Contains(t, err.Error(), "missing address")
	})

	t.Run("MissingPath", func(t *testing.T) {
		logStub := mocks.NewLoggerStub()
		conf := metrics.Configuration{Address: ":8888", Path: ""}

		p, err := metrics.NewServer(conf, logStub.Logger())
		assert.NotNil(t, err)
		assert.Nil(t, p)
		assert.Contains(t, err.Error(), "missing path")
	})
}

func TestMetrics_RunInvalidAddress(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	conf := metrics.Configuration{Address: ":1", Path: "/metrics"}

	p, err := metrics.NewServer(conf, logStub.Logger())
	require.Nil(t, err)
	require.NotNil(t, p)

	err = p.Run()
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "bind: permission denied")
}

func TestMetrics_RunAndStop(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	conf := metrics.Configuration{Address: ":8888", Path: "/metrics"}

	p, err := metrics.NewServer(conf, logStub.Logger())
	require.Nil(t, err)
	require.NotNil(t, p)

	done := make(chan bool)
	go func() {
		err = p.Run()
		done <- true
	}()

	<-time.After(5 * time.Millisecond)
	assert.Contains(t, logStub.String(), "Listening on [::]:8888")
	p.Stop()

	<-done
	assert.Nil(t, err)
	assert.Contains(t, logStub.String(), "stopped with success")
}
