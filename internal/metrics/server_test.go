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

package metrics

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type logIDGenStub struct {
}

func (m *logIDGenStub) NextID() uint64 {
	return 0
}

type logBuffer struct {
	b bytes.Buffer
	m sync.Mutex
}

func (b *logBuffer) Write(p []byte) (n int, err error) {
	b.m.Lock()
	defer b.m.Unlock()
	return b.b.Write(p)
}

func newLogger() logger.Logger {
	out := &logBuffer{}
	return logger.New(out, &logIDGenStub{})
}

func TestMetricsNewServer(t *testing.T) {
	log := newLogger()

	t.Run("Valid", func(t *testing.T) {
		conf := Configuration{Address: ":8888", Path: "/metrics"}

		lsn, err := NewListener(conf, &log)
		assert.Nil(t, err)
		assert.NotNil(t, lsn)
	})

	t.Run("MissingAddress", func(t *testing.T) {
		conf := Configuration{Address: "", Path: "/metrics"}

		lsn, err := NewListener(conf, &log)
		assert.Nil(t, lsn)
		assert.ErrorContains(t, err, "missing address")
	})

	t.Run("MissingPath", func(t *testing.T) {
		conf := Configuration{Address: ":8888", Path: ""}

		lsn, err := NewListener(conf, &log)
		assert.Nil(t, lsn)
		assert.ErrorContains(t, err, "missing path")
	})
}

func TestMetricsRunInvalidAddress(t *testing.T) {
	log := newLogger()
	conf := Configuration{Address: ".", Path: "/metrics"}

	lsn, err := NewListener(conf, &log)
	require.Nil(t, err)
	require.NotNil(t, lsn)

	err = lsn.Listen()
	require.NotNil(t, err)
}

func TestMetricsRunAndStop(t *testing.T) {
	log := newLogger()
	conf := Configuration{Address: ":8888", Path: "/metrics"}

	lsn, err := NewListener(conf, &log)
	require.Nil(t, err)
	require.NotNil(t, lsn)

	done := make(chan bool)
	go func() {
		err = lsn.Listen()
		done <- true
	}()

	<-time.After(5 * time.Millisecond)
	lsn.Stop()

	<-done
	assert.Nil(t, err)
}
