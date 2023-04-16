// Copyright 2023 The MaxMQ Authors
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
	"bytes"
	"os"
	"sync"
	"testing"

	"github.com/gsalomao/maxmq/internal/config"
	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type listenerMock struct {
	mock.Mock
	running chan bool
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

func TestCLIRunStartLoadConfig(t *testing.T) {
	c, found, err := loadConfig()
	assert.Nil(t, err)
	assert.False(t, found)
	assert.Equal(t, config.DefaultConfig, c)
}

func TestCLIRunStartNewLogger(t *testing.T) {
	l, err := newLogger(logger.Pretty, "TRACE", nil)
	assert.Nil(t, err)
	assert.NotNil(t, l)
}

func TestCLIRunStartNewServer(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, nil, logger.Pretty)

	s, err := newServer(config.DefaultConfig, log, 0)
	assert.Nil(t, err)
	assert.NotNil(t, s)
}

func TestCLIRunStartStartServer(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, nil, logger.Pretty)

	s := server.New(log)

	l := &listenerMock{running: make(chan bool)}
	s.AddListener(l)

	l.On("Listen").Return(nil)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		startServer(s, log, true)
	}()

	<-l.running
	l.running <- false
	wg.Wait()
	_ = os.Remove("cpu.prof")
	_ = os.Remove("heap.prof")
}

func TestCLIRunStartHelp(t *testing.T) {
	out := bytes.NewBufferString("")
	c := New(out, []string{"start", "--help"})

	err := c.Run()
	assert.Nil(t, err)
	assert.Contains(t, out.String(), "Start the MaxMQ server")
}
