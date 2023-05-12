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

package cli

import (
	"bytes"
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/gsalomao/maxmq/internal/config"
	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/stretchr/testify/assert"
)

func TestStartLoadConfig(t *testing.T) {
	c, found, err := loadConfig()
	assert.Nil(t, err)
	assert.False(t, found)
	assert.Equal(t, config.DefaultConfig, c)
}

func TestStartNewLogger(t *testing.T) {
	l, err := newLogger(logger.Pretty, "TRACE", 0)
	assert.Nil(t, err)
	assert.NotNil(t, l)
}

func TestStartRunServer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	out := bytes.NewBufferString("")
	log := logger.New(out, nil, logger.Pretty)

	var wg sync.WaitGroup
	starting := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		close(starting)
		runServer(ctx, config.DefaultConfig, true, true, 0, log)
	}()

	<-starting
	<-time.After(100 * time.Millisecond)

	cancel()
	wg.Wait()
	_ = os.Remove("cpu.prof")
	_ = os.Remove("heap.prof")
}

func TestStartHelp(t *testing.T) {
	out := bytes.NewBufferString("")
	c := New(out, []string{"start", "--help"})

	err := c.Run()
	assert.Nil(t, err)
	assert.Contains(t, out.String(), "Start the MaxMQ server")
}
