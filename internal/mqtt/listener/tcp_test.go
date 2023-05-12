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

package listener

import (
	"bytes"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"testing"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type logBuffer struct {
	buffer bytes.Buffer
	mutex  sync.Mutex
}

func (b *logBuffer) Write(p []byte) (n int, err error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.Write(p)
}

func newLogger() *logger.Logger {
	out := &logBuffer{}
	return logger.New(out, nil, logger.Json)
}

func newTCPListener(address string) *TCPListener {
	log := newLogger()
	return NewTCPListener(address, log)
}

func TestNewTCPListener(t *testing.T) {
	address := fmt.Sprintf(":%v", 10000+rand.Intn(10000))
	l := newTCPListener(address)
	assert.NotNil(t, l)
}

func TestTCPListenerListenAndClose(t *testing.T) {
	address := fmt.Sprintf(":%v", 10000+rand.Intn(10000))
	l := newTCPListener(address)

	connStream, err := l.Listen()
	assert.NotNil(t, connStream)
	assert.Nil(t, err)

	err = l.Close()
	assert.Nil(t, err)
}

func TestTCPListenerListenInvalidAddress(t *testing.T) {
	l := newTCPListener(".")

	connStream, err := l.Listen()
	assert.Nil(t, connStream)
	assert.NotNil(t, err)
}

func TestTCPListenerCloseWithoutListen(t *testing.T) {
	address := fmt.Sprintf(":%v", 10000+rand.Intn(10000))
	l := newTCPListener(address)

	err := l.Close()
	assert.Nil(t, err)
}

func TestTCPListenerAcceptConnection(t *testing.T) {
	address := fmt.Sprintf(":%v", 10000+rand.Intn(10000))
	l := newTCPListener(address)

	connStream, err := l.Listen()
	assert.Nil(t, err)

	var conn net.Conn
	conn, err = net.Dial("tcp", address)
	defer func() { _ = conn.Close() }()
	require.Nil(t, err)

	<-connStream
	err = l.Close()
	assert.Nil(t, err)
}
