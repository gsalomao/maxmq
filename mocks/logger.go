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

package mocks

import (
	"bytes"
	"sync"

	"github.com/gsalomao/maxmq/internal/logger"
)

// LoggerStub is responsible to simulate a real logger.
type LoggerStub struct {
	log logger.Logger
	out *loggerOutputStub
}

// NewLoggerStub creates a LoggerStub.
func NewLoggerStub() *LoggerStub {
	out := &loggerOutputStub{
		buf: bytes.NewBufferString(""),
	}
	log := logger.New(out, &logIDGenStub{})

	return &LoggerStub{log: log, out: out}
}

// Logger returns the underlying logger in the stub.
func (l *LoggerStub) Logger() *logger.Logger {
	return &l.log
}

// String returns the string of all generated logs.
func (l *LoggerStub) String() string {
	return l.out.String()
}

type loggerOutputStub struct {
	buf *bytes.Buffer
	mtx sync.Mutex
}

func (l *loggerOutputStub) Read(p []byte) (n int, err error) {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	return l.buf.Read(p)
}
func (l *loggerOutputStub) Write(p []byte) (n int, err error) {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	return l.buf.Write(p)
}
func (l *loggerOutputStub) String() string {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	return l.buf.String()
}

type logIDGenStub struct {
}

func (m *logIDGenStub) NextID() uint64 {
	return 0
}
