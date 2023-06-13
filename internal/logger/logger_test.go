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

package logger

import (
	"bytes"
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type logIDGenMock struct {
	mock.Mock
}

func (m *logIDGenMock) NextID() uint64 {
	args := m.Called()
	return uint64(args.Int(0))
}

func TestLoggerNew(t *testing.T) {
	gen := logIDGenMock{}
	gen.On("NextID").Return(1)

	out := bytes.NewBufferString("")
	log := New(out, &gen, Pretty)
	assert.NotNil(t, log)
}

func TestLoggerNewWithPrefix(t *testing.T) {
	gen := logIDGenMock{}
	gen.On("NextID").Return(1)

	out := bytes.NewBufferString("")
	log := NewWithPrefix(out, &gen, "prefix", JSON)
	assert.NotNil(t, log)
}

func TestLoggerWithPrefix(t *testing.T) {
	gen := logIDGenMock{}
	gen.On("NextID").Return(1)

	out := bytes.NewBufferString("")
	log := NewWithPrefix(out, &gen, "prefix", Pretty)

	nlg := log.WithPrefix("test")
	assert.NotNil(t, nlg)
}

func TestLoggerLog(t *testing.T) {
	gen := logIDGenMock{}
	gen.On("NextID").Return(1)

	out := bytes.NewBufferString("")
	log := New(out, &gen, Pretty)
	msg := gofakeit.Phrase()

	log.Info().Msg(msg)
	assert.Contains(t, out.String(), "INFO")
	assert.Contains(t, out.String(), msg)
}

func TestLoggerLogWithPrefix(t *testing.T) {
	gen := logIDGenMock{}
	gen.On("NextID").Return(1)

	out := bytes.NewBufferString("")
	log := NewWithPrefix(out, &gen, "prefix", Pretty)
	msg := gofakeit.Phrase()

	log.Info().Msg(msg)
	assert.Contains(t, out.String(), "[prefix]")
}

func TestLoggerLogWithoutIDGenerator(t *testing.T) {
	out := bytes.NewBufferString("")
	log := New(out, nil, Pretty)
	msg := gofakeit.Phrase()

	log.Info().Msg(msg)
	assert.Contains(t, out.String(), "INFO")
	assert.Contains(t, out.String(), msg)
}

func TestLoggerWithField(t *testing.T) {
	gen := logIDGenMock{}
	gen.On("NextID").Return(1)

	out := bytes.NewBufferString("")
	log := New(out, &gen, Pretty)
	key := gofakeit.Word()
	val := gofakeit.Phrase()

	log.Info().Str(key, val).Msg("")
	assert.Contains(t, out.String(), key+"=")
	assert.Contains(t, out.String(), val)
}

func TestLoggerWithLogId(t *testing.T) {
	gen := logIDGenMock{}
	gen.On("NextID").Return(255)

	out := bytes.NewBufferString("")
	log := New(out, &gen, Pretty)
	msg := gofakeit.Phrase()

	log.Info().Msg(msg)
	assert.Contains(t, out.String(), "LogId=")
}

func TestLoggerBaseLogger(t *testing.T) {
	gen := logIDGenMock{}
	gen.On("NextID").Return(255)

	out := bytes.NewBufferString("")
	log := New(out, &gen, Pretty)
	msg := gofakeit.Phrase()

	bsLog := log.BaseLogger()
	bsLog.Info().Msg(msg)
	assert.Contains(t, out.String(), "INFO")
	assert.Contains(t, out.String(), msg)
}

func TestLoggerSetSeverity(t *testing.T) {
	gen := logIDGenMock{}
	out := bytes.NewBufferString("")
	log := New(out, &gen, JSON)

	msg := gofakeit.Phrase()
	err := SetSeverityLevel("info")

	log.Debug().Msg(msg)
	assert.Nil(t, err)
	assert.Empty(t, out.String())
}

func TestLoggerSetInvalidSeverity(t *testing.T) {
	err := SetSeverityLevel("invalid")
	assert.NotNil(t, err)
	assert.ErrorContains(t, err, "invalid log level")
}
