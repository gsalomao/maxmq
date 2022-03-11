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

package logger_test

import (
	"bytes"
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/gsalomao/maxmq/logger"
	"github.com/stretchr/testify/assert"
)

func TestLogger_Log(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out)
	msg := gofakeit.Phrase()

	log.Info().Msg(msg)
	assert.Contains(t, out.String(), "INFO")
	assert.Contains(t, out.String(), msg)
}

func TestLogger_WithField(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out)
	key := gofakeit.Word()
	val := gofakeit.Phrase()

	log.Info().Str(key, val).Msg("")
	assert.Contains(t, out.String(), key+"=")
	assert.Contains(t, out.String(), val)
}

func TestLogger_SetSeverity(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out)
	msg := gofakeit.Phrase()
	err := logger.SetSeverityLevel("info")

	log.Debug().Msg(msg)
	assert.Nil(t, err)
	assert.Empty(t, out.String())
}

func TestLogger_SetInvalidSeverity(t *testing.T) {
	err := logger.SetSeverityLevel("invalid")
	assert.NotNil(t, err)
	assert.Equal(t, "invalid log level", err.Error())
}
