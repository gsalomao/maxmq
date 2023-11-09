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

package logger_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/logger/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoggerNew(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, nil)
	assert.NotNil(t, log)
}

func TestLoggerLog(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, &logger.Options{Level: logger.LevelDebug})
	msg := gofakeit.Phrase()
	ctx := context.Background()

	testCases := []struct {
		level string
		log   func()
	}{
		{"DEBUG", func() { log.Debug(ctx, msg) }},
		{"INFO", func() { log.Info(ctx, msg) }},
		{"WARN", func() { log.Warn(ctx, msg) }},
		{"ERROR", func() { log.Error(ctx, msg) }},
	}

	for _, tc := range testCases {
		t.Run(tc.level, func(t *testing.T) {
			out.Reset()
			tc.log()

			assert.Contains(t, out.String(), tc.level)
			assert.Contains(t, out.String(), msg)
		})
	}
}

func TestLoggerLogWithIDGenerator(t *testing.T) {
	idGen := mocks.NewLogIDGenerator(t)
	idGen.EXPECT().NextID().Return(1)

	out := bytes.NewBufferString("")
	log := logger.New(out, &logger.Options{LogIDGenerator: idGen, Format: logger.FormatText})

	log.Info(context.Background(), gofakeit.Phrase())
	assert.Contains(t, out.String(), "log_id=1")
}

func TestLoggerLogStdLog(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, &logger.Options{Level: logger.LevelDebug}).StdLog()
	msg := gofakeit.Phrase()

	log.Debug(msg)
	assert.Contains(t, out.String(), logger.LevelDebug.String())
	assert.Contains(t, out.String(), msg)
}

func TestLoggerLogStdLogWithAttrs(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, &logger.Options{Format: logger.FormatText}).StdLog(logger.Str("a", "b"))
	msg := gofakeit.Phrase()

	log.Info(msg)
	assert.Contains(t, out.String(), "a=b")
}

func TestLoggerAttr(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, &logger.Options{Format: logger.FormatText})
	ctx := context.Background()
	msg := gofakeit.Phrase()
	date := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

	testCases := []struct {
		attr     string
		log      func()
		expected string
	}{
		{"String", func() { log.Info(ctx, msg, logger.Str("key", "val")) }, "key=val"},
		{"Int", func() { log.Info(ctx, msg, logger.Int("key", 1)) }, "key=1"},
		{"Int8", func() { log.Info(ctx, msg, logger.Int8("key", 1)) }, "key=1"},
		{"Int16", func() { log.Info(ctx, msg, logger.Int16("key", 1)) }, "key=1"},
		{"Int32", func() { log.Info(ctx, msg, logger.Int32("key", 1)) }, "key=1"},
		{"Int64", func() { log.Info(ctx, msg, logger.Int64("key", 1)) }, "key=1"},
		{"Uint", func() { log.Info(ctx, msg, logger.Uint("key", 1)) }, "key=1"},
		{"Uint8", func() { log.Info(ctx, msg, logger.Uint8("key", 1)) }, "key=1"},
		{"Uint16", func() { log.Info(ctx, msg, logger.Uint16("key", 1)) }, "key=1"},
		{"Uint32", func() { log.Info(ctx, msg, logger.Uint32("key", 1)) }, "key=1"},
		{"Uint64", func() { log.Info(ctx, msg, logger.Uint64("key", 1)) }, "key=1"},
		{"Float64", func() { log.Info(ctx, msg, logger.Float64("key", 1)) }, "key=1"},
		{"Bool", func() { log.Info(ctx, msg, logger.Bool("key", true)) }, "key=true"},
		{"Time", func() { log.Info(ctx, msg, logger.Time("key", date)) }, "key=2023-01-01T00:00:00.000Z"},
		{"Duration", func() { log.Info(ctx, msg, logger.Duration("key", time.Second)) }, "key=1s"},
		{"Error", func() { log.Info(ctx, msg, logger.Err(errors.New("failed"))) }, "error=failed"},
		{"Group", func() { log.Info(ctx, msg, logger.Group("a", logger.Str("key", "val"))) }, "a.key=val"},
		{"Any", func() { log.Info(ctx, msg, logger.Any("key", logger.LevelInfo)) }, "key=INFO"},
	}

	for _, tc := range testCases {
		t.Run(tc.attr, func(t *testing.T) {
			out.Reset()
			tc.log()
			assert.Contains(t, out.String(), tc.expected)
		})
	}
}

func TestLoggerParseLevel(t *testing.T) {
	testCases := []struct {
		str   string
		level logger.Level
	}{
		{"debug", logger.LevelDebug},
		{"Debug", logger.LevelDebug},
		{"DEBUG", logger.LevelDebug},
		{"info", logger.LevelInfo},
		{"Info", logger.LevelInfo},
		{"INFO", logger.LevelInfo},
		{"warn", logger.LevelWarn},
		{"Warn", logger.LevelWarn},
		{"WARN", logger.LevelWarn},
		{"error", logger.LevelError},
		{"Error", logger.LevelError},
		{"ERROR", logger.LevelError},
	}

	for _, tc := range testCases {
		t.Run(tc.str, func(t *testing.T) {
			lvl, err := logger.ParseLevel(tc.str)
			require.NoError(t, err)
			assert.Equal(t, tc.level, lvl)
		})
	}
}

func TestLoggerParseLevelError(t *testing.T) {
	_, err := logger.ParseLevel("invalid")
	assert.Error(t, err)
}

func TestLoggerSetLevel(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, &logger.Options{Format: logger.FormatPrettyNoColors})
	msg := gofakeit.Phrase()

	log.SetLevel(logger.LevelWarn)
	log.Info(context.Background(), msg)
	assert.Empty(t, out.String())
}

func TestLoggerParseFormat(t *testing.T) {
	testCases := []struct {
		str    string
		format logger.Format
	}{
		{"json", logger.FormatJSON},
		{"Json", logger.FormatJSON},
		{"JSON", logger.FormatJSON},
		{"text", logger.FormatText},
		{"Text", logger.FormatText},
		{"TEXT", logger.FormatText},
		{"pretty", logger.FormatPretty},
		{"Pretty", logger.FormatPretty},
		{"PRETTY", logger.FormatPretty},
		{"pretty-no-colors", logger.FormatPrettyNoColors},
		{"Pretty-No-Colors", logger.FormatPrettyNoColors},
		{"PRETTY-NO-COLORS", logger.FormatPrettyNoColors},
	}

	for _, tc := range testCases {
		t.Run(tc.str, func(t *testing.T) {
			f, err := logger.ParseFormat(tc.str)
			require.NoError(t, err)
			assert.Equal(t, tc.format, f)
		})
	}
}

func TestLoggerParseFormatError(t *testing.T) {
	_, err := logger.ParseFormat("invalid")
	assert.Error(t, err)
}

func TestLoggerFormatString(t *testing.T) {
	testCases := []struct {
		format logger.Format
		str    string
	}{
		{logger.FormatJSON, "json"},
		{logger.FormatText, "text"},
		{logger.FormatPretty, "pretty"},
		{logger.FormatPrettyNoColors, "pretty-no-colors"},
		{logger.Format(10), "invalid"},
	}

	for _, tc := range testCases {
		t.Run(tc.str, func(t *testing.T) {
			str := tc.format.String()
			assert.Equal(t, tc.str, str)
		})
	}
}

func TestLoggerSetFormat(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, &logger.Options{Format: logger.FormatJSON})

	log.SetFormat(logger.FormatText)
	log.Info(context.Background(), "Hello")
	assert.Contains(t, out.String(), "msg=Hello")
}

func TestLoggerParseDestination(t *testing.T) {
	testCases := []struct {
		str  string
		dest io.Writer
	}{
		{"stdout", os.Stdout},
		{"Stdout", os.Stdout},
		{"STDOUT", os.Stdout},
		{"stderr", os.Stderr},
		{"Stderr", os.Stderr},
		{"STDERR", os.Stderr},
	}

	for _, tc := range testCases {
		t.Run(tc.str, func(t *testing.T) {
			d, err := logger.ParseDestination(tc.str)
			require.NoError(t, err)
			assert.Equal(t, tc.dest, d)
		})
	}
}

func TestLoggerParseDestinationError(t *testing.T) {
	_, err := logger.ParseDestination("invalid")
	assert.Error(t, err)
}

func TestLoggerContextNoAttr(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, &logger.Options{Format: logger.FormatText})
	ctx := logger.Context(context.Background())

	log.Info(ctx, gofakeit.Phrase())
	assert.NotContains(t, out.String(), "key=val")
}

func TestLoggerContextWithAttr(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, &logger.Options{Format: logger.FormatText})
	ctx := logger.Context(context.Background(), logger.Str("key", "val"))

	log.Info(ctx, gofakeit.Phrase())
	assert.Contains(t, out.String(), "key=val")
}

func TestLoggerContextWithExistingAttr(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, &logger.Options{Format: logger.FormatText})
	ctx := logger.Context(context.Background(), logger.Str("a", "1"))
	ctx = logger.Context(ctx, logger.Int("b", 2))

	log.Info(ctx, gofakeit.Phrase())
	assert.Contains(t, out.String(), "a=1")
	assert.Contains(t, out.String(), "b=2")
}

func TestLoggerAttrsFromContext(t *testing.T) {
	ctx := logger.Context(context.Background(), logger.Str("a", "1"))
	attrs := logger.Attrs(ctx)
	assert.Contains(t, attrs, logger.Str("a", "1"))
}

func TestLoggerAttrsFromEmptyContext(t *testing.T) {
	ctx := context.Background()
	attrs := logger.Attrs(ctx)
	assert.Empty(t, attrs)
}

func BenchmarkLoggerInfo(b *testing.B) {
	out := bytes.NewBufferString("")
	msg := gofakeit.Phrase()
	ctx := context.Background()

	testCases := []logger.Format{logger.FormatJSON, logger.FormatText, logger.FormatPretty}

	for _, tc := range testCases {
		b.Run(tc.String(), func(b *testing.B) {
			log := logger.New(out, &logger.Options{Format: tc})
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				log.Info(ctx, msg)
				out.Reset()
			}
		})
	}
}
