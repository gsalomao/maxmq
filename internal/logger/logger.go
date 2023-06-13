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
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// LogFormat defines the log format.
type LogFormat string

const (
	// Pretty defines the log format in human-friendly format.
	Pretty LogFormat = "pretty"

	// Json defines the log format in JSON format.
	Json LogFormat = "json"
)

const (
	reset  = "\x1b[0m"
	red    = "\x1b[31m"
	green  = "\x1b[32m"
	yellow = "\x1b[33m"
	blue   = "\x1b[34m"
	cyan   = "\x1b[36m"
	white  = "\x1b[37m"
	bgRed  = "\x1b[41m"
	gray   = "\x1b[90m"
)

var levelColor = map[string]string{
	"TRACE": gray,
	"DEBUG": blue,
	"INFO":  green,
	"WARN":  yellow,
	"ERROR": red,
	"FATAL": bgRed,
}

var levelCode = map[string]zerolog.Level{
	"trace":   zerolog.TraceLevel,
	"TRACE":   zerolog.TraceLevel,
	"debug":   zerolog.DebugLevel,
	"DEBUG":   zerolog.DebugLevel,
	"info":    zerolog.InfoLevel,
	"INFO":    zerolog.InfoLevel,
	"warn":    zerolog.WarnLevel,
	"WARN":    zerolog.WarnLevel,
	"warning": zerolog.WarnLevel,
	"WARNING": zerolog.WarnLevel,
	"error":   zerolog.ErrorLevel,
	"ERROR":   zerolog.ErrorLevel,
	"fatal":   zerolog.FatalLevel,
	"FATAL":   zerolog.FatalLevel,
}

func init() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMicro
	zerolog.TimestampFieldName = "Time"
	zerolog.LevelFieldName = "Level"
	zerolog.MessageFieldName = "Message"
}

// SetSeverityLevel sets the minimal severity level which the logs will be produced.
func SetSeverityLevel(level string) error {
	l, ok := levelCode[level]
	if !ok {
		return errors.New("invalid log level")
	}

	zerolog.SetGlobalLevel(l)
	return nil
}

func colorize(color, msg string) string {
	return color + msg + reset
}

// LogIDGenerator is responsible for generate log IDs.
type LogIDGenerator interface {
	// NextID generates a new log ID.
	NextID() uint64
}

// Logger represents a logging object responsible to generate outputs to an io.Writer.
type Logger struct {
	// Unexported fields.
	zerolog.Logger
	generator LogIDGenerator
	writer    io.Writer
	prefix    string
	format    LogFormat
}

// New creates a new logger.
func New(wr io.Writer, gen LogIDGenerator, f LogFormat) *Logger {
	l := &Logger{generator: gen, writer: wr, format: f}

	var ctx zerolog.Context
	if f == Pretty {
		output := &zerolog.ConsoleWriter{Out: wr, TimeFormat: time.RFC3339Nano}
		output.FormatTimestamp = l.formatTimestamp
		output.FormatLevel = l.formatLevel
		output.FormatMessage = l.formatMessage
		output.FormatFieldName = l.formatFieldName
		output.FormatFieldValue = l.formatFieldValue
		output.FormatErrFieldName = l.formatFieldName
		output.FormatErrFieldValue = l.formatFieldValue
		ctx = zerolog.New(output).Hook(l).With().Timestamp()
	} else {
		ctx = zerolog.New(wr).Hook(l).With().Timestamp()
	}

	l.Logger = ctx.Logger()
	return l
}

// NewWithPrefix creates a new logger with a prefix.
func NewWithPrefix(wr io.Writer, gen LogIDGenerator, prefix string, f LogFormat) *Logger {
	l := New(wr, gen, f)
	l.prefix = prefix
	if f != Pretty {
		l.Logger = l.Logger.With().Str("Prefix", prefix).Logger()
	}
	return l
}

// WithPrefix creates a new logger from the existing one with a prefix.
func (l *Logger) WithPrefix(prefix string) *Logger {
	if prefix != "" {
		if l.prefix != "" {
			prefix = fmt.Sprintf("%s.%s", l.prefix, prefix)
		}
	}

	logger := NewWithPrefix(l.writer, l.generator, prefix, l.format)
	return logger
}

// BaseLogger returns the zerolog.Logger.
func (l *Logger) BaseLogger() *zerolog.Logger {
	return &l.Logger
}

// Run implements the zerolog.Hook interface to add log ID into the log event.
func (l *Logger) Run(e *zerolog.Event, _ zerolog.Level, _ string) {
	if l.generator != nil {
		e.Uint64("LogId", l.generator.NextID())
	}
}

func (l *Logger) formatTimestamp(i interface{}) string {
	v, _ := strconv.ParseInt(fmt.Sprintf("%v", i), 10, 64)
	t := time.UnixMicro(v)
	return colorize(white, t.Format("2006-01-02 15:04:05.000000 -0700"))
}

func (l *Logger) formatLevel(i interface{}) string {
	level := strings.ToUpper(fmt.Sprintf("%s", i))
	color := levelColor[level]
	return fmt.Sprintf("| %-14s |", colorize(color, level))
}

func (l *Logger) formatMessage(i interface{}) string {
	var prefix string
	if l.prefix != "" {
		prefix = fmt.Sprintf("[%s] ", l.prefix)
	}
	return colorize(cyan, fmt.Sprintf("%s%s", prefix, i))
}

func (l *Logger) formatFieldName(i interface{}) string {
	return colorize(gray, fmt.Sprintf("%s=", i))
}

func (l *Logger) formatFieldValue(i interface{}) string {
	return colorize(gray, fmt.Sprintf("%s", i))
}
