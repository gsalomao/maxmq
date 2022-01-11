/*
 * Copyright 2022 The MaxMQ Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

// Logger represents a logging object responsible to generate outputs to
// an io.Writer.
type Logger = zerolog.Logger

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
}

// New creates a new logger object.
func New(out io.Writer) Logger {
	output := &zerolog.ConsoleWriter{
		Out:        out,
		TimeFormat: time.RFC3339Nano,
	}

	output.FormatTimestamp = formatTimestamp
	output.FormatLevel = formatLevel
	output.FormatMessage = formatMessage
	output.FormatFieldName = formatFieldName
	output.FormatFieldValue = formatFieldValue
	output.FormatErrFieldName = formatFieldName
	output.FormatErrFieldValue = formatFieldValue

	return zerolog.New(output).
		With().
		Timestamp().
		Logger()
}

// SetSeverityLevel sets the minimal severity level which the logs will be
// produced.
func SetSeverityLevel(level string) error {
	l, ok := levelCode[level]
	if !ok {
		return errors.New("invalid log level")
	}

	zerolog.SetGlobalLevel(l)
	return nil
}

func formatTimestamp(i interface{}) string {
	v, _ := strconv.ParseInt(fmt.Sprintf("%v", i), 10, 64)
	t := time.UnixMicro(v)
	return colorize(white, t.Format("2006-01-02 15:04:05.000000 -0700"))
}

func formatLevel(i interface{}) string {
	level := strings.ToUpper(fmt.Sprintf("%s", i))
	color := levelColor[level]
	return fmt.Sprintf("| %-14s |", colorize(color, level))
}

func formatMessage(i interface{}) string {
	return colorize(cyan, fmt.Sprintf("%s", i))
}

func formatFieldName(i interface{}) string {
	return colorize(gray, fmt.Sprintf("%s=", i))
}

func formatFieldValue(i interface{}) string {
	return colorize(gray, fmt.Sprintf("%s", i))
}

func colorize(color, msg string) string {
	return color + msg + reset
}
