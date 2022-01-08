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
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// Logger represents a logging object responsible to generate outputs to
// an io.Writer.
type Logger = zerolog.Logger

var (
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

// init sets up the logging package in order to generate the logs correctly.
func init() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMicro
}

// New creates a new logger object.
func New() Logger {
	output := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339Nano,
	}
	output.FormatTimestamp = func(i interface{}) string {
		v, err := strconv.ParseInt(fmt.Sprintf("%v", i), 10, 64)
		if err != nil {
			return ""
		}

		t := time.UnixMicro(v)
		return colorize(white, t.Format("2006-01-02 15:04:05.000000 -0700"))
	}
	output.FormatLevel = func(i interface{}) string {
		var color string

		switch i {
		case "trace":
			color = gray
		case "debug":
			color = blue
		case "info":
			color = green
		case "warn":
			color = yellow
		case "error":
			color = red
		case "fatal":
			color = bgRed
		}

		return fmt.Sprintf("| %-14s |",
			colorize(color, strings.ToUpper(fmt.Sprintf("%s", i))))
	}
	output.FormatMessage = func(i interface{}) string {
		return colorize(cyan, fmt.Sprintf("%s", i))
	}
	output.FormatFieldName = func(i interface{}) string {
		return colorize(gray, fmt.Sprintf("%s=", i))
	}
	output.FormatFieldValue = func(i interface{}) string {
		return colorize(gray, fmt.Sprintf("%s", i))
	}
	output.FormatErrFieldName = func(i interface{}) string {
		return colorize(gray, fmt.Sprintf("%s=", i))
	}
	output.FormatErrFieldValue = func(i interface{}) string {
		return colorize(gray, fmt.Sprintf("%s", i))
	}

	return zerolog.New(output).
		With().
		Timestamp().
		Logger()
}

// SetSeverityLevel sets the minimal severity level which the logs will be
// produced.
func SetSeverityLevel(level string) {
	switch level {
	case "trace", "TRACE":
		zerolog.SetGlobalLevel(zerolog.TraceLevel)
	case "debug", "DEBUG":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info", "INFO":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn", "WARN":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error", "ERROR":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	case "panic", "PANIC":
		zerolog.SetGlobalLevel(zerolog.PanicLevel)
	default:
		panic(errors.New("invalid log level"))
	}
}

// colorize sets the msg in a given color.
func colorize(color, msg string) string {
	if color != "" {
		return color + msg + reset
	}

	return msg
}
