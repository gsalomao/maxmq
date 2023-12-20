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

package logger

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/gsalomao/maxmq/internal/logger/pretty"
)

// An Attr is a key-value pair.
type Attr = slog.Attr

// Str returns an Attr for a string value.
func Str(key, value string) Attr {
	return slog.String(key, value)
}

// Int converts an int to an int64 and returns an Attr with that value.
func Int(key string, value int) Attr {
	return slog.Int64(key, int64(value))
}

// Uint converts an uint to an uint64 and returns an Attr with that value.
func Uint(key string, value uint) Attr {
	return slog.Uint64(key, uint64(value))
}

// Int8 returns an Attr for an int8.
func Int8(key string, value int8) Attr {
	return slog.Int64(key, int64(value))
}

// Uint8 returns an Attr for an uint8.
func Uint8(key string, value uint8) Attr {
	return slog.Uint64(key, uint64(value))
}

// Int16 returns an Attr for an int16.
func Int16(key string, value int16) Attr {
	return slog.Int64(key, int64(value))
}

// Uint16 returns an Attr for an uint16.
func Uint16(key string, value uint16) Attr {
	return slog.Uint64(key, uint64(value))
}

// Uint32 returns an Attr for an uint32.
func Uint32(key string, value uint32) Attr {
	return slog.Uint64(key, uint64(value))
}

// Int32 returns an Attr for an int32.
func Int32(key string, value int32) Attr {
	return slog.Int64(key, int64(value))
}

// Uint64 returns an Attr for an uint64.
func Uint64(key string, value uint64) Attr {
	return slog.Uint64(key, value)
}

// Int64 returns an Attr for an int64.
func Int64(key string, value int64) Attr {
	return slog.Int64(key, value)
}

// Float64 returns an Attr for a floating-point number.
func Float64(key string, value float64) Attr {
	return slog.Float64(key, value)
}

// Bool returns an Attr for a bool.
func Bool(key string, value bool) Attr {
	return slog.Bool(key, value)
}

// Time returns an Attr for a time.Time.
// It discards the monotonic portion.
func Time(key string, value time.Time) Attr {
	return slog.Time(key, value)
}

// Duration returns an Attr for a time.Duration.
func Duration(key string, value time.Duration) Attr {
	return slog.Duration(key, value)
}

// Err returns an Attr for an error.
// It stores the error string in a string attribute with err key.
func Err(err error) Attr {
	return slog.String("error", err.Error())
}

// Group returns an Attr for a Group Value.
// The first argument is the key and the remaining arguments are converted to Attrs.
func Group(key string, args ...any) Attr {
	return slog.Group(key, args...)
}

// Any returns an Attr for the supplied value.
func Any(key string, value any) Attr {
	return slog.Any(key, value)
}

const (
	// LevelDebug defines debug log level.
	LevelDebug = slog.LevelDebug

	// LevelInfo defines info log level.
	LevelInfo = slog.LevelInfo

	// LevelWarn defines warn log level.
	LevelWarn = slog.LevelWarn

	// LevelError defines error log level.
	LevelError = slog.LevelError
)

// Level defines the importance or severity of a log event.
type Level = slog.Level

// ParseLevel converts a level string into a Level value.
// It returns an error if the input string does not match a known level.
func ParseLevel(str string) (Level, error) {
	switch str {
	case "debug", "Debug", "DEBUG":
		return LevelDebug, nil
	case "info", "Info", "INFO":
		return LevelInfo, nil
	case "warn", "Warn", "WARN":
		return LevelWarn, nil
	case "error", "Error", "ERROR":
		return LevelError, nil
	default:
		return LevelInfo, errors.New("invalid log level")
	}
}

const (
	// FormatJSON represents a JSON log format.
	FormatJSON Format = iota

	// FormatText represents a structured human-friendly log format.
	FormatText

	// FormatPretty represents a human-friendly and pretty log format.
	FormatPretty

	// FormatPrettyNoColors represents a human-friendly and pretty log format without colors.
	FormatPrettyNoColors
)

// Format represents the log format to be generated.
type Format int

// ParseFormat converts a format string into a Format value.
// It returns an error if the input string does not match a known format.
func ParseFormat(str string) (Format, error) {
	switch str {
	case "json", "Json", "JSON":
		return FormatJSON, nil
	case "text", "Text", "TEXT":
		return FormatText, nil
	case "pretty", "Pretty", "PRETTY":
		return FormatPretty, nil
	case "pretty-no-colors", "Pretty-No-Colors", "PRETTY-NO-COLORS":
		return FormatPrettyNoColors, nil
	default:
		return FormatJSON, errors.New("invalid log format")
	}
}

// String returns the format name.
func (f Format) String() string {
	switch f {
	case FormatJSON:
		return "json"
	case FormatText:
		return "text"
	case FormatPretty:
		return "pretty"
	case FormatPrettyNoColors:
		return "pretty-no-colors"
	default:
		return "invalid"
	}
}

// ParseDestination returns the io.Writer for the given destination.
func ParseDestination(str string) (io.Writer, error) {
	switch str {
	case "stdout", "Stdout", "STDOUT":
		return os.Stdout, nil
	case "stderr", "Stderr", "STDERR":
		return os.Stderr, nil
	default:
		return nil, fmt.Errorf("invalid log destination")
	}
}

// LogIDGenerator is responsible for generate log IDs.
type LogIDGenerator interface {
	// NextID generates a new log ID.
	NextID() uint64
}

// Options are options for the Logger.
type Options struct {
	// Level defines the minimal log level.
	Level Level

	// Format defines the log format.
	Format Format

	// LogIDGenerator is responsible to generate log identifiers. If it's not provided, no log id is
	// added to the logs.
	LogIDGenerator LogIDGenerator
}

// Logger represents a logging object that generates logs to an io.Writer. Each logging operation
// makes a single call to the Writer's Write method. There is no guarantee on access serialization
// to the Writer. If the Writer is not thread safe, consider a sync wrapper.
type Logger struct {
	level  slog.LevelVar
	idGen  LogIDGenerator
	format atomic.Int64

	json           slog.Handler
	text           slog.Handler
	pretty         slog.Handler
	prettyNoColors slog.Handler
}

// New creates a new Logger which writes log message into out.
//
// The Options parameter allows to set optional settings. If it's not provided, the logger is
// created using the default configuration.
func New(out io.Writer, opts *Options) *Logger {
	var (
		log    = &Logger{}
		format Format
	)

	if opts != nil {
		log.level.Set(opts.Level)
		log.idGen = opts.LogIDGenerator
		format = opts.Format
	}

	replaceAttrs := func(groups []string, a slog.Attr) slog.Attr {
		if a.Key == slog.SourceKey {
			a.Key = "caller"

			if src, _ := a.Value.Any().(*slog.Source); src != nil {
				dir, file := filepath.Split(src.File)
				caller := fmt.Sprintf("%s:%v", filepath.Join(filepath.Base(dir), file), src.Line)
				a.Value = slog.StringValue(caller)
			}
		}
		return a
	}

	log.json = slog.NewJSONHandler(out, &slog.HandlerOptions{
		Level:       &log.level,
		ReplaceAttr: replaceAttrs,
		AddSource:   true,
	})
	log.text = slog.NewTextHandler(out, &slog.HandlerOptions{
		Level:       &log.level,
		ReplaceAttr: replaceAttrs,
		AddSource:   true,
	})
	log.pretty = pretty.NewHandler(out, &pretty.Options{
		Level:       &log.level,
		ReplaceAttr: replaceAttrs,
		AddSource:   true,
		NoColors:    false,
	})
	log.prettyNoColors = pretty.NewHandler(out, &pretty.Options{
		Level:       &log.level,
		ReplaceAttr: replaceAttrs,
		AddSource:   true,
		NoColors:    true,
	})

	log.format.Store(int64(format))
	return log
}

// StdLog returns a slog.Logger from the current logger.
func (l *Logger) StdLog(attrs ...Attr) *slog.Logger {
	return slog.New(&stdLogWrapper{log: l, attrs: attrs})
}

// Log creates a log record with the provided level.
func (l *Logger) Log(ctx context.Context, level Level, msg string, attrs ...Attr) {
	var pcs [1]uintptr
	runtime.Callers(3, pcs[:])

	if l.idGen != nil {
		attrs = append(attrs, Uint64("log_id", l.idGen.NextID()))
	}

	r := slog.NewRecord(time.Now(), level, msg, pcs[0])
	r.AddAttrs(attrs...)

	_ = l.Handle(ctx, r)
}

// Debug creates a log with debug level.
func (l *Logger) Debug(ctx context.Context, msg string, attrs ...Attr) {
	l.Log(ctx, LevelDebug, msg, attrs...)
}

// Info starts a new message with info level.
func (l *Logger) Info(ctx context.Context, msg string, attrs ...Attr) {
	l.Log(ctx, LevelInfo, msg, attrs...)
}

// Warn starts a new message with warn level.
func (l *Logger) Warn(ctx context.Context, msg string, attrs ...Attr) {
	l.Log(ctx, LevelWarn, msg, attrs...)
}

// Error starts a new message with error level.
func (l *Logger) Error(ctx context.Context, msg string, attrs ...Attr) {
	l.Log(ctx, LevelError, msg, attrs...)
}

// SetLevel sets the minimum accepted level to the logger.
func (l *Logger) SetLevel(lvl Level) {
	l.level.Set(lvl)
}

// SetFormat sets the log format.
func (l *Logger) SetFormat(f Format) {
	l.format.Store(int64(f))
}

func (l *Logger) Handler() slog.Handler {
	format := Format(l.format.Load())

	switch format {
	case FormatPretty:
		return l.pretty
	case FormatPrettyNoColors:
		return l.prettyNoColors
	case FormatText:
		return l.text
	default:
		return l.json
	}
}

func (l *Logger) Handle(ctx context.Context, record slog.Record) error {
	handler := l.Handler()

	ctxAttrs := ctx.Value(ctxKeyAttrs{})
	if ctxAttrs != nil {
		record.AddAttrs(ctxAttrs.([]Attr)...)
	}

	if l.idGen != nil {
		record.AddAttrs(Uint64("log_id", l.idGen.NextID()))
	}

	return handler.Handle(ctx, record)
}

type ctxKeyAttrs struct{}

// Context returns a context based on the provided context with optional attributes.
//
// When attributes are added to the context, any logging method called with the returned context
// includes these attributes into the log entry automatically.
func Context(ctx context.Context, attrs ...Attr) context.Context {
	if len(attrs) == 0 {
		return ctx
	}

	if existing := ctx.Value(ctxKeyAttrs{}); existing != nil {
		attrs = append(attrs, existing.([]Attr)...)
	}

	return context.WithValue(ctx, ctxKeyAttrs{}, attrs)
}

// Attrs returns the existing attributes from the provided context.
//
// If the context does not have any attribute, it returns nil.
func Attrs(ctx context.Context) []Attr {
	attrs := ctx.Value(ctxKeyAttrs{})
	if attrs == nil {
		return nil
	}
	return attrs.([]Attr)
}

type stdLogWrapper struct {
	log   *Logger
	attrs []Attr
}

func (s stdLogWrapper) Enabled(ctx context.Context, level slog.Level) bool {
	return s.log.Handler().Enabled(ctx, level)
}

func (s stdLogWrapper) Handle(ctx context.Context, record slog.Record) error { //nolint:gocritic
	if len(s.attrs) > 0 {
		record.AddAttrs(s.attrs...)
	}
	return s.log.Handle(ctx, record)
}

func (s stdLogWrapper) WithAttrs(attrs []slog.Attr) slog.Handler {
	return s.log.Handler().WithAttrs(attrs)
}

func (s stdLogWrapper) WithGroup(name string) slog.Handler {
	return s.log.Handler().WithGroup(name)
}
