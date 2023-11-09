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

package pretty

import (
	"context"
	"encoding"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"strconv"
	"sync"
	"time"
	"unicode"
)

const (
	reset  = "\x1b[0m"
	faint  = "\x1b[2m"
	red    = "\x1b[31m"
	green  = "\x1b[32m"
	yellow = "\x1b[33m"
	blue   = "\x1b[34m"
	cyan   = "\x1b[36m"
	gray   = "\x1b[90m"
)

var levelColor = map[slog.Level]string{
	slog.LevelDebug: blue,
	slog.LevelInfo:  green,
	slog.LevelWarn:  yellow,
	slog.LevelError: red,
}

// Options are options for the Handler.
type Options struct {
	// Level sets the minimal log level.
	Level slog.Leveler

	// ReplaceAttr is called to rewrite each non-group attribute before it is logged.
	ReplaceAttr func(groups []string, attr slog.Attr) slog.Attr

	// TimeFormat sets the time format.
	TimeFormat string

	// AddSource adds caller information.
	AddSource bool

	// NoColors disables the colors.
	NoColors bool
}

// Handler is a log handler which writes log message in a human-friendly and pretty format.
type Handler struct {
	mu          sync.Mutex
	out         io.Writer
	level       slog.Leveler
	replaceAttr func([]string, slog.Attr) slog.Attr
	timeFormat  string
	attrsPrefix string
	groupPrefix string
	groups      []string
	addSource   bool
	noColors    bool
}

// NewHandler creates a Handler that writes to w, using the given options.
// If opts is nil, the default options are used.
func NewHandler(out io.Writer, opts *Options) *Handler {
	h := &Handler{
		out:        out,
		level:      slog.LevelInfo,
		timeFormat: "2006-01-02 15:04:05.000000 -0700",
	}
	if opts == nil {
		return h
	}

	h.replaceAttr = opts.ReplaceAttr
	h.addSource = opts.AddSource
	h.noColors = opts.NoColors

	if opts.Level != nil {
		h.level = opts.Level
	}
	if opts.TimeFormat != "" {
		h.timeFormat = opts.TimeFormat
	}

	return h
}

// Enabled reports whether the handler handles records at the given level.
// The handler ignores records whose level is lower.
func (h *Handler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level.Level()
}

// Handle formats its argument Record as a single line.
func (h *Handler) Handle(_ context.Context, record slog.Record) error { //nolint:gocritic
	buf := newBuffer()
	defer buf.free()

	if !record.Time.IsZero() {
		h.appendTime(buf, record.Time)
		_ = buf.WriteByte(' ')
	}

	if h.replaceAttr == nil {
		h.appendLevel(buf, record.Level)
		_ = buf.WriteByte(' ')
	} else if a := h.replaceAttr(nil, slog.Any(slog.LevelKey, record.Level)); a.Key != "" {
		h.appendValue(buf, a.Value, false)
		_ = buf.WriteByte(' ')
	}

	h.appendMessage(buf, record.Message)
	_ = buf.WriteByte(' ')

	if h.addSource {
		fs := runtime.CallersFrames([]uintptr{record.PC})
		f, _ := fs.Next()
		if f.File != "" {
			src := &slog.Source{
				Function: f.Function,
				File:     f.File,
				Line:     f.Line,
			}

			if h.replaceAttr == nil {
				h.appendSource(buf, src)
				_ = buf.WriteByte(' ')
			} else if a := h.replaceAttr(nil, slog.Any(slog.SourceKey, src)); a.Key != "" {
				h.appendValue(buf, a.Value, false)
				_ = buf.WriteByte(' ')
			}
		}
	}

	if len(h.attrsPrefix) > 0 {
		_, _ = buf.WriteString(h.attrsPrefix)
	}

	record.Attrs(func(attr slog.Attr) bool {
		h.appendAttr(buf, attr, h.groupPrefix, h.groups)
		return true
	})

	if len(*buf) == 0 {
		return nil
	}
	(*buf)[len(*buf)-1] = '\n'

	h.mu.Lock()
	defer h.mu.Unlock()

	_, err := h.out.Write(*buf)
	return err
}

// WithAttrs returns a new Handler whose attributes consists of h's attributes followed by attrs.
func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	h2 := &Handler{
		out:         h.out,
		level:       h.level,
		replaceAttr: h.replaceAttr,
		timeFormat:  h.timeFormat,
		attrsPrefix: h.attrsPrefix,
		groupPrefix: h.groupPrefix,
		groups:      h.groups,
		addSource:   h.addSource,
	}

	buf := newBuffer()
	defer buf.free()

	for _, attr := range attrs {
		h.appendAttr(buf, attr, h.groupPrefix, h.groups)
	}

	h2.attrsPrefix = h.attrsPrefix + string(*buf)
	return h2
}

// WithGroup returns a new Handler with a ground with name.
func (h *Handler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}

	h2 := &Handler{
		out:         h.out,
		level:       h.level,
		replaceAttr: h.replaceAttr,
		timeFormat:  h.timeFormat,
		attrsPrefix: h.attrsPrefix,
		groupPrefix: h.groupPrefix,
		groups:      h.groups,
		addSource:   h.addSource,
	}

	h2.groupPrefix += name + "."
	h2.groups = append(h2.groups, name)
	return h2
}

func (h *Handler) appendTime(buf *buffer, t time.Time) {
	h.appendColor(buf, faint)
	*buf = t.AppendFormat(*buf, h.timeFormat)
	h.appendColor(buf, reset)
}

func (h *Handler) appendLevel(buf *buffer, level slog.Level) {
	h.appendColor(buf, levelColor[level])
	_, _ = fmt.Fprintf(buf, "%-5s", level.String())
	h.appendColor(buf, reset)
}

func (h *Handler) appendMessage(buf *buffer, msg string) {
	h.appendColor(buf, cyan)
	_, _ = buf.WriteString(msg)
	h.appendColor(buf, reset)
}

func (h *Handler) appendKey(buf *buffer, key, groups string) {
	h.appendColor(buf, gray)
	appendString(buf, groups+key, true)

	_ = buf.WriteByte('=')
	h.appendColor(buf, reset)
}

func (h *Handler) appendValue(buf *buffer, v slog.Value, quote bool) {
	h.appendColor(buf, gray)

	switch v.Kind() {
	case slog.KindString:
		appendString(buf, v.String(), quote)
	case slog.KindInt64:
		*buf = strconv.AppendInt(*buf, v.Int64(), 10)
	case slog.KindUint64:
		*buf = strconv.AppendUint(*buf, v.Uint64(), 10)
	case slog.KindFloat64:
		*buf = strconv.AppendFloat(*buf, v.Float64(), 'g', -1, 64)
	case slog.KindBool:
		*buf = strconv.AppendBool(*buf, v.Bool())
	case slog.KindDuration:
		appendString(buf, v.Duration().String(), quote)
	case slog.KindTime:
		appendString(buf, v.Time().String(), quote)
	case slog.KindAny:
		h.appendAny(buf, v.Any(), quote)
	}

	h.appendColor(buf, reset)
}

func (h *Handler) appendAny(buf *buffer, v any, quote bool) {
	switch cv := v.(type) {
	case slog.Level:
		h.appendLevel(buf, cv)
	case encoding.TextMarshaler:
		data, err := cv.MarshalText()
		if err != nil {
			break
		}
		appendString(buf, string(data), quote)
	case *slog.Source:
		h.appendSource(buf, cv)
	default:
		appendString(buf, fmt.Sprint(v), quote)
	}
}

func (h *Handler) appendSource(buf *buffer, src *slog.Source) {
	h.appendColor(buf, gray)

	_, _ = buf.WriteString(src.File)
	_ = buf.WriteByte(':')
	_, _ = buf.WriteString(strconv.Itoa(src.Line))

	h.appendColor(buf, reset)
}

func (h *Handler) appendAttr(buf *buffer, attr slog.Attr, groupsPrefix string, groups []string) {
	attr.Value = attr.Value.Resolve()

	if rep := h.replaceAttr; rep != nil && attr.Value.Kind() != slog.KindGroup {
		attr = rep(groups, attr)
		attr.Value = attr.Value.Resolve()
	}

	if attr.Equal(slog.Attr{}) {
		return
	}

	if attr.Value.Kind() == slog.KindGroup {
		if attr.Key != "" {
			groupsPrefix += attr.Key + "."
			groups = append(groups, attr.Key)
		}
		for _, groupAttr := range attr.Value.Group() {
			h.appendAttr(buf, groupAttr, groupsPrefix, groups)
		}
	} else {
		h.appendKey(buf, attr.Key, groupsPrefix)
		h.appendValue(buf, attr.Value, true)
		_ = buf.WriteByte(' ')
	}
}

func (h *Handler) appendColor(buf *buffer, color string) {
	if h.noColors {
		return
	}
	_, _ = buf.WriteString(color)
}

func appendString(buf *buffer, str string, quote bool) {
	if quote && needsQuoting(str) {
		*buf = strconv.AppendQuote(*buf, str)
	} else {
		_, _ = buf.WriteString(str)
	}
}

func needsQuoting(str string) bool {
	if str == "" {
		return true
	}

	for _, r := range str {
		if unicode.IsSpace(r) || r == '"' || r == '=' || !unicode.IsPrint(r) {
			return true
		}
	}

	return false
}
