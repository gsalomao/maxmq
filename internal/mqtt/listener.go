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

package mqtt

import (
	"context"
	"errors"
	"log/slog"
	"net"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/mochi-mqtt/server/v2/listeners"
)

type Listener interface {
	Name() string
	Address() string
	Listen() error
	Accept() (net.Conn, error)
	Close() error
}

type onConnection func(l listeners.Listener, nc net.Conn, fn listeners.EstablishFn)

type listenerMochiAdaptor struct {
	listener Listener
	log      *logger.Logger
	ctx      context.Context
	done     chan struct{}
	callback onConnection
}

func (l *listenerMochiAdaptor) Init(_ *slog.Logger) error {
	err := l.listener.Listen()
	if err != nil {
		l.log.Error(l.ctx, "Failed to starting listener", logger.Err(err))
		return err
	}

	l.done = make(chan struct{})
	l.log.Info(l.ctx, "MQTT Listener listening on "+l.listener.Address())
	return nil
}

func (l *listenerMochiAdaptor) Serve(fn listeners.EstablishFn) {
	ready := make(chan struct{})

	go func() {
		defer close(l.done)
		close(ready)

		for {
			l.log.Debug(l.ctx, "Listener waiting for new connection")

			nc, err := l.listener.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					break
				}
				l.log.Warn(l.ctx, "Failed to accept connection from listener", logger.Err(err))
				continue
			}

			l.log.Debug(l.ctx, "New connection from listener",
				logger.Str("address", nc.RemoteAddr().String()),
			)

			l.callback(l, nc, fn)
		}
	}()

	<-ready
}

func (l *listenerMochiAdaptor) ID() string {
	return l.listener.Name()
}

func (l *listenerMochiAdaptor) Address() string {
	return l.listener.Address()
}

func (l *listenerMochiAdaptor) Protocol() string {
	return ""
}

func (l *listenerMochiAdaptor) Close(fn listeners.CloseFn) {
	err := l.listener.Close()
	if err != nil {
		l.log.Warn(l.ctx, "Error when closing listener", logger.Err(err))
	}

	fn(l.ID())
	<-l.done
	l.log.Debug(l.ctx, "Listener was closed")
}
